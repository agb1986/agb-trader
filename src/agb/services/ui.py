"""The local UI service: an HTTP server on 127.0.0.1 that renders the pages in `agb.ui`.

The operator views it over an SSH tunnel, and SSH is the authentication: there is no login page,
no session cookie and no outbound connection of any kind. What keeps it off the network is where
the port is published, not where the socket binds — Compose publishes it on the server's
127.0.0.1 alone. Inside a container the socket must bind every interface, because the published
port arrives on the container's own address and nothing would be listening there; the container's
network namespace is the boundary a loopback bind would be asking for. Run outside a container it
binds the loopback address, which is what a bare process on the workstation wants.

Every request opens its own connection as the `ui` role and closes it again, which is both
what a threaded server needs — a psycopg connection is not safe to share between threads — and
the cheapest way to be certain a request cannot leave a transaction open. A GET renders a page
and gets a read-only connection; a POST to one of the write routes in `ui.WRITES` gets one that
may write. A POST to a page is 405 and anything else is 404, so the write paths are the six
the UI declares and nothing else.

A write is answered with a 303 to a GET page, never with the outcome itself, so a reload or a
back-and-forward after a write repeats a read and not the write. The outcome is carried by a
token held in this process's memory (`Outcomes`): the token is 128 random bits, so it cannot be
guessed or forged, and the page it names is shown from the store until it lapses after five
minutes or is pushed out by newer ones. A signed token in the URL would put the reply text in
the address bar and the history, and a database row would need a table and a write grant for
the `ui` role that nothing else wants; the cost of memory is that a restart forgets outcomes,
which the page says, and the audit log still holds every write. Anything a write or a page
raises is caught and answered with a 500 page that names the exception's type and the first line
of its message, capped; the traceback goes to the log only. A write answered with a 500 may
already have been recorded, and its page says so. A request whose body was not read before it
was answered is closed rather than kept alive, since the body would be parsed as the next
request; HEAD is answered as GET is, on the path with its query string removed.

The pages are server-rendered HTML with no JavaScript, so there is nothing to build and nothing
to serve besides the pages themselves.

A form on another site can post here without any cookie of ours, since the tunnel is the only
credential and the browser already holds it. Comparing a request's `Origin` with its own `Host`
is not enough: a page that rebinds its own name to the loopback address sends a matching pair.
So every request must be addressed to one of the names in `AGB_UI_ALLOWED_HOSTS` — by default
the two the tunnel is opened as, `127.0.0.1:8080` and `localhost:8080` — and anything else is a
421 before a page is rendered. A write must also declare an `Origin`, and it must be one of the
same names; one that declares none, or another, is refused before the form is read. The
refusals are logged and not recorded, because such a request names no operator and the audit
log records what operators did. Every response forbids framing, content-type sniffing and the
`Referer`, since a page here carries one-click writes and the basket reference.

The main thread runs the heartbeat loop on its own connection, since a heartbeat is a write;
the server runs in a daemon thread beside it. SIGTERM stops the loop, which shuts the server
down, so `docker stop` ends it cleanly.
"""

import logging
import os
import re
import secrets
import threading
import time
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

import psycopg

from agb import ui
from agb.heartbeat import interval_from_env, serve, stop_on_signals

log = logging.getLogger(__name__)

# The port is fixed: the operator's tunnel and docs/architecture/06-local-ui.md both name it, and
# there is nothing to configure on a machine that serves one operator. The bind address is not:
# the container sets AGB_UI_HOST=0.0.0.0 and Compose decides who may reach the published port.
HOST = "127.0.0.1"
PORT = 8080
ACTOR = "ui"
# The most a posted form may carry. The longest field any page has is a rejection reason.
BODY_MAX = 4096
# The most of an error's message a failed write's page repeats.
CAUSE_MAX = 200
# What a token in `/outcome?id=` looks like: what `secrets.token_urlsafe` emits.
TOKEN = re.compile(r"[A-Za-z0-9_-]{1,64}")
# The names a request may be addressed to: the two the operator's tunnel opens the UI as. A
# tunnel forwarded to another local port sets AGB_UI_ALLOWED_HOSTS, comma-separated host:port.
ALLOWED_HOSTS = ("127.0.0.1:8080", "localhost:8080")
# Sent on every response, refusals and the stdlib's own errors included. The pages are inline
# HTML and CSS posting forms to themselves, so the policy allows exactly that and nothing else.
# The referrer policy is `same-origin`, not `no-referrer`: under `no-referrer` a browser sends
# `Origin: null` on the page's own form posts, which the write check refuses, so every form here
# would fail. `same-origin` still sends no `Referer` to any other site.
SECURITY_HEADERS = {
    "X-Frame-Options": "DENY",
    "Content-Security-Policy": (
        "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; "
        "frame-ancestors 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "same-origin",
}


def host_from_env() -> str:
    return os.environ.get("AGB_UI_HOST", HOST)


def allowed_hosts_from_env() -> frozenset[str]:
    """The `Host` values this server answers, lower-cased: AGB_UI_ALLOWED_HOSTS when it names
    any, the tunnel's two names otherwise."""
    configured = os.environ.get("AGB_UI_ALLOWED_HOSTS", "")
    names = [name.strip().lower() for name in configured.split(",") if name.strip()]
    return frozenset(names or ALLOWED_HOSTS)


class Outcomes:
    """What the writes did, held for the GET a write redirects to.

    A write answers 303 to `/outcome?id=<token>` and that page reads the outcome from here, so a
    reload re-reads the outcome and posts nothing. The token is 128 random bits from `secrets`,
    which makes it unforgeable without signing anything, and it names an entry held in this
    process's memory and not a row: nothing is added to the database for the UI to write to, the
    command row the write itself left is the durable record, and an outcome that is gone (it
    expired, or the server restarted) says so and points at the log. An entry is not consumed on
    read, because a reload of a GET has to show the same page; it lapses after `HELD_FOR`
    seconds, and the oldest go first when `CAPACITY` is reached, so a loop of writes cannot
    grow it."""

    HELD_FOR = 300.0
    CAPACITY = 64

    def __init__(self, clock=time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._held: dict[str, tuple[float, ui.Outcome]] = {}

    def put(self, outcome: ui.Outcome) -> str:
        token = secrets.token_urlsafe(16)
        now = self._clock()
        with self._lock:
            self._held = {k: v for k, v in self._held.items() if v[0] > now}
            while len(self._held) >= self.CAPACITY:
                del self._held[next(iter(self._held))]
            self._held[token] = (now + self.HELD_FOR, outcome)
        return token

    def get(self, token: str) -> ui.Outcome | None:
        now = self._clock()
        with self._lock:
            held = self._held.get(token)
        return held[1] if held and held[0] > now else None


class Server(ThreadingHTTPServer):
    """The HTTP server, carrying the `Host` names its handlers answer and the outcomes of the
    writes it has taken."""

    allowed_hosts: frozenset[str] = frozenset(ALLOWED_HOSTS)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.outcomes = Outcomes()


def cause_of(exc: BaseException) -> str:
    """A raised error as the browser is told it: its type and the first line of its message,
    capped. The traceback goes to the log and never to the page."""
    first = (str(exc).splitlines() or [""])[0][:CAUSE_MAX]
    return f"{type(exc).__name__}: {first}" if first else type(exc).__name__


class Handler(BaseHTTPRequestHandler):
    """One request: a page read, or one of the UI's two write paths."""

    server_version = "agb-ui"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802
        """One of the UI's write paths. The handler is passed the fields the form posted and the
        outcome it returns is answered with a 303 to the GET page that shows it, so a reload
        reads the outcome again and never posts the write again. Anything the write raises is
        answered: the database's errors with a 503 and every other with a 500 naming the cause.

        The body is read only on the way to a write, so every answer given before it is read
        closes the connection: a body left unread on a kept-alive connection would be parsed as
        the next request."""
        if not self._allowed_host():
            return
        keep_alive = not self.close_connection
        self.close_connection = True
        path = self._path()
        write = ui.WRITES.get(path)
        if write is None:
            if path in ui.ROUTES:
                self._respond(405, ui.not_allowed(path), allow="GET, HEAD")
            else:
                self._respond(404, ui.not_found(path))
            return
        if not self._same_origin():
            self._respond(403, "<!doctype html>\n<p>That request did not come from here.\n")
            return
        fields = self._fields()
        if fields is None:
            self._respond(400, "<!doctype html>\n<p>That form was not readable.\n")
            return
        self.close_connection = not keep_alive
        try:
            with ui.connect(write=True) as conn:
                outcome = write(conn, fields, datetime.now(UTC))
        except psycopg.Error:
            log.exception("%s failed", path)
            self._respond(503, "<!doctype html>\n<p>The database is unreachable.\n")
            return
        except Exception as exc:
            log.exception("%s failed", path)
            self._respond(500, ui.failed(path, cause_of(exc), wrote=True))
            return
        token = self.server.outcomes.put(outcome)
        self._respond(303, "<!doctype html>\n<p>Done.\n", location=f"{ui.OUTCOME}?id={token}")

    def _path(self) -> str:
        """The request's path with its query string and any trailing slash left off."""
        return self.path.split("?", 1)[0].rstrip("/") or "/"

    def _allowed_host(self) -> bool:
        """Whether the request is addressed to one of the allowed names, answering a 421 when it
        is not. A browser sends the name it looked up, so a page on another site that rebinds
        its own name to this address arrives carrying that name and is refused here."""
        host = (self.headers.get("Host") or "").strip().lower()
        if host in self.server.allowed_hosts:
            return True
        log.warning("refused a %s to %s addressed to host %r", self.command, self.path, host)
        # A refused POST's body is never read, so the connection cannot carry another request.
        self.close_connection = True
        self._respond(421, "<!doctype html>\n<p>That request was not addressed to this server.\n")
        return False

    def _same_origin(self) -> bool:
        """Whether a POST declares an origin that is one of the allowed names. A browser sends
        `Origin` on every form post, so one that is missing or names another host is not a form
        from one of these pages; the operator's own curl passes `-H 'Origin: ...'`."""
        origin = (self.headers.get("Origin") or "").strip().lower()
        if origin in {f"http://{host}" for host in self.server.allowed_hosts}:
            return True
        log.warning("refused a %s to %s from origin %r", self.command, self.path, origin or None)
        return False

    def _fields(self) -> dict[str, str] | None:
        """The form's fields, first value of each. A body longer than the cap is not a form from
        one of our pages: the longest field here is a rejection reason."""
        if self.headers.get("Transfer-Encoding") is not None:
            return None
        length = self.headers.get("Content-Length")
        try:
            size = int(length or 0)
        except ValueError:
            return None
        if size < 0 or size > BODY_MAX:
            return None
        body = self.rfile.read(size).decode("utf-8", "replace")
        return {key: values[0] for key, values in parse_qs(body, keep_blank_values=True).items()}

    def do_GET(self) -> None:  # noqa: N802  (the stdlib's method name)
        if not self._allowed_host():
            return
        self._close_on_body()
        path = self._path()
        view = ui.ROUTES.get(path)
        try:
            with ui.connect() as conn:
                if path == ui.OUTCOME:
                    held = self.server.outcomes.get(self._outcome_token())
                    status, body = (200 if held else 404), ui.outcome(conn, datetime.now(UTC), held)
                elif view:
                    status, body = 200, view(conn, datetime.now(UTC))
                else:
                    status, body = 404, ui.not_found(path)
        except psycopg.Error:
            log.exception("%s failed", path)
            self._respond(503, "<!doctype html>\n<p>The database is unreachable.\n")
            return
        except Exception as exc:
            log.exception("%s failed", path)
            self._respond(500, ui.failed(path, cause_of(exc), wrote=False))
            return
        self._respond(status, body)

    def _outcome_token(self) -> str:
        """The token a redirect carried, or the empty string when it is not shaped like one."""
        values = parse_qs(self.path.partition("?")[2]).get("id", [""])
        return values[0] if TOKEN.fullmatch(values[0]) else ""

    def do_HEAD(self) -> None:  # noqa: N802
        """What a GET of the same path would answer, with the headers and no body. The query
        string is no part of the path, as it is not for a GET."""
        if not self._allowed_host():
            return
        self._close_on_body()
        path = self._path()
        known = path in ui.ROUTES or path == ui.OUTCOME
        self._respond(200 if known else 404, "", head=True)

    def _close_on_body(self) -> None:
        """A GET or HEAD is answered without reading a body, so one that carries any would be
        read as the next request on a kept-alive connection: close it instead."""
        length = self.headers.get("Content-Length") or "0"
        if self.headers.get("Transfer-Encoding") is not None or length != "0":
            self.close_connection = True

    def _respond(
        self,
        status: int,
        body: str,
        head: bool = False,
        allow: str | None = None,
        location: str | None = None,
    ) -> None:
        encoded = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        if allow is not None:
            self.send_header("Allow", allow)
        if location is not None:
            self.send_header("Location", location)
        self.send_header("Content-Length", str(len(encoded)))
        # Nothing here is cacheable: every page is the current state of the database.
        self.send_header("Cache-Control", "no-store")
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        if not head:
            self.wfile.write(encoded)

    def end_headers(self) -> None:
        """Every response's headers end here, the stdlib's own error pages included, so this is
        where the framing, sniffing and referrer protections are added."""
        for name, value in SECURITY_HEADERS.items():
            self.send_header(name, value)
        super().end_headers()

    def log_message(self, format: str, *args) -> None:
        log.info("%s %s", self.address_string(), format % args)


def server() -> Server:
    httpd = Server((host_from_env(), PORT), Handler)
    httpd.daemon_threads = True
    httpd.allowed_hosts = allowed_hosts_from_env()
    return httpd


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    stop = stop_on_signals()
    httpd = server()
    threading.Thread(target=httpd.serve_forever, name="agb-ui", daemon=True).start()
    log.info("ui listening on http://%s:%d", *httpd.server_address[:2])
    try:
        serve(ACTOR, interval_from_env(), stop)
    finally:
        httpd.shutdown()
        httpd.server_close()


if __name__ == "__main__":
    main()
