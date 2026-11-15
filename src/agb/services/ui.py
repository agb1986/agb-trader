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
may write. A POST to a page is 405 and anything else is 404, so the write paths are the five
the UI declares and nothing else. The pages are server-rendered HTML with no JavaScript, so
there is nothing to build and nothing to serve besides the pages themselves.

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
import threading
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


class Server(ThreadingHTTPServer):
    """The HTTP server, carrying the `Host` names its handlers answer."""

    allowed_hosts: frozenset[str] = frozenset(ALLOWED_HOSTS)


class Handler(BaseHTTPRequestHandler):
    """One request: a page read, or one of the UI's two write paths."""

    server_version = "agb-ui"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802
        """One of the UI's write paths. The handler is passed the fields the form posted and
        answers with a page carrying the reply."""
        if not self._allowed_host():
            return
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        write = ui.WRITES.get(path)
        if write is None:
            if path in ui.ROUTES:
                self._respond(405, ui.not_allowed(path), allow="GET, HEAD")
            else:
                self._respond(404, ui.not_found(path))
            return
        if not self._same_origin():
            self.close_connection = True
            self._respond(403, "<!doctype html>\n<p>That request did not come from here.\n")
            return
        fields = self._fields()
        if fields is None:
            self._respond(400, "<!doctype html>\n<p>That form was not readable.\n")
            return
        try:
            with ui.connect(write=True) as conn:
                body = write(conn, fields, datetime.now(UTC))
        except psycopg.Error:
            log.exception("%s failed", path)
            self._respond(503, "<!doctype html>\n<p>The database is unreachable.\n")
            return
        self._respond(200, body)

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
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        view = ui.ROUTES.get(path)
        try:
            with ui.connect() as conn:
                body = (
                    view(conn, datetime.now(UTC)) if view else ui.not_found(self.path.split("?")[0])
                )
        except psycopg.Error:
            log.exception("%s failed", path)
            self._respond(503, "<!doctype html>\n<p>The database is unreachable.\n")
            return
        self._respond(200 if view else 404, body)

    def do_HEAD(self) -> None:  # noqa: N802
        if not self._allowed_host():
            return
        self._respond(200 if ui.ROUTES.get(self.path.rstrip("/") or "/") else 404, "", head=True)

    def _respond(
        self, status: int, body: str, head: bool = False, allow: str | None = None
    ) -> None:
        encoded = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        if allow is not None:
            self.send_header("Allow", allow)
        self.send_header("Content-Length", str(len(encoded)))
        # Nothing here is cacheable: every page is the current state of the database.
        self.send_header("Cache-Control", "no-store")
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
