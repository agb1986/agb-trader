"""The local UI service: an HTTP server on 127.0.0.1 that renders the pages in `agb.ui`.

It binds the loopback address only, so nothing on the network can reach it; the operator
views it over an SSH tunnel, and SSH is the authentication. There is no login page, no
session cookie and no outbound connection of any kind.

Every request opens its own read-only connection as the `ui` role and closes it again, which
is both what a threaded server needs — a psycopg connection is not safe to share between
threads — and the cheapest way to be certain a page cannot leave a transaction open. Only GET
is answered. The pages are server-rendered HTML with no JavaScript, so there is nothing to
build and nothing to serve besides the pages themselves.

The main thread runs the heartbeat loop on its own connection, since a heartbeat is a write;
the server runs in a daemon thread beside it. SIGTERM stops the loop, which shuts the server
down, so `docker stop` ends it cleanly.
"""

import logging
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

from agb import ui
from agb.heartbeat import interval_from_env, serve, stop_on_signals

log = logging.getLogger(__name__)

# Loopback only. The port is fixed: the operator's tunnel and docs/architecture/06-local-ui.md
# both name it, and there is nothing to configure on a machine that serves one operator.
HOST = "127.0.0.1"
PORT = 8080
ACTOR = "ui"


class Handler(BaseHTTPRequestHandler):
    """One request. Reads the database, renders a page, and writes nothing."""

    server_version = "agb-ui"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802  (the stdlib's method name)
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
        self._respond(200 if ui.ROUTES.get(self.path.rstrip("/") or "/") else 404, "", head=True)

    def _respond(self, status: int, body: str, head: bool = False) -> None:
        encoded = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        # Nothing here is cacheable: every page is the current state of the database.
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not head:
            self.wfile.write(encoded)

    def log_message(self, format: str, *args) -> None:
        log.info("%s %s", self.address_string(), format % args)


def server() -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    httpd.daemon_threads = True
    return httpd


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    stop = stop_on_signals()
    httpd = server()
    threading.Thread(target=httpd.serve_forever, name="agb-ui", daemon=True).start()
    log.info("ui listening on http://%s:%d", HOST, PORT)
    try:
        serve(ACTOR, interval_from_env(), stop)
    finally:
        httpd.shutdown()
        httpd.server_close()


if __name__ == "__main__":
    main()
