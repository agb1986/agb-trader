"""The Telegram client, against a fake HTTP proxy on a free local port: every request goes
through the proxy, the client refuses to exist without one, and the token never appears in an
error."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from conftest import ROOT

from agb import telegram as telegram_module
from agb.telegram import Telegram, TelegramError

TOKEN = "123456:secret-token"
API = "http://telegram.invalid"
PROXY_VARS = ("http_proxy", "https_proxy", "no_proxy", "all_proxy")


class FakeProxy:
    """Answers every request with the next queued (status, body), and records what it got."""

    def __init__(self) -> None:
        self.requests: list[tuple[str, dict]] = []
        self.answers: list[tuple[int, dict | str]] = []
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers["Content-Length"]))
                proxy.requests.append((self.path, json.loads(body)))
                status, answer = proxy.answers.pop(0)
                data = answer.encode() if isinstance(answer, str) else json.dumps(answer).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"


@pytest.fixture
def no_proxy_env(monkeypatch):
    for name in PROXY_VARS:
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.upper(), raising=False)
    return monkeypatch


@pytest.fixture
def proxy(no_proxy_env):
    fake = FakeProxy()
    thread = threading.Thread(target=fake.server.serve_forever, daemon=True)
    thread.start()
    no_proxy_env.setenv("http_proxy", fake.url)
    yield fake
    fake.server.shutdown()
    fake.server.server_close()


def test_every_request_goes_through_the_proxy(proxy):
    proxy.answers = [
        (200, {"ok": True, "result": {"id": 1, "is_bot": True, "username": "agb_bot"}}),
        (200, {"ok": True, "result": {"message_id": 77, "chat": {"id": 4242}}}),
        (200, {"ok": True, "result": [{"update_id": 5}]}),
    ]
    client = Telegram(TOKEN, api=API)
    assert client.proxy == proxy.url
    assert client.get_me()["username"] == "agb_bot"
    assert client.send_message("4242", "HEALTH ALERT *not markdown*") == 77
    assert client.get_updates(5) == [{"update_id": 5}]
    assert proxy.requests == [
        (f"{API}/bot{TOKEN}/getMe", {}),
        # Plain text: no parse mode, so nothing in an alert is read as markup.
        (
            f"{API}/bot{TOKEN}/sendMessage",
            {"chat_id": "4242", "text": "HEALTH ALERT *not markdown*"},
        ),
        (
            f"{API}/bot{TOKEN}/getUpdates",
            {"timeout": 0, "allowed_updates": ["message"], "offset": 5},
        ),
    ]


def test_an_answer_that_is_not_ok_is_an_error_without_the_token(proxy):
    proxy.answers = [
        (401, {"ok": False, "error_code": 401, "description": "Unauthorized"}),
        (400, {"ok": False, "description": f"Bad Request: echo /bot{TOKEN}/sendMessage"}),
        (502, "<html>Bad Gateway</html>"),
        (200, {"ok": False}),
    ]
    client = Telegram(TOKEN, api=API)
    errors = []
    for _ in proxy.answers[:]:
        with pytest.raises(TelegramError) as caught:
            client.send_message("4242", "hi")
        errors.append(str(caught.value))
    assert errors == [
        "sendMessage: Unauthorized",
        "sendMessage: Bad Request: echo /bot<token>/sendMessage",
        "sendMessage: HTTP 502",
        "sendMessage: not ok",
    ]


def test_an_unreachable_proxy_is_an_error_without_the_token(proxy):
    proxy.server.shutdown()
    proxy.server.server_close()
    client = Telegram(TOKEN, api=API, timeout=2)
    with pytest.raises(TelegramError) as caught:
        client.get_me()
    assert str(caught.value).startswith("getMe: ")
    assert TOKEN not in str(caught.value)
    # Raised `from None`: no chained exception carries the request URL into a traceback.
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


def test_no_client_without_a_proxy(no_proxy_env):
    with pytest.raises(ValueError, match="HTTPS_PROXY is not set"):
        Telegram(TOKEN)
    no_proxy_env.setenv("HTTP_PROXY", "http://vpn:8888")
    with pytest.raises(ValueError, match="HTTPS_PROXY is not set"):
        Telegram(TOKEN)


@pytest.mark.parametrize("no_proxy", ["api.telegram.org", ".telegram.org", "*"])
def test_no_client_when_no_proxy_exempts_telegram(no_proxy_env, no_proxy):
    no_proxy_env.setenv("HTTPS_PROXY", "http://vpn:8888")
    no_proxy_env.setenv("NO_PROXY", no_proxy)
    with pytest.raises(ValueError, match="NO_PROXY exempts api.telegram.org"):
        Telegram(TOKEN)


def test_no_client_without_a_token(no_proxy_env):
    no_proxy_env.setenv("HTTPS_PROXY", "http://vpn:8888")
    no_proxy_env.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    with pytest.raises(ValueError, match="TELEGRAM_BOT_TOKEN is not set"):
        Telegram.from_env()


def test_the_bot_env_example_routes_telegram_through_the_vpn(no_proxy_env):
    for line in (ROOT / "env" / "bot.env.example").read_text().splitlines():
        if line and not line.startswith("#"):
            name, value = line.split("=", 1)
            no_proxy_env.setenv(name, value)
    no_proxy_env.setenv("TELEGRAM_BOT_TOKEN", TOKEN)
    assert Telegram.from_env().proxy == "http://vpn:8888"


def test_the_check_command_fails_without_a_proxy(no_proxy_env, capsys):
    no_proxy_env.setenv("TELEGRAM_BOT_TOKEN", TOKEN)
    assert telegram_module.main() == 1
    assert capsys.readouterr().err.startswith("telegram: HTTPS_PROXY is not set")
