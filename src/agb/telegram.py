"""A minimal Telegram Bot API client on the standard library.

The bot reaches Telegram only through its VPN's HTTP proxy (docs/architecture/07). urllib
takes the proxy from HTTPS_PROXY and NO_PROXY, and without one it would silently connect
direct and put the server's own address in Telegram's logs. So the client refuses to exist
unless a proxy is set for the API's scheme and NO_PROXY does not exempt the API's host.

The token is part of every request URL. It is never logged, and it is scrubbed from every
error this module raises.

Run inside the bot's container, `python -m agb.telegram` checks the token and the path to
Telegram without sending a message.
"""

import http.client
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Protocol

API = "https://api.telegram.org"
TIMEOUT = 10.0


class TelegramError(Exception):
    """A request that did not succeed: Telegram unreachable, or its answer not ok."""


class Messenger(Protocol):
    """What the bot needs from Telegram; tests pass a fake."""

    def send_message(self, chat_id: str, text: str) -> int: ...

    def get_updates(self, offset: int | None) -> list[dict[str, Any]]: ...


class Telegram:
    def __init__(self, token: str, *, api: str = API, timeout: float = TIMEOUT) -> None:
        if not token:
            raise ValueError("TELEGRAM_BOT_TOKEN is not set")
        parts = urllib.parse.urlsplit(api)
        proxy = urllib.request.getproxies().get(parts.scheme)
        if not proxy:
            raise ValueError(
                f"{parts.scheme.upper()}_PROXY is not set: the bot reaches Telegram only "
                "through the VPN's proxy"
            )
        if urllib.request.proxy_bypass(parts.hostname or ""):
            raise ValueError(
                f"NO_PROXY exempts {parts.hostname}: the bot reaches Telegram only through "
                "the VPN's proxy"
            )
        self.proxy = proxy
        self._token = token
        self._base = f"{api.rstrip('/')}/bot{token}/"
        self._timeout = timeout
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({parts.scheme: proxy})
        )

    @classmethod
    def from_env(cls) -> "Telegram":
        return cls(os.environ.get("TELEGRAM_BOT_TOKEN", ""))

    def _scrub(self, text: str) -> str:
        return text.replace(self._token, "<token>")

    def _call(self, method: str, params: dict[str, Any], timeout: float = 0.0) -> Any:
        request = urllib.request.Request(
            self._base + method,
            data=json.dumps(params).encode(),
            headers={"Content-Type": "application/json"},
        )
        # Every error is re-raised `from None`, so no traceback carries the request URL.
        try:
            with self._opener.open(request, timeout=self._timeout + timeout) as response:
                answer = json.load(response)
        except urllib.error.HTTPError as exc:
            # Telegram explains a refusal in a JSON body, whatever the status.
            try:
                answer = json.loads(exc.read())
            except (OSError, ValueError, http.client.HTTPException):
                answer = {"description": f"HTTP {exc.code}"}
        except (OSError, ValueError, http.client.HTTPException) as exc:
            raise TelegramError(self._scrub(f"{method}: {exc}")) from None
        if not isinstance(answer, dict) or answer.get("ok") is not True:
            description = answer.get("description") if isinstance(answer, dict) else None
            raise TelegramError(self._scrub(f"{method}: {description or 'not ok'}"))
        return answer["result"]

    def get_me(self) -> dict[str, Any]:
        return self._call("getMe", {})

    def send_message(self, chat_id: str, text: str) -> int:
        """Send plain text, with no parse mode, and return the new message's id."""
        return int(self._call("sendMessage", {"chat_id": chat_id, "text": text})["message_id"])

    def get_updates(self, offset: int | None, timeout: int = 0) -> list[dict[str, Any]]:
        """New messages since `offset`. Asking from an offset confirms everything before it."""
        params: dict[str, Any] = {"timeout": timeout, "allowed_updates": ["message"]}
        if offset is not None:
            params["offset"] = offset
        return self._call("getUpdates", params, timeout)


def main() -> int:
    try:
        telegram = Telegram.from_env()
        me = telegram.get_me()
    except (ValueError, TelegramError) as exc:
        print(f"telegram: {exc}", file=sys.stderr)
        return 1
    print(f"reached Telegram as @{me.get('username')} through {telegram.proxy}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
