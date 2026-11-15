"""The broker client's retry: a read that does not get an answer the broker stands by is
attempted again, and nothing else is.

The opener is a fake that serves a queued answer per attempt and records every request, and
the clock's sleep is a recorder, so the waits are asserted rather than waited for.
"""

import io
import urllib.error

import pytest

from agb.alpaca import BACKOFF, RETRIES, Alpaca, AlpacaError

KEY = "key-aaa"
SECRET = "secret-bbb"
ACCOUNT = b'{"status": "ACTIVE", "equity": "100.00"}'
CONFIGURATION = b'{"suspend_trade": true, "no_shorting": false}'


class FakeBroker:
    """Serves `answers` one per attempt and records each request.

    An answer is either bytes, which is a 200 with that body, an `Exception`, which is
    raised, or a `(status, body, headers)` triple, which is Alpaca refusing.
    """

    def __init__(self, *answers):
        self.answers = list(answers)
        self.requests: list[tuple[str, str]] = []

    def open(self, request, timeout=None):
        self.requests.append((request.get_method(), request.full_url))
        answer = self.answers.pop(0) if self.answers else b"{}"
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, tuple):
            status, body, headers = answer
            raise urllib.error.HTTPError(
                request.full_url, status, "refused", headers, io.BytesIO(body)
            )
        return io.BytesIO(answer)


def failing(status: int, retry_after: str | None = None):
    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    return (status, b'{"message": "no", "code": 42910000}', headers)


class Clock:
    def __init__(self):
        self.waits: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.waits.append(seconds)


def client(broker: FakeBroker, clock: Clock, **kwargs) -> Alpaca:
    return Alpaca(KEY, SECRET, opener=broker, sleep=clock, **kwargs)


def test_a_read_that_times_out_is_attempted_again():
    broker = FakeBroker(urllib.error.URLError("timed out"), ACCOUNT)
    clock = Clock()
    account = client(broker, clock).account()
    assert account.status == "ACTIVE"
    assert len(broker.requests) == 2
    assert clock.waits == [BACKOFF]


def test_the_backoff_doubles_and_the_attempts_are_bounded():
    broker = FakeBroker(*[failing(503)] * (RETRIES + 1))
    clock = Clock()
    with pytest.raises(AlpacaError) as caught:
        client(broker, clock).account()
    assert caught.value.status == 503
    assert len(broker.requests) == RETRIES + 1
    assert clock.waits == [BACKOFF * 2**n for n in range(RETRIES)]


def test_a_rate_limit_is_attempted_again():
    broker = FakeBroker(failing(429), ACCOUNT)
    clock = Clock()
    assert client(broker, clock).account().status == "ACTIVE"
    assert len(broker.requests) == 2


def test_a_refusal_is_not_attempted_again():
    broker = FakeBroker(failing(404))
    clock = Clock()
    with pytest.raises(AlpacaError) as caught:
        client(broker, clock).asset("ACME")
    assert caught.value.not_found
    assert len(broker.requests) == 1
    assert clock.waits == []


def test_a_write_that_times_out_is_not_attempted_again():
    """A PATCH or POST may have reached the broker, so the silence is the caller's to
    decide about. The order path is the one this protects."""
    broker = FakeBroker(urllib.error.URLError("timed out"), CONFIGURATION)
    clock = Clock()
    with pytest.raises(AlpacaError):
        client(broker, clock).suspend(True)
    assert len(broker.requests) == 1
    assert clock.waits == []


def test_a_write_that_is_rate_limited_is_not_attempted_again():
    broker = FakeBroker(failing(429), CONFIGURATION)
    clock = Clock()
    with pytest.raises(AlpacaError):
        client(broker, clock).suspend(True)
    assert len(broker.requests) == 1


def test_the_broker_s_own_retry_after_is_waited_for():
    broker = FakeBroker(failing(429, "2"), ACCOUNT)
    clock = Clock()
    assert client(broker, clock).account().status == "ACTIVE"
    assert clock.waits == [2.0]


def test_a_retry_after_past_the_cap_gives_up():
    """A broker asking for a minute is asking for longer than one cycle should wait on it."""
    broker = FakeBroker(failing(429, "60"), ACCOUNT)
    clock = Clock()
    with pytest.raises(AlpacaError) as caught:
        client(broker, clock).account()
    assert caught.value.retry_after == 60.0
    assert len(broker.requests) == 1
    assert clock.waits == []


def test_an_unreadable_retry_after_leaves_the_plain_backoff_in_charge():
    broker = FakeBroker(failing(429, "Wed, 21 Oct 2026 07:28:00 GMT"), ACCOUNT)
    clock = Clock()
    assert client(broker, clock).account().status == "ACTIVE"
    assert clock.waits == [BACKOFF]


def test_retries_can_be_turned_off():
    broker = FakeBroker(failing(503), ACCOUNT)
    clock = Clock()
    with pytest.raises(AlpacaError):
        client(broker, clock, retries=0).account()
    assert len(broker.requests) == 1


def test_no_key_reaches_the_message_of_a_retried_failure():
    broker = FakeBroker(*[urllib.error.URLError(f"{KEY} {SECRET} refused")] * (RETRIES + 1))
    clock = Clock()
    with pytest.raises(AlpacaError) as caught:
        client(broker, clock).account()
    assert KEY not in str(caught.value)
    assert SECRET not in str(caught.value)


def test_an_order_answer_without_an_id_is_a_broker_error_not_a_key_error():
    broker = FakeBroker(b'{"symbol": "AAA", "side": "buy", "status": "new"}')
    with pytest.raises(AlpacaError) as caught:
        client(broker, Clock()).order("x")
    assert caught.value.status is None
    assert "order: the broker's answer is malformed" in str(caught.value)
    assert "'id'" in str(caught.value)


def test_a_submission_answered_without_an_id_is_not_sent_again():
    broker = FakeBroker(b'{"symbol": "AAA", "side": "buy", "status": "new"}')
    with pytest.raises(AlpacaError) as caught:
        client(broker, Clock()).submit("AAA", 5, "buy", "2026-10-AAA")
    assert caught.value.status is None
    assert len(broker.requests) == 1


def test_an_answer_of_the_wrong_shape_is_a_broker_error():
    broker = FakeBroker(b'[{"symbol": "AAA"}]')
    with pytest.raises(AlpacaError) as caught:
        client(broker, Clock()).account()
    assert "account: the broker's answer is malformed" in str(caught.value)
