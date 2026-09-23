import json
import threading
import time
from datetime import date

import pytest
from edgar_fake import AS_OF, COMPANIES, USER_AGENT, FakeEdgar, client, submissions

from agb import edgar


def test_refuses_to_start_without_a_user_agent(monkeypatch):
    monkeypatch.setenv("EDGAR_USER_AGENT", "  ")
    with pytest.raises(ValueError, match="EDGAR_USER_AGENT"):
        edgar.Edgar.from_env()
    monkeypatch.delenv("EDGAR_USER_AGENT")
    with pytest.raises(ValueError, match="EDGAR_USER_AGENT"):
        edgar.Edgar.from_env()


def test_every_request_declares_the_user_agent(monkeypatch):
    monkeypatch.setenv("EDGAR_USER_AGENT", USER_AGENT)
    fake = FakeEdgar()
    client = edgar.Edgar.from_env()
    client._opener = fake

    edgar.retrieve(client, "ACME", AS_OF)

    assert len(fake.requests) > 3
    assert {agent for _, agent in fake.requests} == {USER_AGENT}


class FakeClock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def test_throttle_spaces_calls_at_the_rate():
    clock = FakeClock()
    throttle = edgar.Throttle(8.0, clock=clock, sleep=clock.sleep)

    starts = []
    for _ in range(5):
        throttle.wait()
        starts.append(clock.now)

    assert starts == pytest.approx([100.0, 100.125, 100.25, 100.375, 100.5])
    clock.now += 10
    throttle.wait()
    assert len(clock.sleeps) == 4


def test_stays_under_ten_requests_a_second_whatever_the_concurrency():
    stamps = []
    lock = threading.Lock()

    class Stamping(FakeEdgar):
        def __call__(self, request):
            with lock:
                stamps.append(time.monotonic())
            return super().__call__(request)

    client = edgar.Edgar(USER_AGENT, opener=Stamping())

    def fetch():
        for _ in range(4):
            client.submissions("0000000123")

    threads = [threading.Thread(target=fetch) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    stamps.sort()
    assert len(stamps) == 16
    # Any ten consecutive requests span more than a second.
    assert min(stamps[i + 10] - stamps[i] for i in range(len(stamps) - 10)) > 1.0


def test_retrieves_the_latest_10k_10q_and_the_8ks_since():
    fake = FakeEdgar()
    result = edgar.retrieve(client(fake), "ACME", AS_OF)

    assert result.outcome == "retrieved"
    assert result.cik == "0000000123"
    assert result.name == "ACME WIDGETS CORP /DE/"
    assert result.aliases == ["ACME WIDGETS CORP /DE/", "Acme Holdings Inc"]
    assert result.tickers == ["ACME", "ACMEW"]
    assert result.delinquent is None
    assert result.ipo is False
    assert result.detail is None
    read = [(s.filing.form, s.filing.filed, s.section) for s in result.sections]
    assert read == [
        ("10-K", date(2026, 2, 20), "Item 1A"),
        ("10-K", date(2026, 2, 20), "Item 7"),
        ("10-Q", date(2026, 5, 1), "Full text"),
        ("8-K", date(2026, 6, 10), "Full text"),
        ("8-K", date(2026, 8, 15), "Full text"),
    ]
    # Nothing filed after the as-of date is even fetched.
    assert not any("q3.htm" in url or "k24.htm" in url for url, _ in fake.requests)


def test_cuts_the_10k_to_items_1a_and_7():
    result = edgar.retrieve(client(), "ACME", AS_OF)
    risk, mdna = result.sections[0].text, result.sections[1].text

    assert risk.startswith("Item 1A. Risk Factors")
    assert "62% of our revenue" in risk
    assert "Unresolved Staff Comments" not in risk
    assert mdna.startswith("Item 7. Management’s Discussion")
    assert "leverage covenant" in mdna
    assert "Market Risk" not in mdna
    assert "color: red" not in result.sections[2].text
    assert result.sections[0].raw.startswith(b"<html>")


def test_records_an_nt_10k_filer_as_delinquent_and_still_reads_it():
    result = edgar.retrieve(client(), "LATE", AS_OF)

    assert result.outcome == "retrieved"
    assert result.delinquent == "nt_10k"
    assert [s.filing.form for s in result.sections] == ["10-K", "10-K", "10-Q"]


def test_records_a_stale_10k_as_delinquent():
    result = edgar.retrieve(client(), "STALE", AS_OF)

    assert result.outcome == "retrieved"
    assert result.delinquent == "late_10k"
    # Fifteen months after the fiscal year end it was not yet late.
    assert edgar.retrieve(client(), "STALE", date(2025, 9, 30)).delinquent is None


def test_reads_a_recent_ipo_from_its_final_prospectus():
    result = edgar.retrieve(client(), "NEWCO", AS_OF)

    assert result.outcome == "retrieved"
    assert result.ipo is True
    assert result.delinquent is None
    [section] = result.sections
    assert (section.filing.form, section.section) == ("424B4", "Risk Factors")
    assert "accumulated deficit of $412 million" in section.text
    assert "Forward-looking statements" not in section.text


def test_reads_a_recent_ipo_from_its_s1_before_the_prospectus_is_filed():
    fake = FakeEdgar()
    newco = COMPANIES["NEWCO"]
    fake.pages["https://data.sec.gov/submissions/CIK0000001011.json"] = json.dumps(
        submissions({**newco, "filings": newco["filings"][1:]})
    ).encode()

    result = edgar.retrieve(client(fake), "NEWCO", AS_OF)

    assert result.ipo is True
    assert [(s.filing.form, s.section) for s in result.sections] == [("S-1", "Risk Factors")]


def test_a_name_with_nothing_to_read_has_no_filings():
    result = edgar.retrieve(client(), "SHELL", AS_OF)

    assert result.outcome == "no_filings"
    assert result.detail
    assert result.sections == []


def test_an_edgar_failure_is_recorded_not_raised():
    result = edgar.retrieve(client(), "DOWN", AS_OF)

    assert result.outcome == "edgar_failure"
    assert result.cik == "0000001415"
    assert "connection reset by peer" in result.detail
    assert result.sections == []


def test_an_unlisted_ticker_and_a_failing_document_are_edgar_failures():
    assert "not in EDGAR's ticker list" in edgar.retrieve(client(), "NOPE", AS_OF).detail

    fake = FakeEdgar()
    fake.failing.add("/k.htm")
    result = edgar.retrieve(client(fake), "ACME", AS_OF)
    assert result.outcome == "edgar_failure"
    assert result.name == "ACME WIDGETS CORP /DE/"
    assert "k.htm" in result.detail


def test_a_10k_without_its_items_is_an_edgar_failure():
    fake = FakeEdgar()
    for url in fake.pages:
        if url.endswith("/k.htm"):
            fake.pages[url] = b"<html><body><p>Nothing here.</p></body></html>"
    result = edgar.retrieve(client(fake), "ACME", AS_OF)

    assert result.outcome == "edgar_failure"
    assert "Item 1A not found" in result.detail


def test_class_share_tickers_match_edgar_dashes():
    fake = FakeEdgar()
    fake.pages[edgar.TICKERS_URL] = b'{"0": {"cik_str": 1067983, "ticker": "BRK-B"}}'

    assert client(fake).cik("BRK.B") == "0001067983"
