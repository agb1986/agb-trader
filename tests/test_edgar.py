import json
import threading
import time
import urllib.error
from datetime import date

import pytest
from edgar_fake import (
    AS_OF,
    COMPANIES,
    TEN_K,
    USER_AGENT,
    FakeEdgar,
    client,
    submissions,
)

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


def test_a_10k_without_its_items_is_read_whole_and_marked():
    fake = FakeEdgar()
    for url in fake.pages:
        if url.endswith("/k.htm"):
            fake.pages[url] = (
                b"<html><body><p>Part I.</p><h2>Principal Risks</h2>"
                b"<p>Our largest customer is 62% of revenue.</p></body></html>"
            )
    result = edgar.retrieve(client(fake), "ACME", AS_OF)

    assert result.outcome == "retrieved"
    annual = [s for s in result.sections if s.filing.form == "10-K"]
    assert [s.section for s in annual] == ["Whole document (Item 1A, Item 7 not found)"]
    assert "62% of revenue" in annual[0].text


def test_a_10k_with_one_missing_heading_is_read_whole_and_names_it():
    page = TEN_K.replace("Management&#8217;s Discussion", "Operating review")
    page = page.replace("Management's Discussion", "Operating review")
    fake = FakeEdgar()
    for url in fake.pages:
        if url.endswith("/k.htm"):
            fake.pages[url] = page.encode()
    result = edgar.retrieve(client(fake), "ACME", AS_OF)

    annual = [s for s in result.sections if s.filing.form == "10-K"]
    assert [s.section for s in annual] == ["Whole document (Item 7 not found)"]
    assert "leverage covenant" in annual[0].text


def test_a_prospectus_without_a_risk_factors_heading_is_read_whole_and_marked():
    fake = FakeEdgar()
    for url in fake.pages:
        if url.endswith("/p.htm"):
            fake.pages[url] = b"<html><body><p>We have a history of losses.</p></body></html>"
    result = edgar.retrieve(client(fake), "NEWCO", AS_OF)

    assert result.outcome == "retrieved"
    assert [s.section for s in result.sections] == ["Whole document (Risk Factors not found)"]


def test_an_empty_10k_is_still_an_edgar_failure():
    fake = FakeEdgar()
    for url in fake.pages:
        if url.endswith("/k.htm"):
            fake.pages[url] = b"<html><body></body></html>"
    result = edgar.retrieve(client(fake), "ACME", AS_OF)

    assert result.outcome == "edgar_failure"
    assert "empty document" in result.detail


class Flaky(FakeEdgar):
    """Answers the first `times` requests for any URL containing `fragment` with `error`."""

    def __init__(self, fragment, error, times=1):
        super().__init__()
        self.fragment, self.error, self.left = fragment, error, times

    def __call__(self, request):
        if self.fragment in request.full_url and self.left > 0:
            self.left -= 1
            self.requests.append((request.full_url, request.get_header("User-agent")))
            raise self.error(request.full_url)
        return super().__call__(request)


def http_error(code, headers=None):
    return lambda url: urllib.error.HTTPError(url, code, "error", headers, None)


def test_one_503_then_success_scores_the_name_normally():
    fake = Flaky("CIK0000000123", http_error(503))
    pauses = []
    throttled = []

    class CountingThrottle(edgar.Throttle):
        def wait(self):
            throttled.append(1)

    result = edgar.retrieve(
        edgar.Edgar(USER_AGENT, opener=fake, throttle=CountingThrottle(1e9), sleep=pauses.append),
        "ACME",
        AS_OF,
    )

    assert result.outcome == "retrieved"
    assert result.detail is None
    assert len(result.sections) == 5
    assert pauses == [edgar.BACKOFF]
    # The retry took its own slot in the rate limit.
    assert len(throttled) == len(fake.requests)


def test_a_dropped_connection_and_a_429_are_retried_with_a_growing_pause():
    for error, times in ((lambda url: urllib.error.URLError("reset"), 2), (http_error(429), 2)):
        pauses = []
        fake = Flaky("CIK0000000123", error, times=times)
        client_ = edgar.Edgar(
            USER_AGENT, opener=fake, throttle=edgar.Throttle(1e9), sleep=pauses.append
        )

        assert client_.submissions("0000000123")["name"] == "ACME WIDGETS CORP /DE/"
        assert pauses == [edgar.BACKOFF, edgar.BACKOFF * 2]


def test_retry_after_sets_the_pause_up_to_a_ceiling():
    for header, expected in (("7", 7.0), ("9999", edgar.MAX_PAUSE), ("soon", edgar.BACKOFF)):
        pauses = []
        fake = Flaky("CIK0000000123", http_error(429, {"Retry-After": header}))
        client_ = edgar.Edgar(
            USER_AGENT, opener=fake, throttle=edgar.Throttle(1e9), sleep=pauses.append
        )

        client_.submissions("0000000123")
        assert pauses == [expected]


def test_a_persistent_error_gives_up_after_the_retries_and_is_an_edgar_failure():
    pauses = []
    fake = Flaky("CIK0000000123", http_error(503), times=100)
    result = edgar.retrieve(
        edgar.Edgar(USER_AGENT, opener=fake, throttle=edgar.Throttle(1e9), sleep=pauses.append),
        "ACME",
        AS_OF,
    )

    assert result.outcome == "edgar_failure"
    assert "HTTP 503" in result.detail
    assert len(pauses) == edgar.RETRIES
    assert sum("CIK0000000123" in url for url, _ in fake.requests) == edgar.RETRIES + 1


def test_a_client_error_is_not_retried():
    pauses = []
    fake = FakeEdgar()
    fake.pages.pop("https://data.sec.gov/submissions/CIK0000000123.json")
    client_ = edgar.Edgar(
        USER_AGENT, opener=fake, throttle=edgar.Throttle(1e9), sleep=pauses.append
    )

    with pytest.raises(edgar.EdgarError, match="HTTP 404"):
        client_.submissions("0000000123")
    assert pauses == []
    assert len(fake.requests) == 1


def test_threads_share_one_fetch_of_the_ticker_list():
    fake = FakeEdgar()
    shared = client(fake)
    found = []
    barrier = threading.Barrier(8)

    def look():
        barrier.wait()
        found.append(shared.cik("ACME"))

    threads = [threading.Thread(target=look) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert found == ["0000000123"] * 8
    assert sum(url == edgar.TICKERS_URL for url, _ in fake.requests) == 1


def busy_company(fake, as_of_filings=None):
    """ACME with only 8-Ks in `recent`: its 10-K and 10-Q sit on an older page."""
    company = COMPANIES["ACME"]
    recent = [row for row in company["filings"] if row[0] == "8-K"]
    older = [row for row in company["filings"] if row[0] != "8-K"]
    body = submissions({**company, "filings": recent})
    body["filings"]["files"] = [
        {
            "name": "CIK0000000123-submissions-001.json",
            "filingFrom": "2025-02-21",
            "filingTo": "2026-10-30",
        },
        {
            "name": "CIK0000000123-submissions-002.json",
            "filingFrom": "2010-01-01",
            "filingTo": "2025-02-20",
        },
    ]
    fake.pages["https://data.sec.gov/submissions/CIK0000000123.json"] = json.dumps(body).encode()
    page = submissions({**company, "filings": older})["filings"]["recent"]
    fake.pages["https://data.sec.gov/submissions/CIK0000000123-submissions-001.json"] = json.dumps(
        page
    ).encode()
    fake.pages["https://data.sec.gov/submissions/CIK0000000123-submissions-002.json"] = b"{}"


def test_the_older_pages_are_read_when_recent_does_not_reach_a_10k():
    fake = FakeEdgar()
    busy_company(fake)
    result = edgar.retrieve(client(fake), "ACME", AS_OF)

    assert result.outcome == "retrieved"
    read = [(s.filing.form, s.filing.filed, s.section) for s in result.sections]
    assert read == [
        ("10-K", date(2026, 2, 20), "Item 1A"),
        ("10-K", date(2026, 2, 20), "Item 7"),
        ("10-Q", date(2026, 5, 1), "Full text"),
        ("8-K", date(2026, 6, 10), "Full text"),
        ("8-K", date(2026, 8, 15), "Full text"),
    ]
    urls = [url for url, _ in fake.requests]
    assert "https://data.sec.gov/submissions/CIK0000000123-submissions-001.json" in urls
    # The 10-K was on the first older page, so the second is never fetched.
    assert not any("submissions-002" in url for url in urls)


def test_the_older_pages_are_not_read_when_recent_has_a_10k():
    fake = FakeEdgar()
    body = submissions(COMPANIES["ACME"])
    body["filings"]["files"] = [
        {
            "name": "CIK0000000123-submissions-001.json",
            "filingFrom": "2000-01-01",
            "filingTo": "2025-01-01",
        }
    ]
    fake.pages["https://data.sec.gov/submissions/CIK0000000123.json"] = json.dumps(body).encode()

    result = edgar.retrieve(client(fake), "ACME", AS_OF)

    assert result.outcome == "retrieved"
    assert not any("submissions-001" in url for url, _ in fake.requests)


def test_an_older_page_that_fails_is_an_edgar_failure():
    fake = FakeEdgar()
    busy_company(fake)
    fake.failing.add("submissions-001")

    result = edgar.retrieve(client(fake), "ACME", AS_OF)

    assert result.outcome == "edgar_failure"
    assert "submissions-001" in result.detail


def test_a_page_filed_after_the_as_of_date_is_skipped():
    fake = FakeEdgar()
    busy_company(fake)
    body = json.loads(fake.pages["https://data.sec.gov/submissions/CIK0000000123.json"])
    body["filings"]["files"].insert(
        0,
        {
            "name": "CIK0000000123-submissions-009.json",
            "filingFrom": "2026-11-01",
            "filingTo": "2026-12-31",
        },
    )
    fake.pages["https://data.sec.gov/submissions/CIK0000000123.json"] = json.dumps(body).encode()

    result = edgar.retrieve(client(fake), "ACME", AS_OF)

    assert result.outcome == "retrieved"
    assert not any("submissions-009" in url for url, _ in fake.requests)


def test_class_share_tickers_match_edgar_dashes():
    fake = FakeEdgar()
    fake.pages[edgar.TICKERS_URL] = b'{"0": {"cik_str": 1067983, "ticker": "BRK-B"}}'

    assert client(fake).cik("BRK.B") == "0001067983"
