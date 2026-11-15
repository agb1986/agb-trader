"""A fake SEC EDGAR for the panel's tests: a handful of companies, each with the filing history
one retrieval case needs, served through the opener the client takes in place of urlopen.

    ACME   a regular filer: a 10-K, a 10-Q, two 8-Ks since and a 10-Q filed after AS_OF
    LATE   filed an NT 10-K after its latest 10-K
    STALE  its latest 10-K is for a fiscal year that ended more than 15 months before AS_OF
    NEWCO  a recent IPO: an S-1 and a final prospectus, no 10-K yet
    SHELL  only 8-Ks, nothing the Analyst can read
    DOWN   its submissions page fails
"""

import io
import json
import threading
import urllib.error
from contextlib import contextmanager
from datetime import date

from agb.edgar import Edgar, Throttle

USER_AGENT = "agb-trader test test@example.com"
AS_OF = date(2026, 9, 30)

TEN_K = """<html><head><title>acme-10k</title><style>p {color: red}</style></head><body>
<p>ACME WIDGETS CORP</p>
<table><tr><td>Item 1.</td><td>Business</td><td>3</td></tr>
<tr><td>Item 1A.</td><td>Risk Factors</td><td>12</td></tr>
<tr><td>Item 1B.</td><td>Unresolved Staff Comments</td><td>25</td></tr>
<tr><td>Item 7.</td><td>Management's Discussion and Analysis</td><td>30</td></tr>
<tr><td>Item 7A.</td><td>Market Risk</td><td>44</td></tr>
<tr><td>Item 8.</td><td>Financial Statements</td><td>46</td></tr></table>
<p>Item 1. Business</p>
<p>Acme Widgets Corp (NASDAQ: ACME) makes widgets.</p>
<h2>Item 1A. Risk Factors</h2>
<p>Our largest customer, a national retailer, accounted for 62% of our revenue in fiscal 2025,
and the loss of that customer would materially harm Acme.</p>
<p>In March 2025 the Company's former chief financial officer resigned after four months.</p>
<h2>Item 1B. Unresolved Staff Comments</h2>
<p>None.</p>
<h2>Item 7. Management&#8217;s Discussion and Analysis</h2>
<p>Liquidity and Capital Resources</p>
<p>As of December 31, 2025, ACME was not in compliance with the leverage covenant under its
revolving credit facility and has obtained a waiver through June 30, 2026.</p>
<h2>Item 7A. Quantitative and Qualitative Disclosures About Market Risk</h2>
<p>Item 8. Financial Statements</p>
</body></html>"""

TEN_Q = """<html><body>
<p>Acme Widgets Corp quarterly report.</p>
<p>During the quarter we identified a material weakness in our internal control over
financial reporting related to revenue recognition for bill-and-hold arrangements.</p>
</body></html>"""

EIGHT_K = """<html><body><p>Item 5.02 Departure of Directors or Certain Officers.</p>
<p>On {day}, the Board of Acme Widgets Corp appointed a new Chief Financial Officer.</p>
</body></html>"""

S_1 = """<html><body><p>Prospectus summary</p>
<h2>RISK FACTORS</h2>
<p>We have a history of losses and may not achieve profitability. NewCo Robotics, Inc. has an
accumulated deficit of $412 million.</p>
<h2>SPECIAL NOTE REGARDING FORWARD-LOOKING STATEMENTS</h2>
<p>Forward-looking statements.</p>
<h2>Use of Proceeds</h2>
</body></html>"""


def filing(form, accession, filed, report="", document="doc.htm"):
    return (form, accession, filed, report, document)


COMPANIES = {
    "ACME": {
        "cik": 123,
        "name": "ACME WIDGETS CORP /DE/",
        "former": ["Acme Holdings Inc"],
        "tickers": ["ACME", "ACMEW"],
        "filings": [
            filing("10-Q", "0000000123-26-000009", "2026-10-30", "2026-09-30", "q3.htm"),
            filing("8-K", "0000000123-26-000008", "2026-08-15", "", "ek2.htm"),
            filing("8-K", "0000000123-26-000007", "2026-06-10", "", "ek1.htm"),
            filing("10-Q", "0000000123-26-000006", "2026-05-01", "2026-03-31", "q1.htm"),
            filing("8-K", "0000000123-26-000005", "2026-04-02", "", "ek0.htm"),
            filing("10-K", "0000000123-26-000004", "2026-02-20", "2025-12-31", "k.htm"),
            filing("10-K", "0000000123-25-000002", "2025-02-21", "2024-12-31", "k24.htm"),
        ],
        "documents": {
            "q3.htm": TEN_Q,
            "ek2.htm": EIGHT_K.format(day="August 14, 2026"),
            "ek1.htm": EIGHT_K.format(day="June 9, 2026"),
            "q1.htm": TEN_Q,
            "ek0.htm": EIGHT_K.format(day="April 1, 2026"),
            "k.htm": TEN_K,
            "k24.htm": TEN_K,
        },
    },
    "LATE": {
        "cik": 456,
        "name": "LATE FILER INC",
        "tickers": ["LATE"],
        "filings": [
            filing("NT 10-K", "0000000456-26-000003", "2026-04-01"),
            filing("10-Q", "0000000456-25-000002", "2025-11-10", "2025-09-30", "q.htm"),
            filing("10-K", "0000000456-25-000001", "2025-03-01", "2024-12-31", "k.htm"),
        ],
        "documents": {"q.htm": TEN_Q, "k.htm": TEN_K},
    },
    "STALE": {
        "cik": 789,
        "name": "STALE CO",
        "tickers": ["STALE"],
        "filings": [
            filing("10-K", "0000000789-24-000001", "2024-09-15", "2024-06-30", "k.htm"),
        ],
        "documents": {"k.htm": TEN_K},
    },
    "NEWCO": {
        "cik": 1011,
        "name": "NewCo Robotics, Inc.",
        "tickers": ["NEWC"],
        "filings": [
            filing("424B4", "0000001011-26-000002", "2026-07-02", "", "p.htm"),
            filing("S-1", "0000001011-26-000001", "2026-05-20", "", "s1.htm"),
        ],
        "documents": {"p.htm": S_1, "s1.htm": S_1},
    },
    "SHELL": {
        "cik": 1213,
        "name": "SHELL CORP",
        "tickers": ["SHELL"],
        "filings": [filing("8-K", "0000001213-26-000001", "2026-06-01", "", "e.htm")],
        "documents": {"e.htm": EIGHT_K.format(day="June 1, 2026")},
    },
    "DOWN": {"cik": 1415, "name": "DOWN INC", "tickers": ["DOWN"], "filings": [], "documents": {}},
}


def submissions(company):
    rows = company["filings"]
    return {
        "cik": str(company["cik"]),
        "name": company["name"],
        "tickers": company["tickers"],
        "formerNames": [{"name": name} for name in company.get("former", [])],
        "filings": {
            "recent": {
                "form": [r[0] for r in rows],
                "accessionNumber": [r[1] for r in rows],
                "filingDate": [r[2] for r in rows],
                "reportDate": [r[3] for r in rows],
                "primaryDocument": [r[4] for r in rows],
            }
        },
    }


def pages():
    out = {
        "https://www.sec.gov/files/company_tickers.json": json.dumps(
            {
                str(n): {"cik_str": c["cik"], "ticker": ticker, "title": c["name"]}
                for n, (ticker, c) in enumerate(COMPANIES.items())
            }
        ).encode()
    }
    for company in COMPANIES.values():
        cik = company["cik"]
        out[f"https://data.sec.gov/submissions/CIK{cik:010d}.json"] = json.dumps(
            submissions(company)
        ).encode()
        for _, accession, _, _, document in company["filings"]:
            if document not in company["documents"]:
                continue
            url = (
                f"https://www.sec.gov/Archives/edgar/data/{cik}/"
                f"{accession.replace('-', '')}/{document}"
            )
            out[url] = company["documents"][document].encode()
    return out


class FakeEdgar:
    """The opener: serves `pages()`, records each request, and fails any URL containing a
    string in `failing`."""

    def __init__(self):
        self.pages = pages()
        self.failing = {"CIK0000001415"}
        self.requests = []
        self._lock = threading.Lock()

    @contextmanager
    def __call__(self, request):
        url = request.full_url
        with self._lock:
            self.requests.append((url, request.get_header("User-agent")))
        if any(fragment in url for fragment in self.failing):
            raise urllib.error.URLError("connection reset by peer")
        if url not in self.pages:
            raise urllib.error.HTTPError(url, 404, "Not Found", None, None)
        yield io.BytesIO(self.pages[url])


def client(fake=None):
    """An Edgar client on the fake, unthrottled and not pausing between retries."""
    return Edgar(
        USER_AGENT, opener=fake or FakeEdgar(), throttle=Throttle(1e9), sleep=lambda seconds: None
    )
