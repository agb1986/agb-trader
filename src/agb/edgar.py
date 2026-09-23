"""SEC EDGAR retrieval for the panel: the filings each shortlisted name is judged on.

For a name and the date the panel runs, point in time (nothing filed after that date counts):

- The latest 10-K, cut down to Item 1A (risk factors) and Item 7 (MD&A).
- The latest 10-Q, whole.
- Every 8-K filed since the later of the two.
- For a recent IPO with no 10-K yet, the risk factors of its latest S-1 or final prospectus.

A name is a delinquent filer when an NT 10-K was filed after its latest 10-K, or when its
latest 10-K's fiscal year ended more than about 15 months ago. That is a finding the rubric
scores (docs/architecture/03, "Missing and stale filings"), so it is recorded, and the name's
filings are still read. EDGAR being unreachable, or a document that cannot be parsed, is the
system failing rather than the company: `retrieve` returns it as an `edgar_failure` outcome,
and the blend gives the name the neutral 0.5.

The SEC's fair-access policy requires every request to declare a User-Agent with a name and a
contact email, and limits a client to ten requests a second. The client takes the User-Agent
from EDGAR_USER_AGENT, refuses to start without it, and spaces its requests at RATE a second
across every thread that shares it.
"""

from __future__ import annotations

import calendar
import html.parser
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import date
from typing import Any, BinaryIO

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"
TIMEOUT = 30.0
# Requests a second, kept clear of the SEC's limit of ten.
RATE = 8.0
# A 10-K is late when its fiscal year ended more than this many months ago.
DELINQUENT_MONTHS = 15

ANNUAL = ("10-K", "10-KT")
QUARTERLY = ("10-Q",)
CURRENT = ("8-K",)
LATE_ANNUAL = ("NT 10-K",)
# A final prospectus first, then the registration statement and its amendments.
IPO = ("424B4", "424B1", "S-1/A", "S-1")

OUTCOMES = ("retrieved", "no_filings", "edgar_failure")

Opener = Callable[[urllib.request.Request], AbstractContextManager[BinaryIO]]


def urlopen(request: urllib.request.Request) -> AbstractContextManager[BinaryIO]:
    return urllib.request.urlopen(request, timeout=TIMEOUT)


class EdgarError(Exception):
    """EDGAR unreachable, or an answer or document that could not be parsed."""


class Throttle:
    """Spaces calls at least 1/rate seconds apart, across every thread that shares it."""

    def __init__(
        self,
        rate: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.interval = 1.0 / rate
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next = float("-inf")

    def wait(self) -> None:
        # The lock is held through the sleep, so each caller takes the next free slot in turn.
        with self._lock:
            now = self._clock()
            if now < self._next:
                self._sleep(self._next - now)
                now = self._next
            self._next = now + self.interval


class Edgar:
    def __init__(
        self, user_agent: str, opener: Opener = urlopen, throttle: Throttle | None = None
    ) -> None:
        if not user_agent or not user_agent.strip():
            raise ValueError(
                "EDGAR_USER_AGENT is not set: the SEC requires a User-Agent with a name and "
                "contact email, as 'agb-trader <name> <email>'"
            )
        self.user_agent = user_agent.strip()
        self._opener = opener
        self._throttle = throttle or Throttle(RATE)
        self._ciks: dict[str, str] | None = None

    @classmethod
    def from_env(cls) -> Edgar:
        return cls(os.environ.get("EDGAR_USER_AGENT", ""))

    def get(self, url: str) -> bytes:
        request = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
        self._throttle.wait()
        try:
            with self._opener(request) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            raise EdgarError(f"{url}: HTTP {exc.code}") from None
        except (urllib.error.URLError, OSError) as exc:
            raise EdgarError(f"{url}: {getattr(exc, 'reason', exc)}") from None

    def get_json(self, url: str) -> Any:
        try:
            return json.loads(self.get(url))
        except ValueError:
            raise EdgarError(f"{url}: not JSON") from None

    def cik(self, ticker: str) -> str | None:
        """The ticker's ten-digit CIK, or None when EDGAR does not list it."""
        if self._ciks is None:
            listing = self.get_json(TICKERS_URL)
            try:
                self._ciks = {
                    str(entry["ticker"]).upper(): f"{int(entry['cik_str']):010d}"
                    for entry in listing.values()
                }
            except (AttributeError, KeyError, TypeError, ValueError):
                raise EdgarError(f"{TICKERS_URL}: unexpected shape") from None
        # EDGAR writes class shares with a dash (BRK-B) where some vendors use a dot.
        return self._ciks.get(ticker.upper()) or self._ciks.get(ticker.upper().replace(".", "-"))

    def submissions(self, cik: str) -> dict[str, Any]:
        return self.get_json(SUBMISSIONS_URL.format(cik=cik))

    def document(self, cik: str, accession: str, name: str) -> bytes:
        url = ARCHIVES_URL.format(cik=int(cik), accession=accession.replace("-", ""), document=name)
        return self.get(url)


@dataclass(frozen=True)
class Filing:
    form: str
    accession: str
    filed: date
    report_date: date | None
    document: str


@dataclass(frozen=True)
class Section:
    """One piece of text the Analyst reads, with the raw document it was cut from."""

    filing: Filing
    section: str
    text: str
    raw: bytes


@dataclass
class Retrieval:
    ticker: str
    outcome: str
    cik: str | None = None
    name: str | None = None
    aliases: list[str] = field(default_factory=list)
    tickers: list[str] = field(default_factory=list)
    delinquent: str | None = None
    ipo: bool = False
    detail: str | None = None
    sections: list[Section] = field(default_factory=list)


def filings(submissions: dict[str, Any], as_of: date) -> list[Filing]:
    """The filings EDGAR lists in `recent`, filed on or before `as_of`, newest first."""
    try:
        recent = submissions["filings"]["recent"]
        rows = zip(
            recent["form"],
            recent["accessionNumber"],
            recent["filingDate"],
            recent["reportDate"],
            recent["primaryDocument"],
            strict=True,
        )
        out = [
            Filing(form, accession, date.fromisoformat(filed), _date(report), document)
            for form, accession, filed, report, document in rows
        ]
    except (KeyError, TypeError, ValueError) as exc:
        raise EdgarError(f"submissions: unexpected shape ({exc})") from None
    out = [f for f in out if f.filed <= as_of]
    return sorted(out, key=lambda f: (f.filed, f.accession), reverse=True)


def _date(text: str) -> date | None:
    return date.fromisoformat(text) if text else None


def _latest(listed: list[Filing], forms: tuple[str, ...]) -> Filing | None:
    return next((f for f in listed if f.form in forms), None)


def _add_months(day: date, months: int) -> date:
    year, month = divmod(day.year * 12 + day.month - 1 + months, 12)
    return date(year, month + 1, min(day.day, calendar.monthrange(year, month + 1)[1]))


def delinquency(listed: list[Filing], as_of: date) -> str | None:
    """'nt_10k' when an NT 10-K followed the latest 10-K, 'late_10k' when the latest 10-K's
    fiscal year ended more than DELINQUENT_MONTHS ago, else None."""
    annual = _latest(listed, ANNUAL)
    late = _latest(listed, LATE_ANNUAL)
    if late and (annual is None or late.filed > annual.filed):
        return "nt_10k"
    if annual:
        year_end = annual.report_date or annual.filed
        if _add_months(year_end, DELINQUENT_MONTHS) < as_of:
            return "late_10k"
    return None


def plan(listed: list[Filing]) -> tuple[list[Filing], bool]:
    """The filings to read, in reading order, and whether the name is a recent IPO."""
    annual = _latest(listed, ANNUAL)
    quarterly = _latest(listed, QUARTERLY)
    chosen: list[Filing] = []
    ipo = False
    if annual:
        chosen.append(annual)
    else:
        prospectus = next((f for form in IPO for f in listed if f.form == form), None)
        if prospectus:
            chosen.append(prospectus)
            ipo = True
    if quarterly:
        chosen.append(quarterly)
    periodic = [f.filed for f in chosen]
    since = max(periodic) if periodic else None
    chosen += [f for f in reversed(listed) if f.form in CURRENT and since and f.filed > since]
    return chosen, ipo


def retrieve(edgar: Edgar, ticker: str, as_of: date) -> Retrieval:
    """Everything the panel reads for `ticker` as of `as_of`, or the reason it could not."""
    result = Retrieval(ticker=ticker, outcome="retrieved")
    try:
        result.cik = edgar.cik(ticker)
        if result.cik is None:
            raise EdgarError(f"{ticker} is not in EDGAR's ticker list")
        submissions = edgar.submissions(result.cik)
        result.name = str(submissions.get("name") or "")
        result.aliases = [result.name] + [
            str(former.get("name", "")) for former in submissions.get("formerNames") or []
        ]
        result.aliases = [alias for alias in result.aliases if alias]
        result.tickers = sorted({ticker, *(str(t) for t in submissions.get("tickers") or [])})
        listed = filings(submissions, as_of)
        result.delinquent = delinquency(listed, as_of)
        chosen, result.ipo = plan(listed)
        for filing in chosen:
            raw = edgar.document(result.cik, filing.accession, filing.document)
            result.sections += sections(filing, raw)
    except EdgarError as exc:
        return Retrieval(
            ticker=ticker,
            outcome="edgar_failure",
            cik=result.cik,
            name=result.name,
            aliases=result.aliases,
            tickers=result.tickers,
            detail=str(exc),
        )
    if not result.sections:
        result.outcome = "no_filings"
        result.detail = "no 10-K, 10-Q or registration statement on file"
    return result


def sections(filing: Filing, raw: bytes) -> list[Section]:
    """The parts of one filing the Analyst reads."""
    text = html_to_text(raw)
    if not text.strip():
        raise EdgarError(f"{filing.form} {filing.accession}: empty document")
    if filing.form in ANNUAL:
        return [
            Section(filing, "Item 1A", _cut(filing, text, *ITEM_1A), raw),
            Section(filing, "Item 7", _cut(filing, text, *ITEM_7), raw),
        ]
    if filing.form in IPO:
        return [Section(filing, "Risk Factors", _cut(filing, text, *RISK_FACTORS), raw)]
    return [Section(filing, "Full text", text, raw)]


# Each section is (name, start heading, end heading). The table of contents repeats every
# heading, so the section is the longest span from a start heading to the next end heading.
ITEM_1A = ("Item 1A", r"item\s*1a\.?\s*[-–—:.]?\s*risk\s+factors", r"item\s*(?:1b|1c|2)\b")
ITEM_7 = (
    "Item 7",
    r"item\s*7\.?\s*[-–—:.]?\s*management[’'`]?s\s+discussion",
    r"item\s*(?:7a|8)\b",
)
RISK_FACTORS = (
    "Risk Factors",
    r"risk\s+factors",
    r"(?:special|cautionary)\s+note\s+regarding\s+forward|use\s+of\s+proceeds",
)
_HEADING = r"(?:^|\n)[ \t]*(?:{})"


def _cut(filing: Filing, text: str, name: str, start: str, end: str) -> str:
    starts = [m.start() for m in re.finditer(_HEADING.format(start), text, re.IGNORECASE)]
    ends = [m.start() for m in re.finditer(_HEADING.format(end), text, re.IGNORECASE)]
    best = ""
    for begin in starts:
        stop = next((e for e in ends if e > begin), len(text))
        if stop - begin > len(best):
            best = text[begin:stop]
    if not best.strip():
        raise EdgarError(f"{filing.form} {filing.accession}: {name} not found")
    return best.strip()


_BLOCK = {
    "p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6", "table", "section",
    "article", "ul", "ol", "dl", "dt", "dd", "hr", "title", "center", "blockquote", "pre",
}  # fmt: skip
_SKIP = {"script", "style", "head", "ix:header"}


class _Text(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skipping = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _SKIP:
            self._skipping += 1
        elif tag in _BLOCK:
            self.parts.append("\n")
        elif tag in ("td", "th"):
            self.parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in _SKIP:
            self._skipping = max(0, self._skipping - 1)
        elif tag in _BLOCK:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skipping:
            self.parts.append(data)


def html_to_text(raw: bytes) -> str:
    """A filing's readable text: one line per block, runs of spaces collapsed."""
    try:
        document = raw.decode("utf-8")
    except UnicodeDecodeError:
        document = raw.decode("latin-1")
    if "<" not in document:
        body = document
    else:
        parser = _Text()
        try:
            parser.feed(document)
            parser.close()
        except Exception as exc:  # html.parser is lenient; anything it raises is malformed input
            raise EdgarError(f"malformed document: {exc}") from None
        body = "".join(parser.parts)
    lines = (re.sub(r"[ \t  ​]+", " ", line).strip() for line in body.split("\n"))
    return "\n".join(line for line in lines if line)
