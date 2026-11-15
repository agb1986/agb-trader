"""A stand-in for the vendor's table API. It serves CSV the way the real API does, with its
column filters, sort order and paging, and the full export as a zip holding one CSV. Its
headers are the real ones, cut down to a few metrics for SF1. Each page names the next in a
`cursor_id` header, empty on the last, as the vendor's table API does; a vendor made with
`cursors=False` sends none, and pages only by `skip`."""

import csv
import http.client
import io
import operator
import zipfile
from collections.abc import Callable
from urllib.parse import parse_qsl, urlsplit
from urllib.request import HTTPError

HEADERS = {
    "stocks": [
        "ticker", "date", "open", "high", "low", "close", "volume", "closeadj", "closeunadj",
        "lastupdated",
    ],
    "fundamentals": [
        "ticker", "dimension", "calendardate", "date", "reportperiod", "fiscalperiod",
        "lastupdated", "revenue", "netinc", "eps",
    ],
    "tickers": [
        "table", "permaticker", "ticker", "name", "exchange", "isdelisted", "category", "cusips",
        "siccode", "sicsector", "sicindustry", "figi", "famaindustry", "sector", "industry",
        "scalemarketcap", "scalerevenue", "relatedtickers", "currency", "location", "lastupdated",
        "firstadded", "firstpricedate", "lastpricedate", "firstquarter", "lastquarter",
        "secfilings", "companysite",
    ],
}  # fmt: skip
PAGING = {"api_key", "sort", "limit", "skip", "qopts.cursor_id"}
OPS = {"": operator.eq, "gte": operator.ge, "lte": operator.le}


def sep_row(ticker, day, close, lastupdated=None):
    return {
        "ticker": ticker, "date": day, "open": close, "high": close, "low": close,
        "close": close, "volume": "1250000", "closeadj": close, "closeunadj": close,
        "lastupdated": lastupdated or day,
    }  # fmt: skip


def sf1_row(ticker, dimension, datekey, lastupdated, revenue, eps=""):
    return {
        "ticker": ticker, "dimension": dimension, "calendardate": datekey, "date": datekey,
        "reportperiod": datekey, "fiscalperiod": "2026-Q3", "lastupdated": lastupdated,
        "revenue": revenue, "netinc": "", "eps": eps,
    }  # fmt: skip


def ticker_row(table, permaticker, ticker, name, *, delisted=False, lastupdated="2026-09-14"):
    row = dict.fromkeys(HEADERS["tickers"], "")
    row.update(
        table=table,
        permaticker=str(permaticker),
        ticker=ticker,
        name=name,
        isdelisted="Y" if delisted else "N",
        location="New York; U.S.A",
        lastupdated=lastupdated,
        firstpricedate="1994-05-02",
        secfilings=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={permaticker}",
    )
    return row


def status(code, retry_after=None) -> HTTPError:
    """The error urllib raises for an answer with status `code`."""
    headers = http.client.HTTPMessage()
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    return HTTPError("https://data.example/tables", code, f"status {code}", headers, None)


class Answer(io.BytesIO):
    """A response body with headers, as urllib's response has."""

    def __init__(self, body: bytes, cursor: str | None) -> None:
        super().__init__(body)
        self.headers = http.client.HTTPMessage()
        if cursor is not None:
            self.headers["cursor_id"] = cursor


def to_csv(header, rows) -> bytes:
    out = io.StringIO()
    writer = csv.DictWriter(out, header, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode()


class FakeVendor:
    """The opener a Sharadar client is given in tests. `urls` lists what was asked for, and
    `fail`, when set, makes each request raise what it returns for the URL. `errors` holds
    errors the next requests raise, one each, before the vendor answers again."""

    def __init__(self, stocks=(), fundamentals=(), tickers=(), *, cursors=True):
        self.rows = {
            "stocks": list(stocks),
            "fundamentals": list(fundamentals),
            "tickers": list(tickers),
        }
        self.urls: list[str] = []
        self.fail: Callable[[str], Exception] | None = None
        self.errors: list[Exception] = []
        self.cursors = cursors

    def __call__(self, url):
        self.urls.append(url)
        if self.fail is not None:
            raise self.fail(url)
        if self.errors:
            raise self.errors.pop(0)
        parts = urlsplit(url)
        table = parts.path.rsplit("/", 1)[-1]
        params = dict(parse_qsl(parts.query, keep_blank_values=True))
        if table not in self.rows:
            raise HTTPError(parts.path, 403, "Forbidden", {}, None)
        header, rows = HEADERS[table], self.rows[table]
        if params.get("years") == "full":
            archive = io.BytesIO()
            with zipfile.ZipFile(archive, "w") as z:
                z.writestr(f"SHARADAR_{table}.csv", to_csv(header, rows))
            archive.seek(0)
            return archive
        for name, value in params.items():
            if name not in PAGING:
                column, _, op = name.partition(".")
                rows = [row for row in rows if OPS[op](row[column], value)]
        for field in reversed(params.get("sort", "").split(",")):
            if field:
                column, _, direction = field.partition(".")
                rows = sorted(rows, key=operator.itemgetter(column), reverse=direction == "desc")
        limit = int(params.get("limit", 10_000))
        cursor = params.get("qopts.cursor_id")
        skip = int(cursor.removeprefix("c")) if cursor else int(params.get("skip", 0))
        body = to_csv(header, rows[skip : skip + limit])
        if not self.cursors:
            return io.BytesIO(body)
        return Answer(body, f"c{skip + limit}" if skip + limit < len(rows) else "")


def sample() -> FakeVendor:
    """A small market as of the close on 2026-09-14. AAPL and MSFT trade; LEHMQ was delisted in
    2008. Some rows fall before 1998, for the backfill to leave out, and one SF1 row was last
    changed outside the week a pull re-lands."""
    return FakeVendor(
        stocks=[
            sep_row("AAPL", "1997-12-31", "0.95"),
            sep_row("LEHMQ", "1997-12-31", "40.5"),
            sep_row("AAPL", "1998-01-02", "1.01"),
            sep_row("LEHMQ", "1998-01-02", "41.25"),
            sep_row("LEHMQ", "2008-09-12", "3.65"),
            sep_row("AAPL", "2026-09-11", "327.5"),
            sep_row("MSFT", "2026-09-11", "509.9"),
            sep_row("AAPL", "2026-09-14", "328.21"),
            sep_row("MSFT", "2026-09-14", "512.3", lastupdated="2026-09-15"),
        ],
        fundamentals=[
            sf1_row("AAPL", "ARQ", "1997-11-14", "2019-03-01", "1575000000"),
            sf1_row("LEHMQ", "ARQ", "2008-07-10", "2019-03-01", "5400000000", "-4.29"),
            sf1_row("AAPL", "ARY", "2025-10-31", "2025-11-03", "416161000000", "7.46"),
            sf1_row("MSFT", "ARQ", "2026-07-30", "2026-09-08", "76441000000", "3.65"),
            sf1_row("AAPL", "ARQ", "2026-07-31", "2026-09-14", "94036000000", "1.57"),
        ],
        tickers=[
            *(
                row
                for table in ("stocks", "fundamentals")
                for row in (
                    ticker_row(table, 199059, "AAPL", "APPLE INC"),
                    ticker_row(table, 194755, "LEHMQ", "LEHMAN BROTHERS HOLDINGS", delisted=True),
                    ticker_row(table, 198508, "MSFT", "MICROSOFT CORP"),
                )
            ),
            ticker_row("insiders", 199059, "AAPL", "APPLE INC"),
        ],
    )  # fmt: skip
