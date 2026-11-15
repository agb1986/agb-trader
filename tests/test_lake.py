"""The Parquet lake: the vendor's CSV in the lake's types, partitions whose bytes and hash are
the same each time they land, the bulk backfill from 1998, and DuckDB over the result."""

import http.client
import io
import urllib.error
import zipfile
from datetime import date

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sharadar_fake import (
    HEADERS,
    FakeVendor,
    sample,
    sep_row,
    sf1_row,
    status,
    ticker_row,
    to_csv,
)

from agb import lake
from agb.lake import SEP, SF1, TICKERS, ZIP_MAGIC, LakeError, NotYet, Sharadar

SESSION = date(2026, 9, 14)
KEY = "k3y-not-a-real-one"


def client(vendor, page=lake.PAGE, waits=None):
    """A client of `vendor` that waits for nothing between attempts, noting each wait it was
    asked for in `waits`."""
    waits = [] if waits is None else waits
    return Sharadar(KEY, vendor, page=page, sleep=waits.append)


def trading(vendor, *days, tickers=("AAPL", "MSFT")):
    """Give `vendor` a price for each of `tickers` on each of `days`."""
    vendor.rows["stocks"] += [sep_row(t, day, "300") for day in days for t in tickers]
    return vendor


def read(root, path):
    return pq.read_table(root / path / lake.FILE)


def mtimes(root):
    return {p: p.stat().st_mtime_ns for p in root.rglob("*") if p.is_file()}


def landed(root):
    return sorted(str(p.parent.relative_to(root)) for p in root.rglob(lake.FILE))


def bulk(tmp_path, vendor, dataset, root):
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    archive = client(vendor).bulk(dataset, work / f"{dataset.name}.zip")
    return lake.land_bulk(root, dataset, archive, lake.SINCE, work)


def test_parse_gives_the_lake_types_and_keeps_the_filing_date_as_datekey():
    rows = [sf1_row("AAPL", "ARQ", "2026-07-31", "2026-09-14", "94036000000")]
    table = lake.parse(SF1, to_csv(HEADERS["fundamentals"], rows))
    assert "date" not in table.column_names
    assert table.schema.field("datekey").type == pa.date32()
    assert table.schema.field("revenue").type == pa.float64()
    assert table.schema.field("fiscalperiod").type == pa.string()
    assert table.to_pylist()[0] == {
        "ticker": "AAPL",
        "dimension": "ARQ",
        "calendardate": date(2026, 7, 31),
        "datekey": date(2026, 7, 31),
        "reportperiod": date(2026, 7, 31),
        "fiscalperiod": "2026-Q3",
        "lastupdated": date(2026, 9, 14),
        "revenue": 94036000000.0,
        "netinc": None,
        "eps": None,
    }


def test_parse_keeps_quoted_values_and_types_tickers():
    rows = [ticker_row("stocks", 194755, "LEHMQ", "LEHMAN BROTHERS HOLDINGS, INC", delisted=True)]
    [row] = lake.parse(TICKERS, to_csv(HEADERS["tickers"], rows)).to_pylist()
    assert row["name"] == "LEHMAN BROTHERS HOLDINGS, INC"
    assert row["permaticker"] == 194755
    assert row["firstpricedate"] == date(1994, 5, 2)
    assert row["secfilings"].endswith("action=getcompany&CIK=194755")
    assert row["cusips"] is None


def test_a_header_with_no_rows_is_an_empty_table():
    table = lake.parse(SEP, to_csv(HEADERS["stocks"], []))
    assert table.num_rows == 0
    assert table.schema.field("date").type == pa.date32()


@pytest.mark.parametrize(
    ("header", "message"),
    [("ticker,open,close\n", "lack \\['date'\\]"), ("ticker,date,Close Price\n", "unexpected")],
)
def test_parse_refuses_an_unexpected_header(header, message):
    with pytest.raises(LakeError, match=message):
        lake.parse(SEP, header.encode())


def test_a_query_pages_through_every_row():
    vendor = FakeVendor(stocks=[sep_row("AAPL", f"2026-09-0{d}", "300") for d in range(1, 6)])
    table = client(vendor, page=2).query(SEP, {"ticker": "AAPL"})
    assert table.column("date").to_pylist() == [date(2026, 9, d) for d in range(1, 6)]
    assert ["skip=0" in u for u in vendor.urls] == [True, False, False]
    assert len(vendor.urls) == 3


def test_a_missing_key_is_refused():
    with pytest.raises(LakeError, match="SHARADAR_API_KEY is not set"):
        Sharadar("", sample())


def test_a_query_ends_on_the_vendors_cursor():
    # Four rows in pages of two: the second page is full, and only its empty cursor says it is
    # the last, so no third request is made.
    vendor = FakeVendor(stocks=[sep_row("AAPL", f"2026-09-0{d}", "300") for d in range(1, 5)])
    table = client(vendor, page=2).query(SEP, {"ticker": "AAPL"})
    assert table.column("date").to_pylist() == [date(2026, 9, d) for d in range(1, 5)]
    assert len(vendor.urls) == 2
    assert "qopts.cursor_id=c2" in vendor.urls[1]
    assert "skip" not in vendor.urls[1]


def test_a_query_of_a_vendor_without_cursors_pages_by_skip():
    vendor = FakeVendor(
        stocks=[sep_row("AAPL", f"2026-09-0{d}", "300") for d in range(1, 5)], cursors=False
    )
    table = client(vendor, page=2).query(SEP, {"ticker": "AAPL"})
    assert table.num_rows == 4
    assert [u.split("skip=")[1].split("&")[0] for u in vendor.urls] == ["0", "2", "4"]


def test_a_cursor_that_does_not_move_is_refused():
    def opener(url):
        answer = io.BytesIO(to_csv(HEADERS["stocks"], [sep_row("AAPL", "2026-09-01", "300")]))
        answer.headers = {"cursor_id": "stuck"}
        return answer

    with pytest.raises(LakeError, match="cursor did not move"):
        client(opener, page=1).query(SEP, {"ticker": "AAPL"})


def test_no_error_shows_the_key():
    vendor = sample()
    vendor.fail = lambda url: urllib.error.URLError(f"timed out fetching {url}")
    waits = []
    with pytest.raises(LakeError) as caught:
        client(vendor, waits=waits).query(SEP, {"date": SESSION})
    assert "timed out" in str(caught.value)
    assert f"after {lake.ATTEMPTS} attempts" in str(caught.value)
    assert KEY not in str(caught.value)
    assert KEY in vendor.urls[0]  # the check is not vacuous
    assert len(vendor.urls) == lake.ATTEMPTS
    assert waits == [lake.BACKOFF, 2 * lake.BACKOFF]


@pytest.mark.parametrize(
    "error",
    [
        status(503),
        status(429),
        urllib.error.URLError("timed out"),
        http.client.RemoteDisconnected("closed"),
        http.client.IncompleteRead(b"ticker,da"),
    ],
    ids=["503", "429", "timeout", "dropped", "cut-short"],
)
def test_a_vendor_error_then_an_answer_lands_the_session(tmp_path, error):
    vendor = sample()
    vendor.errors = [error]
    waits = []

    partitions = lake.pull(tmp_path, client(vendor, waits=waits), SESSION)

    assert ("sep/date=2026-09-14", 2) in [(p.path, p.rows) for p in partitions]
    assert waits == [lake.BACKOFF]
    assert vendor.urls[0] == vendor.urls[1]


def test_the_vendors_retry_after_is_waited_out():
    vendor = sample()
    vendor.errors = [status(429, retry_after=30), status(503, retry_after="soon")]
    waits = []
    assert client(vendor, waits=waits).query(SEP, {"date": SESSION}).num_rows == 2
    # The HTTP-date form, or anything else that is not a number, leaves the backoff in charge.
    assert waits == [30.0, 2 * lake.BACKOFF]


def test_a_retry_after_past_the_cap_is_not_waited_out():
    vendor = sample()
    vendor.errors = [status(503, retry_after=lake.RETRY_AFTER_CAP + 1)]
    waits = []
    with pytest.raises(LakeError, match="status 503"):
        client(vendor, waits=waits).query(SEP, {"date": SESSION})
    assert waits == []
    assert len(vendor.urls) == 1


def test_a_refusal_is_not_tried_again():
    vendor = sample()
    vendor.errors = [status(403)]
    waits = []
    with pytest.raises(LakeError, match="status 403") as caught:
        client(vendor, waits=waits).query(SEP, {"date": SESSION})
    assert "attempts" not in str(caught.value)
    assert waits == []
    assert len(vendor.urls) == 1


def test_a_bulk_download_is_tried_again_from_the_start(tmp_path):
    vendor = sample()
    vendor.errors = [http.client.IncompleteRead(b"PK")]
    archive = client(vendor).bulk(SEP, tmp_path / "sep.zip")
    assert zipfile.ZipFile(archive).namelist() == ["SHARADAR_stocks.csv"]


def test_a_response_that_is_not_csv_is_refused():
    def opener(url):
        return io.BytesIO(f'{{"error": "bad key {KEY}"}}'.encode())

    with pytest.raises(LakeError, match="did not return CSV") as caught:
        client(opener).query(SEP, {"date": SESSION})
    assert KEY not in str(caught.value)


def test_a_pull_lands_each_dataset(tmp_path):
    partitions = lake.pull(tmp_path, client(sample()), SESSION)

    assert [(p.path, p.rows) for p in partitions] == [
        ("sep/date=2026-09-14", 2),
        ("sf1/lastupdated=2026-09-08", 1),
        ("sf1/lastupdated=2026-09-14", 1),
        ("tickers/snapshot=2026-09-14", 6),
    ]
    for p in partitions:
        assert p.sha256 == lake.file_hash(tmp_path / p.path / lake.FILE)
    assert read(tmp_path, "sep/date=2026-09-14").column("ticker").to_pylist() == ["AAPL", "MSFT"]
    assert read(tmp_path, "sf1/lastupdated=2026-09-14").column("datekey").to_pylist() == [
        date(2026, 7, 31)
    ]
    tickers = read(tmp_path, "tickers/snapshot=2026-09-14").to_pylist()
    assert {(t["table"], t["ticker"]) for t in tickers if t["isdelisted"] == "Y"} == {
        ("fundamentals", "LEHMQ"),
        ("stocks", "LEHMQ"),
    }
    assert {t["table"] for t in tickers} == {"stocks", "fundamentals"}
    assert not list(tmp_path.rglob("*.partial"))


def test_pulling_a_landed_date_again_changes_nothing(tmp_path):
    vendor = sample()
    first = lake.pull(tmp_path, client(vendor), SESSION)
    before = mtimes(tmp_path)

    assert lake.pull(tmp_path, client(vendor), SESSION) == first
    assert mtimes(tmp_path) == before


def test_a_revised_partition_lands_with_a_new_hash(tmp_path):
    vendor = sample()
    first = {p.path: p.sha256 for p in lake.pull(tmp_path, client(vendor), SESSION)}
    vendor.rows["stocks"][-1] = sep_row("MSFT", "2026-09-14", "512.4", lastupdated="2026-09-15")

    second = {p.path: p.sha256 for p in lake.pull(tmp_path, client(vendor), SESSION)}

    changed = {path for path in first if first[path] != second[path]}
    assert changed == {"sep/date=2026-09-14"}
    assert second["sep/date=2026-09-14"] == lake.file_hash(
        tmp_path / "sep/date=2026-09-14" / lake.FILE
    )


def test_sf1_keeps_each_version_and_the_view_shows_the_latest(tmp_path):
    vendor = sample()
    vendor.rows["fundamentals"].append(
        sf1_row("MSFT", "ARY", "2026-07-30", "2026-09-14", "305000000000", "13.1")
    )
    lake.pull(tmp_path, client(vendor), SESSION)
    partition = tmp_path / "sf1/lastupdated=2026-09-14" / lake.FILE
    landed = lake.file_hash(partition)
    # The vendor restates AAPL's quarter the next day. Its API now has only the new version,
    # and only MSFT's row still says it was last changed on the 14th.
    vendor.rows["fundamentals"][4] = sf1_row(
        "AAPL", "ARQ", "2026-07-31", "2026-09-15", "94100000000", "1.58"
    )

    lake.pull(tmp_path, client(vendor), SESSION)  # a re-run of the landed date
    assert lake.file_hash(partition) == landed
    assert pq.read_table(partition).column("ticker").to_pylist() == ["AAPL", "MSFT"]

    vendor.rows["stocks"] += [
        sep_row("AAPL", "2026-09-15", "330.1"),
        sep_row("MSFT", "2026-09-15", "513"),
    ]
    lake.pull(tmp_path, client(vendor), date(2026, 9, 15))
    versions = lake.connect(tmp_path).sql(
        "SELECT revenue FROM read_parquet(?) WHERE ticker = 'AAPL' AND dimension = 'ARQ' "
        "ORDER BY lastupdated",
        params=[str(tmp_path / "sf1/*" / lake.FILE)],
    )
    assert versions.fetchall() == [(94036000000.0,), (94100000000.0,)]
    latest = lake.connect(tmp_path).sql(
        "SELECT ticker, dimension, datekey, eps FROM sf1 ORDER BY ticker, dimension"
    )
    assert latest.fetchall() == [
        ("AAPL", "ARQ", date(2026, 7, 31), 1.58),
        ("MSFT", "ARQ", date(2026, 7, 30), 3.65),
        ("MSFT", "ARY", date(2026, 7, 30), 13.1),
    ]


def test_sf1_as_of_a_day_serves_each_row_as_known_that_day(tmp_path):
    vendor = sample()
    lake.pull(tmp_path, client(vendor), SESSION)
    # AAPL's quarter is restated the next day.
    vendor.rows["fundamentals"][4] = sf1_row(
        "AAPL", "ARQ", "2026-07-31", "2026-09-15", "94100000000", "1.58"
    )
    vendor.rows["stocks"] += [
        sep_row("AAPL", "2026-09-15", "330.1"),
        sep_row("MSFT", "2026-09-15", "513"),
    ]
    lake.pull(tmp_path, client(vendor), date(2026, 9, 15))
    conn = lake.connect(tmp_path)

    def eps(as_of):
        query = "SELECT eps FROM sf1_as_of(?) WHERE ticker = 'AAPL' AND dimension = 'ARQ'"
        return conn.sql(query, params=[as_of]).fetchall()

    assert eps(SESSION) == [(1.57,)]
    assert eps(date(2026, 9, 15)) == [(1.58,)]
    # A row the lake first saw after the day, as every row of the backfill is, is served as
    # first seen, while one filed after the day is not served at all.
    assert eps(date(2026, 9, 1)) == [(1.57,)]
    assert eps(date(2026, 7, 30)) == []


def test_sf1_as_of_today_is_the_view(tmp_path):
    vendor = sample()
    lake.pull(tmp_path, client(vendor), SESSION)
    vendor.rows["fundamentals"][4] = sf1_row(
        "AAPL", "ARQ", "2026-07-31", "2026-09-15", "94100000000", "1.58"
    )
    vendor.rows["stocks"] += [
        sep_row("AAPL", "2026-09-15", "330.1"),
        sep_row("MSFT", "2026-09-15", "513"),
    ]
    lake.pull(tmp_path, client(vendor), date(2026, 9, 15))
    conn = lake.connect(tmp_path)

    view = conn.sql("SELECT * FROM sf1 ORDER BY ALL").fetchall()
    assert conn.sql("SELECT * FROM sf1_as_of(current_date) ORDER BY ALL").fetchall() == view
    assert [(row[0], row[-1]) for row in view] == [("AAPL", 1.58), ("MSFT", 3.65)]


def test_two_periods_filed_on_one_day_both_survive(tmp_path):
    vendor = sample()
    # A late filer catches up on two quarters in one filing.
    earlier = sf1_row("AAPL", "ARQ", "2026-07-31", "2026-09-14", "90000000000", "1.40")
    earlier["reportperiod"] = "2026-03-31"
    vendor.rows["fundamentals"].append(earlier)
    lake.pull(tmp_path, client(vendor), SESSION)
    conn = lake.connect(tmp_path)

    expected = [(date(2026, 3, 31), 1.40), (date(2026, 7, 31), 1.57)]
    partition = pq.read_table(tmp_path / "sf1/lastupdated=2026-09-14" / lake.FILE)
    assert (
        sorted(
            (row["reportperiod"], row["eps"])
            for row in partition.to_pylist()
            if row["ticker"] == "AAPL"
        )
        == expected
    )
    for source in ("sf1", "sf1_as_of(DATE '2026-09-14')"):
        rows = conn.sql(
            f"SELECT reportperiod, eps FROM {source} "
            "WHERE ticker = 'AAPL' AND dimension = 'ARQ' AND datekey = DATE '2026-07-31' "
            "ORDER BY reportperiod"
        )
        assert rows.fetchall() == expected


def test_a_landed_tickers_snapshot_is_kept(tmp_path):
    vendor = sample()
    lake.pull(tmp_path, client(vendor), SESSION)
    snapshot = tmp_path / "tickers/snapshot=2026-09-14" / lake.FILE
    before = lake.file_hash(snapshot)
    vendor.rows["tickers"].append(ticker_row("stocks", 637001, "NEWCO", "NEWCO INC"))

    [kept] = [p for p in lake.pull(tmp_path, client(vendor), SESSION) if p.dataset == "tickers"]

    assert (kept.sha256, kept.rows) == (before, 6)
    assert lake.file_hash(snapshot) == before


def test_a_session_without_prices_yet_lands_nothing(tmp_path):
    with pytest.raises(NotYet, match="no SEP prices for 2026-09-15 yet"):
        lake.pull(tmp_path, client(sample()), date(2026, 9, 15))
    assert list(tmp_path.iterdir()) == []


def test_a_crashed_write_leaves_no_partial_behind(tmp_path):
    directory = tmp_path / "sep/date=2026-09-14"
    directory.mkdir(parents=True)
    (directory / ".tmpab12.partial").write_bytes(b"half a file")
    lake.pull(tmp_path, client(sample()), SESSION)
    assert sorted(p.name for p in directory.iterdir()) == [lake.FILE]


def test_the_backfill_lands_history_from_1998_with_delisted_tickers(tmp_path):
    vendor = sample()
    root = tmp_path / "lake"

    sep = bulk(tmp_path, vendor, SEP, root)
    sf1 = bulk(tmp_path, vendor, SF1, root)

    assert [p.path for p in sep] == [
        "sep/date=1998-01-02",
        "sep/date=2008-09-12",
        "sep/date=2026-09-11",
        "sep/date=2026-09-14",
    ]
    assert read(root, "sep/date=2008-09-12").column("ticker").to_pylist() == ["LEHMQ"]
    assert [p.path for p in sf1] == [
        "sf1/lastupdated=2019-03-01",
        "sf1/lastupdated=2025-11-03",
        "sf1/lastupdated=2026-09-08",
        "sf1/lastupdated=2026-09-14",
    ]
    assert read(root, "sf1/lastupdated=2019-03-01").column("ticker").to_pylist() == ["LEHMQ"]
    db = lake.connect(root)
    assert db.sql("SELECT min(date) FROM sep").fetchone() == (date(1998, 1, 2),)
    assert db.sql("SELECT min(datekey) FROM sf1").fetchone() == (date(2008, 7, 10),)


def test_the_backfill_and_a_daily_pull_land_the_same_bytes(tmp_path):
    vendor = sample()
    daily = {p.path: p.sha256 for p in lake.pull(tmp_path / "daily", client(vendor), SESSION)}
    backfill = {p.path: p.sha256 for p in bulk(tmp_path, vendor, SEP, tmp_path / "bulk")}
    backfill |= {p.path: p.sha256 for p in bulk(tmp_path, vendor, SF1, tmp_path / "bulk")}

    for path in ("sep/date=2026-09-14", "sf1/lastupdated=2026-09-08", "sf1/lastupdated=2026-09-14"):
        assert backfill[path] == daily[path], path


def test_the_backfill_again_changes_nothing(tmp_path):
    vendor = sample()
    root = tmp_path / "lake"
    first = bulk(tmp_path, vendor, SEP, root)
    before = mtimes(root)
    assert bulk(tmp_path, vendor, SEP, root) == first
    assert mtimes(root) == before


def test_the_backfill_copies_a_year_at_a_time_over_a_gap(tmp_path):
    # The export is split into one copy per year of the partition key, so rows have to survive
    # the years with no rows at all, and no row may land twice where two copies share a lake.
    days = ["1998-01-02", "1998-06-15", "1999-03-04", "2002-11-27"]
    vendor = FakeVendor(stocks=[sep_row(t, day, "10.5") for day in days for t in ("AAPL", "MSFT")])
    root = tmp_path / "lake"

    sep = bulk(tmp_path, vendor, SEP, root)

    assert [p.path for p in sep] == [f"sep/date={day}" for day in days]
    for day in days:
        assert read(root, f"sep/date={day}").column("ticker").to_pylist() == ["AAPL", "MSFT"]


def test_an_export_must_hold_one_csv(tmp_path):
    archive = tmp_path / "sep.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("a.csv", to_csv(HEADERS["stocks"], []))
        z.writestr("b.csv", to_csv(HEADERS["stocks"], []))
    with pytest.raises(LakeError, match="one CSV"):
        lake.land_bulk(tmp_path / "lake", SEP, archive, lake.SINCE, tmp_path / "work")


def test_a_bulk_download_that_is_not_a_zip_says_what_came_back(tmp_path):
    body = f'{{"error": "too many requests for key {KEY}"}}'.encode()
    client = Sharadar(KEY, lambda url: io.BytesIO(body))

    with pytest.raises(LakeError) as exc:
        client.bulk(SEP, tmp_path / "sep.zip")

    assert "did not return a zip" in str(exc.value)
    assert "too many requests" in str(exc.value)
    assert KEY not in str(exc.value)


def test_a_corrupt_export_fails_as_a_lake_error(tmp_path):
    archive = tmp_path / "sep.zip"
    archive.write_bytes(ZIP_MAGIC + b" but truncated here")
    with pytest.raises(LakeError, match="not a readable zip"):
        lake.land_bulk(tmp_path / "lake", SEP, archive, lake.SINCE, tmp_path / "work")


def test_a_query_too_big_for_memory_spills_beside_the_lake(tmp_path, monkeypatch):
    # The service runs from a working directory it cannot write, so DuckDB's default spill
    # directory is unusable and a query that outgrows memory has nowhere to go.
    read_only = tmp_path / "cwd"
    read_only.mkdir()
    read_only.chmod(0o555)
    monkeypatch.chdir(read_only)
    root = tmp_path / "lake"
    root.mkdir()

    db = lake.connect(root)
    db.execute("SET memory_limit = '100MB'")
    db.execute("SET threads = 1")
    big = "SELECT count(*) FROM (SELECT i FROM generate_series(1, 10000000) t(i) ORDER BY i)"
    rows = db.sql(big)

    assert rows.fetchone() == (10_000_000,)
    assert (root / lake.SPILL).exists()


def test_a_lake_it_cannot_write_leaves_the_spill_directory_alone(tmp_path):
    # The query path is read-only work, so a reader with no write access still gets its views.
    root = tmp_path / "lake"
    root.mkdir()
    lake.pull(root, client(sample()), SESSION)
    root.chmod(0o555)
    try:
        db = lake.connect(root)
        assert db.sql("SELECT count(*) FROM sep").fetchone() == (2,)
        assert not (root / lake.SPILL).exists()
    finally:
        root.chmod(0o755)


def test_duckdb_views_cover_what_has_landed(tmp_path):
    assert lake.connect(tmp_path).sql("SHOW TABLES").fetchall() == []
    with pytest.raises(duckdb.CatalogException):
        lake.connect(tmp_path).sql("SELECT * FROM sep")

    vendor = sample()
    lake.pull(tmp_path, client(vendor), date(2026, 9, 11))
    vendor.rows["tickers"].append(ticker_row("stocks", 637001, "NEWCO", "NEWCO INC"))
    lake.pull(tmp_path, client(vendor), SESSION)

    db = lake.connect(tmp_path)
    assert db.sql("SELECT count(*), max(date) FROM sep").fetchone() == (4, SESSION)
    # The newest snapshot only.
    assert db.sql("SELECT count(*) FROM tickers").fetchone() == (7,)
    assert db.sql("SELECT DISTINCT ticker FROM tickers WHERE isdelisted = 'Y'").fetchall() == [
        ("LEHMQ",)
    ]


def test_the_query_command_prints_rows(tmp_path, capsys):
    lake.pull(tmp_path, client(sample()), SESSION)
    lake.main(["--lake", str(tmp_path), "SELECT ticker, close FROM sep ORDER BY ticker"])
    assert capsys.readouterr().out == "ticker\tclose\nAAPL\t328.21\nMSFT\t512.3\n"


def sep_partitions(partitions):
    return [p.path for p in partitions if p.dataset == "sep"]


def test_a_pull_after_a_gap_lands_each_session_it_missed(tmp_path):
    # The lake has the 9th. The pulls of the 10th and 11th failed; the 14th's lands all three.
    vendor = trading(sample(), "2026-09-09", "2026-09-10")
    lake.pull(tmp_path, client(vendor), date(2026, 9, 9))
    vendor.urls.clear()

    partitions = lake.pull(tmp_path, client(vendor), SESSION)

    assert sep_partitions(partitions) == [
        "sep/date=2026-09-10",
        "sep/date=2026-09-11",
        "sep/date=2026-09-14",
    ]
    for day in ("2026-09-10", "2026-09-11", "2026-09-14"):
        assert read(tmp_path, f"sep/date={day}").column("ticker").to_pylist() == ["AAPL", "MSFT"]
    [sep] = [u for u in vendor.urls if "/stocks?" in u]
    assert "date.gte=2026-09-10" in sep and "date.lte=2026-09-14" in sep


def test_a_pull_again_after_a_gap_changes_nothing(tmp_path):
    vendor = trading(sample(), "2026-09-10")
    lake.pull(tmp_path, client(vendor), date(2026, 9, 10))
    first = lake.pull(tmp_path, client(vendor), SESSION)
    before = mtimes(tmp_path)
    # Re-run, the pull of the 14th looks back to the 11th, the last session before it.
    again = lake.pull(tmp_path, client(vendor), SESSION)
    assert sep_partitions(again) == ["sep/date=2026-09-14"]
    assert {p.path: p.sha256 for p in again} == {
        p.path: p.sha256 for p in first if p.path != "sep/date=2026-09-11"
    }
    assert mtimes(tmp_path) == before


def test_a_session_still_being_published_is_deferred_not_landed(tmp_path):
    # The lake has the 10th. The vendor has all of the 11th, but only AAPL of the 14th so far:
    # the 11th lands, and the 14th waits, with nothing of SF1 or the tickers landed for it.
    vendor = sample()
    vendor.rows["stocks"] = [r for r in vendor.rows["stocks"] if r["date"] != "2026-09-14"]
    trading(vendor, "2026-09-10")
    trading(vendor, "2026-09-14", tickers=("AAPL",))
    lake.pull(tmp_path, client(vendor), date(2026, 9, 10))
    before = landed(tmp_path)

    with pytest.raises(
        NotYet, match="1 SEP prices for 2026-09-14, short of 90% of the 2"
    ) as caught:
        lake.pull(tmp_path, client(vendor), SESSION)

    assert [p.path for p in caught.value.landed] == ["sep/date=2026-09-11"]
    assert landed(tmp_path) == sorted([*before, "sep/date=2026-09-11"])

    trading(vendor, "2026-09-14", tickers=("MSFT",))
    partitions = lake.pull(tmp_path, client(vendor), SESSION)
    assert sep_partitions(partitions) == ["sep/date=2026-09-14"]
    assert read(tmp_path, "sep/date=2026-09-14").num_rows == 2


def test_a_session_missing_from_the_middle_of_a_gap_is_waited_for(tmp_path):
    vendor = trading(sample(), "2026-09-10")
    vendor.rows["stocks"] = [r for r in vendor.rows["stocks"] if r["date"] != "2026-09-11"]
    lake.pull(tmp_path, client(vendor), date(2026, 9, 10))

    with pytest.raises(NotYet, match="no SEP prices for 2026-09-11 yet") as caught:
        lake.pull(tmp_path, client(vendor), SESSION)

    assert caught.value.landed == []
    assert "sep/date=2026-09-14" not in landed(tmp_path)


def test_a_gap_too_long_to_page_is_left_to_the_backfill(tmp_path):
    vendor = trading(sample(), "2026-07-01")
    lake.pull(tmp_path, client(vendor), date(2026, 7, 1))
    vendor.urls.clear()
    with pytest.raises(LakeError, match="land the gap with the backfill"):
        lake.pull(tmp_path, client(vendor), SESSION)
    assert vendor.urls == []


def test_sf1_is_fetched_from_a_week_before_the_last_pull(tmp_path):
    # The lake was last pulled on the 1st. A row first stamped on the 3rd, more than a week
    # before the 14th, lands with the 14th's pull; a row landed on the 1st and fetched again
    # lands once; and the version of a row the vendor has since restated is kept.
    days = ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09"]
    vendor = trading(sample(), *days, "2026-09-10")
    vendor.rows["fundamentals"] += [
        sf1_row("MSFT", "ARY", "2026-06-30", "2026-08-28", "281000000000", "12.1"),
        sf1_row("AAPL", "ARQ", "2026-06-27", "2026-09-01", "94000000000", "1.55"),
    ]
    lake.pull(tmp_path, client(vendor), date(2026, 9, 1))
    vendor.rows["fundamentals"][-1] = sf1_row(
        "AAPL", "ARQ", "2026-06-27", "2026-09-03", "94200000000", "1.56"
    )
    vendor.urls.clear()

    partitions = lake.pull(tmp_path, client(vendor), SESSION)

    [sf1] = [u for u in vendor.urls if "/fundamentals?" in u]
    assert f"lastupdated.gte={date(2026, 9, 1) - lake.SF1_LOOKBACK}" in sf1
    assert "sf1/lastupdated=2026-09-03" in [p.path for p in partitions]
    rows = lake.connect(tmp_path).sql(
        "SELECT ticker, dimension, datekey, lastupdated, eps FROM read_parquet(?)"
        " WHERE datekey < '2026-07-01' AND datekey > '2026-01-01'"
        " ORDER BY ticker, lastupdated",
        params=[str(tmp_path / "sf1/*" / lake.FILE)],
    )
    assert rows.fetchall() == [
        ("AAPL", "ARQ", date(2026, 6, 27), date(2026, 9, 1), 1.55),
        ("AAPL", "ARQ", date(2026, 6, 27), date(2026, 9, 3), 1.56),
        ("MSFT", "ARY", date(2026, 6, 30), date(2026, 8, 28), 12.1),
    ]
