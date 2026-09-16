"""The Parquet lake: the vendor's CSV in the lake's types, partitions whose bytes and hash are
the same each time they land, the bulk backfill from 1998, and DuckDB over the result."""

import io
import urllib.error
import zipfile
from datetime import date

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sharadar_fake import HEADERS, FakeVendor, sample, sep_row, sf1_row, ticker_row, to_csv

from agb import lake
from agb.lake import SEP, SF1, TICKERS, LakeError, NotYet, Sharadar

SESSION = date(2026, 9, 14)
KEY = "k3y-not-a-real-one"


def client(vendor, page=lake.PAGE):
    return Sharadar(KEY, vendor, page=page)


def read(root, path):
    return pq.read_table(root / path / lake.FILE)


def mtimes(root):
    return {p: p.stat().st_mtime_ns for p in root.rglob("*") if p.is_file()}


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


def test_no_error_shows_the_key():
    vendor = sample()
    vendor.fail = lambda url: urllib.error.URLError(f"timed out fetching {url}")
    with pytest.raises(LakeError) as caught:
        client(vendor).query(SEP, {"date": SESSION})
    assert "timed out" in str(caught.value)
    assert KEY not in str(caught.value)
    assert KEY in vendor.urls[0]  # the check is not vacuous


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

    vendor.rows["stocks"].append(sep_row("AAPL", "2026-09-15", "330.1"))
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


def test_an_export_must_hold_one_csv(tmp_path):
    archive = tmp_path / "sep.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("a.csv", to_csv(HEADERS["stocks"], []))
        z.writestr("b.csv", to_csv(HEADERS["stocks"], []))
    with pytest.raises(LakeError, match="one CSV"):
        lake.land_bulk(tmp_path / "lake", SEP, archive, lake.SINCE, tmp_path / "work")


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
