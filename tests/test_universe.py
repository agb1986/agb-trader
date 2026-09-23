"""The point-in-time universe: the four filters, the `datekey` gate that keeps a late filing out
of an earlier universe, delisted names in the universes before they went, and the snapshot date
and partition digest each universe is built from."""

from datetime import date, timedelta

import pyarrow as pa
import pytest

from agb import lake, universe

# A month-end session, and the Sunday whose month it ends.
SESSION = date(2026, 8, 31)
MONTH_END = date(2026, 8, 31)
COMMON = "Domestic Common Stock"

_SEP = pa.schema([
    ("ticker", pa.string()), ("date", pa.date32()), ("close", pa.float64()),
    ("closeunadj", pa.float64()), ("volume", pa.float64()),
])  # fmt: skip
_SF1 = pa.schema([
    ("ticker", pa.string()), ("dimension", pa.string()), ("datekey", pa.date32()),
    ("lastupdated", pa.date32()), ("sharesbas", pa.float64()), ("sharefactor", pa.float64()),
])  # fmt: skip
_TICKERS = pa.schema([
    ("table", pa.string()), ("permaticker", pa.int64()), ("ticker", pa.string()),
    ("category", pa.string()), ("industry", pa.string()), ("siccode", pa.string()),
])  # fmt: skip


def weekdays(end: date, count: int) -> list[date]:
    """`count` weekdays ending at `end`, oldest first. The universe takes its window from the
    sessions the lake holds, so weekdays stand in for sessions without a calendar."""
    days, day = [], end
    while len(days) < count:
        if day.weekday() < 5:
            days.append(day)
        day -= timedelta(days=1)
    return sorted(days)


class Market:
    """A lake built a name at a time. Every knob defaults to a value that clears its filter, so
    a test sets only the one it is about."""

    def __init__(self, root):
        self.root = root
        self.rows = {"sep": [], "sf1": [], "tickers": []}
        self.permaticker = 100_000

    def trades(self, ticker, sessions, *, price=20.0, close=None, volume=1_000_000.0):
        for day in sessions:
            self.rows["sep"].append((ticker, day, price if close is None else close, price, volume))
        return self

    def files(self, ticker, *, datekey, shares=100_000_000.0, dimension="ARQ", sharefactor=1.0):
        self.rows["sf1"].append((ticker, dimension, datekey, SESSION, shares, sharefactor))
        return self

    def listed(
        self, ticker, *, category=COMMON, industry="Software", siccode="7372", table="stocks"
    ):
        self.permaticker += 1
        self.rows["tickers"].append((table, self.permaticker, ticker, category, industry, siccode))
        return self

    def name(self, ticker, sessions=None, *, shares=100_000_000.0, datekey=None, **kwargs):
        """A whole name: prices, one filing, and a row in the tickers table."""
        sessions = weekdays(SESSION, 60) if sessions is None else sessions
        listing_keys = ("category", "industry", "siccode", "table")
        listing = {k: kwargs.pop(k) for k in listing_keys if k in kwargs}
        filing = {k: kwargs.pop(k) for k in ("dimension", "sharefactor") if k in kwargs}
        self.trades(ticker, sessions, **kwargs)
        if shares is not None:
            self.files(ticker, datekey=datekey or sessions[0], shares=shares, **filing)
        self.listed(ticker, **listing)
        return self

    def land_tickers(self, snapshot):
        """The tickers table as its own snapshot, the way each pull lands it."""
        lake.land(self.root, lake.TICKERS, self._table(lake.TICKERS, _TICKERS), snapshot)
        return self.root

    def _table(self, dataset, schema):
        rows = self.rows[dataset.name]
        return pa.Table.from_pylist(
            [dict(zip(schema.names, row, strict=True)) for row in rows], schema=schema
        )

    def land(self):
        for dataset, schema in ((lake.SEP, _SEP), (lake.SF1, _SF1), (lake.TICKERS, _TICKERS)):
            if self.rows[dataset.name]:
                lake.land(self.root, dataset, self._table(dataset, schema), SESSION)
        return self.root


def market(root, **kwargs):
    """A lake holding one name that clears every filter."""
    return Market(root).name("GOOD", **kwargs).land()


def test_a_name_that_clears_every_filter_is_in_the_universe(tmp_path):
    result = universe.snapshot(MONTH_END, market(tmp_path))

    assert result.tickers == ("GOOD",)
    assert result.size == 1
    member = result.members[0]
    assert member.price == pytest.approx(20.0)
    assert member.dollar_volume == pytest.approx(20_000_000.0)
    assert member.market_cap == pytest.approx(2_000_000_000.0)


def test_the_snapshot_falls_back_to_the_last_session_before_a_closed_month_end(tmp_path):
    root = market(tmp_path, sessions=weekdays(SESSION, 90))

    # 2026-05-31 is a Sunday; the last session of that month is the Friday before it.
    result = universe.snapshot(date(2026, 5, 31), root)

    assert result.as_of == date(2026, 5, 31)
    assert result.snapshot_date == date(2026, 5, 29)


def test_a_lake_with_no_session_before_the_date_refuses(tmp_path):
    root = market(tmp_path)

    with pytest.raises(universe.UniverseError, match="no session on or before 1997-12-31"):
        universe.snapshot(date(1997, 12, 31), root)


@pytest.mark.parametrize(
    "odd",
    [
        pytest.param({"price": 4.5}, id="under five dollars"),
        pytest.param({"volume": 1_000.0}, id="too thin to trade"),
        pytest.param({"shares": 1_000_000.0}, id="too small to matter"),
        pytest.param({"shares": 15_000_000.0}, id="exactly three hundred million"),
        pytest.param({"shares": None}, id="never filed"),
        pytest.param({"category": "ETF"}, id="an ETF"),
        pytest.param({"category": "ADR Common Stock"}, id="an ADR"),
        pytest.param({"category": "Domestic Common Stock Warrant"}, id="a warrant"),
        pytest.param({"category": "Domestic Preferred Stock"}, id="preferred"),
        pytest.param({"industry": "REIT - Diversified"}, id="a REIT by industry"),
        pytest.param({"siccode": "6798"}, id="a REIT by SIC code"),
        pytest.param({"table": "fundamentals"}, id="not in the stocks table"),
    ],
)
def test_a_name_that_fails_one_filter_is_left_out(tmp_path, odd):
    root = Market(tmp_path).name("GOOD").name("ODD", **odd).land()

    assert universe.snapshot(MONTH_END, root).tickers == ("GOOD",)


def test_a_name_that_did_not_trade_on_the_session_is_left_out(tmp_path):
    sessions = weekdays(SESSION, 60)
    root = Market(tmp_path).name("GOOD").name("HALT", sessions[:-1]).land()

    assert universe.snapshot(MONTH_END, root).tickers == ("GOOD",)


def test_a_filing_is_invisible_until_its_datekey(tmp_path):
    # LATE's older filing leaves it under the market cap floor; the one it files on 2026-08-20
    # lifts it over. The lake holds both from the start, as it would after a backfill.
    builder = Market(tmp_path).name("GOOD")
    builder.name("LATE", shares=1_000_000.0, datekey=date(2025, 6, 30))
    builder.files("LATE", datekey=date(2026, 8, 20), shares=100_000_000.0)
    root = builder.land()

    before = universe.snapshot(date(2026, 8, 19), root)
    after = universe.snapshot(date(2026, 8, 20), root)

    assert "LATE" not in before.tickers
    assert "LATE" in after.tickers
    assert after.members[1].market_cap == pytest.approx(2_000_000_000.0)


def test_the_latest_filing_on_or_before_the_date_wins(tmp_path):
    builder = Market(tmp_path).name("GOOD", shares=100_000_000.0, datekey=date(2026, 2, 10))
    builder.files("GOOD", datekey=date(2026, 8, 10), shares=50_000_000.0)
    root = builder.land()

    assert universe.snapshot(MONTH_END, root).members[0].market_cap == pytest.approx(1e9)


def test_a_restated_dimension_is_never_read(tmp_path):
    # MRQ carries the vendor's restatement of the same quarter. Only the as-reported share
    # count may decide membership, so a name with nothing but MRQ has no fundamentals at all.
    builder = Market(tmp_path).name("GOOD")
    builder.name("REST", shares=100_000_000.0, dimension="MRQ")
    root = builder.land()

    assert universe.snapshot(MONTH_END, root).tickers == ("GOOD",)


def test_a_name_delisted_later_is_in_the_universes_before_it(tmp_path):
    # DELI trades through 2008 and stops. It keeps its row in the tickers table, as the vendor
    # keeps delisted names, so it stays classifiable long after its last print.
    gone = date(2008, 9, 12)
    builder = Market(tmp_path).name("GOOD")
    builder.name("DELI", weekdays(gone, 60), datekey=date(2008, 7, 10))
    root = builder.land()

    delisting_year = universe.snapshot(date(2008, 8, 29), root)
    today = universe.snapshot(MONTH_END, root)

    assert delisting_year.tickers == ("DELI",)
    assert delisting_year.snapshot_date == date(2008, 8, 29)
    assert today.tickers == ("GOOD",)


def test_market_cap_and_price_use_the_close_as_traded(tmp_path):
    # SPLT traded at $6 and split three for one afterwards, so its adjusted close is $2 while
    # its share count was reported before the split. Screening on the adjusted close would put
    # it under $5 and value it at a third of what it was worth.
    root = Market(tmp_path).name("SPLT", price=6.0, close=2.0, shares=60_000_000.0).land()

    result = universe.snapshot(MONTH_END, root)

    assert result.tickers == ("SPLT",)
    assert result.members[0].price == pytest.approx(6.0)
    assert result.members[0].market_cap == pytest.approx(360_000_000.0)
    # Dollar volume stays on the adjusted close, whose product with adjusted volume is what the
    # name really turned over.
    assert result.members[0].dollar_volume == pytest.approx(2_000_000.0)


def test_a_share_factor_scales_the_market_cap(tmp_path):
    root = Market(tmp_path).name("HALF", shares=100_000_000.0, sharefactor=0.1).land()

    assert universe.snapshot(MONTH_END, root).tickers == ()


def test_the_universe_records_the_session_and_the_partitions_it_read(tmp_path):
    root = market(tmp_path)

    result = universe.snapshot(MONTH_END, root)

    assert result.as_of == MONTH_END
    assert result.snapshot_date == SESSION
    assert len(result.partition_hash) == 64
    assert result.partition_hash == universe.snapshot(MONTH_END, root).partition_hash
    assert result.partition_hash == lake.partition_digest(root, SESSION)


def test_a_session_landed_after_the_date_leaves_the_digest_alone(tmp_path):
    root = market(tmp_path)
    before = universe.snapshot(MONTH_END, root)

    Market(root).trades("GOOD", [date(2026, 9, 1)]).land()

    after = universe.snapshot(MONTH_END, root)
    assert after.partition_hash == before.partition_hash
    assert universe.snapshot(date(2026, 9, 1), root).partition_hash != before.partition_hash


def test_a_restated_filing_moves_the_digest(tmp_path):
    # SF1 is partitioned by the day the vendor last changed a row, so a restatement lands in a
    # new partition. The digest covers every SF1 partition, so the gate sees the lake move.
    root = market(tmp_path)
    before = universe.snapshot(MONTH_END, root)

    table = pa.Table.from_pylist(
        [{"ticker": "GOOD", "dimension": "ARQ", "datekey": date(2026, 6, 30),
          "lastupdated": date(2026, 9, 30), "sharesbas": 9e7, "sharefactor": 1.0}],
        schema=_SF1,
    )  # fmt: skip
    lake.land(root, lake.SF1, table, SESSION)

    assert universe.snapshot(MONTH_END, root).partition_hash != before.partition_hash


def test_a_digest_cache_gives_the_same_answer(tmp_path):
    root = market(tmp_path)
    hashes: dict[object, str] = {}

    first = universe.snapshot(MONTH_END, root, hashes=hashes)
    second = universe.snapshot(MONTH_END, root, hashes=hashes)

    assert hashes
    assert first.partition_hash == second.partition_hash == lake.partition_digest(root, SESSION)


def test_an_open_connection_is_reused_rather_than_rebuilt(tmp_path):
    root = market(tmp_path)

    with lake.connect(root) as db:
        first = universe.snapshot(date(2026, 7, 31), root, db=db)
        second = universe.snapshot(MONTH_END, root, db=db)
        assert db.sql("SELECT count(*) FROM sep").fetchone()[0] > 0  # still open

    assert first.tickers == second.tickers == ("GOOD",)


def test_the_gate_s_band_is_the_one_the_ticket_names():
    assert (universe.MIN_SIZE, universe.MAX_SIZE) == (2_500, 5_000)
    assert not universe.size_is_plausible(200)
    assert not universe.size_is_plausible(7_000)
    assert universe.size_is_plausible(3_500)


def test_a_universe_reports_whether_its_size_is_plausible(tmp_path):
    result = universe.snapshot(MONTH_END, market(tmp_path))

    assert result.size == 1
    assert result.plausible_size is False


def test_month_ends_run_from_1998_and_are_sessions():
    ends = universe.month_ends(date(1998, 1, 1), date(2026, 8, 31))

    assert ends[0] == date(1998, 1, 30)  # 31 January 1998 was a Saturday
    assert ends[-1] == date(2026, 8, 31)
    assert len(ends) == (2026 - 1998) * 12 + 8
    assert all(a < b for a, b in zip(ends, ends[1:], strict=False))
    assert date(2026, 5, 29) in ends  # 31 May 2026 was a Sunday
    assert universe.month_ends(date(2026, 8, 1), date(2026, 7, 1)) == []


def test_a_universe_builds_for_a_month_end_anywhere_in_the_history(tmp_path):
    ends = [date(1998, 1, 30), date(2008, 8, 29), date(2020, 5, 29), date(2026, 8, 31)]
    assert set(ends) <= set(universe.month_ends(date(1998, 1, 1), date(2026, 8, 31)))
    root = Market(tmp_path).name("GOOD", ends, datekey=date(1997, 11, 14)).land()

    with lake.connect(root) as db:
        built = [universe.snapshot(end, root, db=db) for end in ends]

    assert [result.snapshot_date for result in built] == ends
    assert all(result.tickers == ("GOOD",) for result in built)


def test_a_lake_missing_a_dataset_says_which(tmp_path):
    root = Market(tmp_path).trades("GOOD", weekdays(SESSION, 60)).land()

    with pytest.raises(universe.UniverseError, match="no sf1 partitions"):
        universe.snapshot(MONTH_END, root)


def test_a_lake_missing_a_column_says_which(tmp_path):
    root = Market(tmp_path).name("GOOD", shares=None).land()
    thin = pa.Table.from_pylist(
        [{"ticker": "GOOD", "dimension": "ARQ", "datekey": date(2026, 6, 30),
          "lastupdated": date(2026, 9, 30)}],
        schema=pa.schema([("ticker", pa.string()), ("dimension", pa.string()),
                          ("datekey", pa.date32()), ("lastupdated", pa.date32())]),
    )  # fmt: skip
    lake.land(root, lake.SF1, thin, SESSION)

    with pytest.raises(universe.UniverseError, match=r"sf1 lacks \['sharefactor', 'sharesbas'\]"):
        universe.snapshot(MONTH_END, root)


def test_the_command_prints_the_provenance_and_the_members(tmp_path, capsys):
    root = market(tmp_path)

    universe.main(["--as-of", MONTH_END.isoformat(), "--lake", str(root), "--tickers"])

    printed = dict(line.split("\t", 1) for line in capsys.readouterr().out.splitlines())
    assert printed["snapshot_date"] == SESSION.isoformat()
    assert printed["size"] == "1"
    assert printed["plausible_size"] == "False"
    assert printed["partition_hash"] == lake.partition_digest(root, SESSION)
    assert printed["GOOD"].startswith("20.0000\t")


def test_the_command_reports_a_lake_it_cannot_read(tmp_path, capsys):
    with pytest.raises(SystemExit, match="no sep partitions"):
        universe.main(["--as-of", MONTH_END.isoformat(), "--lake", str(tmp_path)])


def test_the_digest_covers_what_a_query_reads_and_no_more(tmp_path):
    root = Market(tmp_path).name("GOOD", [date(2026, 8, 28), SESSION]).land()
    # A second tickers snapshot, as the next pull would land. `lake.connect` shows the newest
    # one, so only that one belongs in the digest.
    Market(root).listed("GOOD").land_tickers(date(2026, 9, 1))

    read = [str(path.relative_to(root).parent) for path in lake.partition_files(root, SESSION)]

    assert read == [
        "sep/date=2026-08-28",
        f"sep/date={SESSION}",
        f"sf1/lastupdated={SESSION}",
        "tickers/snapshot=2026-09-01",
    ]
