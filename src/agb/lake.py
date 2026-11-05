"""The Parquet lake: Sharadar's SEP, SF1 and TICKERS tables as Parquet under one directory.

The server is the only machine that talks to the vendor. It lands the lake on its data disk,
and the workstation later copies the same bytes. Each dataset is a directory of partitions,
one Parquet file each:

    sep/date=YYYY-MM-DD/part.parquet          prices for one session
    sf1/lastupdated=YYYY-MM-DD/part.parquet   fundamentals rows the vendor last changed that day
    tickers/snapshot=YYYY-MM-DD/part.parquet  the tickers table as it stood when pulled

A partition's file is a function of its rows alone: they are sorted by the dataset's primary
key and written with fixed options, so landing the same rows twice writes the same bytes. The
partition hash is the SHA-256 of that file. A partition whose new bytes match the file already
there is left untouched, so re-running a pull for a date that already landed changes nothing.
A SEP partition takes the vendor's corrections; an SF1 partition keeps each row the vendor
has since restated; a tickers snapshot, once landed, is never rewritten.

SF1 keeps the vendor's `date` column, the SEC filing date, under the name `datekey`, so research
can use only what had been filed by a given day. Delisted tickers are never filtered out.

`connect` opens DuckDB over the lake, and `python -m agb.lake "SQL"` runs one query.
"""

import argparse
import csv
import hashlib
import http.client
import io
import os
import re
import shutil
import tempfile
import urllib.request
import zipfile
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import BinaryIO
from urllib.parse import urlencode

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

BASE_URL = "https://api.sharadar.com/v1.0/data"
PAGE = 10_000  # the most rows the API returns for one request
TIMEOUT = 300  # seconds
# The backfill keeps SEP from this session and SF1 from this filing date on.
SINCE = date(1998, 1, 1)
# Each pull re-lands SF1 for the week to its session, to pick up rows the vendor stamped with a
# day after that day's pull ran, or on a weekend.
SF1_LOOKBACK = timedelta(days=7)
# The tables whose tickers the lake keeps. TICKERS also lists the vendor's other tables.
TICKER_TABLES = ("stocks", "fundamentals")
DEFAULT_DIR = Path("/lake")
FILE = "part.parquet"
# Where a query that outgrows memory spills. It sits in the lake directory, beside the datasets,
# whose names are the ones in DATASETS, so the leading dot keeps it out of their way.
SPILL = ".query"
CHUNK = 1 << 20
# A zip file's first bytes. The vendor answers a bulk request with a redirect to a zip, but an
# error can arrive as an ordinary 200 whose body is JSON or XML, which would otherwise only show
# up later as a corrupt archive.
ZIP_MAGIC = b"PK\x03\x04"
# DuckDB's memory while it splits a bulk export; it spills the rest to disk. The pipeline
# container is limited to 2176m, measured (docs/build/measured-limits.md).
BULK_MEMORY = "1GB"
# A bulk export is written as one Parquet file per partition, and SEP's history from 1998 is
# about 7,200 daily partitions. DuckDB buffers rows for every distinct partition value one COPY
# writes, and a partitioned write does not spill those buffers, so a single COPY over the whole
# history runs out of BULK_MEMORY no matter how few files it holds open. land_bulk copies a year
# at a time instead, which is at most 366 partitions. Holding open more files than a year has
# dates then leaves one file per partition, rather than a new file each time DuckDB cycles past
# a partition it had closed.
BULK_THREADS = 2
BULK_OPEN_FILES = 400
BULK_FLUSH_ROWS = 65_536
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")

STRING, FLOAT, DATE, INT = pa.string(), pa.float64(), pa.date32(), pa.int64()
_DUCKDB_TYPES = {STRING: "VARCHAR", FLOAT: "DOUBLE", DATE: "DATE", INT: "BIGINT"}


class LakeError(Exception):
    """A pull that cannot land: the vendor refused, failed, or sent something unexpected."""


class NotYet(LakeError):
    """The vendor has not yet published the session a pull asked for."""


@dataclass(frozen=True)
class Dataset:
    """One of the vendor's tables as the lake keeps it. Column names are the vendor's, except
    where `rename` maps them to the lake's."""

    name: str  # the lake directory and the DuckDB view
    api: str  # the table's name in the API
    partition: str  # the label in each partition's directory name
    key: str | None  # the column whose value names the partition; None for one per pull
    primary_key: tuple[str, ...]  # the order rows are written in
    sort: str  # the API's sort order, so pages do not overlap
    types: Mapping[str, pa.DataType]  # the columns whose type is not `default`
    default: pa.DataType
    since: str | None = None  # the date column the backfill's start applies to
    rename: Mapping[str, str] = field(default_factory=dict)
    # What landing rows does to a partition that already has a file: "replace" it, "merge"
    # into it, the new rows replacing those with the same primary key, or "keep" it as it is.
    existing: str = "replace"

    def vendor(self, column: str) -> str:
        return {v: k for k, v in self.rename.items()}.get(column, column)

    def schema(self, header: list[str]) -> pa.Schema:
        """The lake's schema for a CSV with this header. Raises LakeError if the header lacks a
        column the lake depends on, or has a name that is not a plain identifier."""
        odd = [name for name in header if not _NAME.match(name)]
        if odd:
            raise LakeError(f"{self.name}: unexpected column names {odd}")
        needed = {self.vendor(c) for c in (*self.primary_key, self.key, self.since) if c}
        missing = sorted(needed - set(header))
        if missing:
            raise LakeError(f"{self.name}: the vendor's columns lack {missing}")
        return pa.schema([(self.rename.get(n, n), self.types.get(n, self.default)) for n in header])


SEP = Dataset(
    name="sep",
    api="stocks",
    partition="date",
    key="date",
    primary_key=("ticker", "date"),
    sort="ticker.asc,date.asc",
    types={"ticker": STRING, "date": DATE, "lastupdated": DATE},
    default=FLOAT,
    since="date",
)
SF1 = Dataset(
    name="sf1",
    api="fundamentals",
    partition="lastupdated",
    key="lastupdated",
    primary_key=("ticker", "dimension", "datekey"),
    sort="ticker.asc,dimension.asc,date.asc",
    types={
        "ticker": STRING,
        "dimension": STRING,
        "fiscalperiod": STRING,
        "calendardate": DATE,
        "date": DATE,
        "reportperiod": DATE,
        "lastupdated": DATE,
    },
    default=FLOAT,
    since="datekey",
    rename={"date": "datekey"},
    # A row the vendor restates moves to the partition of its new `lastupdated`. Merging keeps
    # the version it replaced in the partition it first landed in.
    existing="merge",
)
TICKERS = Dataset(
    name="tickers",
    api="tickers",
    partition="snapshot",
    key=None,
    primary_key=("table", "permaticker", "ticker"),
    sort="permaticker.asc,ticker.asc",
    types={
        "permaticker": INT,
        "lastupdated": DATE,
        "firstadded": DATE,
        "firstpricedate": DATE,
        "lastpricedate": DATE,
        "firstquarter": DATE,
        "lastquarter": DATE,
    },
    default=STRING,
    # A snapshot is the table as it stood when first pulled.
    existing="keep",
)
DATASETS = (SEP, SF1, TICKERS)


def parse(dataset: Dataset, data: bytes) -> pa.Table:
    """One CSV response from the API, in the lake's schema. Blank values are nulls."""
    header, _, body = data.partition(b"\n")
    names = next(csv.reader([header.decode().rstrip("\r")]), [])
    schema = dataset.schema(names)
    if not body.strip():
        return schema.empty_table()
    try:
        table = pacsv.read_csv(
            io.BytesIO(body),
            read_options=pacsv.ReadOptions(column_names=names),
            convert_options=pacsv.ConvertOptions(
                column_types=dict(zip(names, schema.types, strict=True)),
                strings_can_be_null=True,
            ),
        )
    except pa.ArrowInvalid as exc:
        raise LakeError(f"{dataset.name}: cannot parse the vendor's CSV: {exc}") from None
    return table.rename_columns(schema.names)


Opener = Callable[[str], AbstractContextManager[BinaryIO]]


def urlopen(url: str) -> AbstractContextManager[BinaryIO]:
    return urllib.request.urlopen(url, timeout=TIMEOUT)


class Sharadar:
    """The vendor's table API. `opener` fetches a URL, following redirects; tests pass a fake.
    The key goes in each URL, so no error message this raises includes a URL with it."""

    def __init__(
        self, api_key: str, opener: Opener = urlopen, base_url: str = BASE_URL, page: int = PAGE
    ) -> None:
        if not api_key:
            raise LakeError("SHARADAR_API_KEY is not set in the pipeline's env file")
        self.api_key = api_key
        self.opener = opener
        self.base_url = base_url
        self.page = page

    def _fetch(self, table: str, params: Mapping[str, object], out: BinaryIO) -> None:
        url = f"{self.base_url}/{table}?{urlencode({**params, 'api_key': self.api_key})}"
        try:
            with self.opener(url) as response:
                shutil.copyfileobj(response, out, CHUNK)
        except (OSError, http.client.HTTPException) as exc:
            detail = str(exc).replace(self.api_key, "***")
            raise LakeError(f"GET {table}?{urlencode(params)} failed: {detail}") from None

    def query(self, dataset: Dataset, filters: Mapping[str, object]) -> pa.Table:
        """Every row of `dataset` that matches `filters`, fetched a page at a time."""
        pages: list[pa.Table] = []
        while True:
            skip = sum(page.num_rows for page in pages)
            params = {**filters, "sort": dataset.sort, "limit": self.page, "skip": skip}
            out = io.BytesIO()
            self._fetch(dataset.api, params, out)
            data = out.getvalue()
            if data.lstrip()[:1] in (b"{", b"<"):
                shown = data[:300].decode(errors="replace").replace(self.api_key, "***")
                raise LakeError(
                    f"GET {dataset.api}?{urlencode(params)} did not return CSV: {shown}"
                )
            pages.append(parse(dataset, data))
            if pages[-1].num_rows < self.page:
                return pa.concat_tables(pages)

    def tickers(self) -> pa.Table:
        """The tickers of the tables the lake keeps, delisted ones included."""
        return pa.concat_tables(self.query(TICKERS, {"table": t}) for t in TICKER_TABLES)

    def bulk(self, dataset: Dataset, target: Path) -> Path:
        """Download the vendor's export of all of `dataset`, a zip holding one CSV, to
        `target`. The API answers with a redirect to the file."""
        with open(target, "wb") as out:
            self._fetch(dataset.api, {"years": "full"}, out)
        with open(target, "rb") as f:
            head = f.read(300)
        if not head.startswith(ZIP_MAGIC):
            shown = head.decode(errors="replace").replace(self.api_key, "***")
            raise LakeError(
                f"GET {dataset.api}?years=full did not return a zip, but "
                f"{target.stat().st_size} bytes starting: {shown}"
            )
        return target


@dataclass(frozen=True)
class Partition:
    dataset: str
    path: str  # relative to the lake, e.g. "sep/date=2026-09-14"
    sha256: str
    rows: int


def file_hash(path: Path) -> str:
    with open(path, "rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def partition_files(lake: Path, through: date | None = None) -> list[Path]:
    """The partition files `connect` reads, in path order, with SEP cut to the sessions on or
    before `through`.

    Only SEP is cut. SF1 is partitioned by `lastupdated`, the day the vendor last changed a row,
    so a figure filed in 1999 sits in whichever partition the vendor most recently stamped it
    with; cutting SF1 by a date in the past would drop nearly all of history. What keeps a later
    filing out of an earlier result is the `datekey` gate in the query, not the partition it
    landed in. The tickers table is a snapshot, and `connect` shows the newest one.
    """
    files: list[Path] = []
    for dataset in DATASETS:
        landed = sorted(lake.glob(f"{dataset.name}/{dataset.partition}=*/{FILE}"))
        if not landed:
            continue
        if dataset.key is None:
            files.append(landed[-1])
        elif dataset is SEP and through is not None:
            cutoff = through.isoformat()
            files += [p for p in landed if p.parent.name.partition("=")[2] <= cutoff]
        else:
            files += landed
    return files


def partition_digest(
    lake: Path, through: date | None = None, *, hashes: dict[object, str] | None = None
) -> str:
    """A SHA-256 over the partitions a query up to `through` reads: one line of
    "<path in the lake> <file sha256>" each, in path order. It names the exact bytes a result
    was built from, so a basket can be tied to the data it saw, and the pre-trade gate can tell
    that the lake moved under it.

    `hashes` is a dictionary the caller keeps across calls. A file whose path, size and
    modification time are unchanged is not read a second time. Pass one when building many
    snapshots in a row; leave it out for a single build.
    """
    digest = hashlib.sha256()
    for path in partition_files(lake, through):
        digest.update(f"{path.relative_to(lake)} {_cached_hash(path, hashes)}\n".encode())
    return digest.hexdigest()


def _cached_hash(path: Path, hashes: dict[object, str] | None) -> str:
    if hashes is None:
        return file_hash(path)
    stat = path.stat()
    key = (str(path), stat.st_size, stat.st_mtime_ns)
    if key not in hashes:
        hashes[key] = file_hash(path)
    return hashes[key]


def _fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _merge(old: pa.Table, new: pa.Table, key: tuple[str, ...]) -> pa.Table:
    """The rows of `new`, and those of `old` whose primary key `new` lacks."""
    replaced = set(zip(*(new.column(c).to_pylist() for c in key), strict=True))
    kept = [k not in replaced for k in zip(*(old.column(c).to_pylist() for c in key), strict=True)]
    return pa.concat_tables(
        [old.filter(pa.array(kept, pa.bool_())), new], promote_options="default"
    )


def write_partition(lake: Path, dataset: Dataset, value: str, table: pa.Table) -> Partition:
    """Write `table` as the partition of `dataset` named `value`, as `dataset.existing` says
    if it already has a file. A file whose new bytes are the same as before is left as it
    was."""
    path = f"{dataset.name}/{dataset.partition}={value}"
    directory = lake / path
    directory.mkdir(parents=True, exist_ok=True)
    for stale in directory.glob(".*.partial"):  # left by a crash
        stale.unlink(missing_ok=True)
    final = directory / FILE
    if final.exists() and dataset.existing == "keep":
        return Partition(
            dataset.name, path, file_hash(final), pq.ParquetFile(final).metadata.num_rows
        )
    if final.exists() and dataset.existing == "merge":
        table = _merge(pq.read_table(final), table, dataset.primary_key)
    table = table.sort_by([(column, "ascending") for column in dataset.primary_key])
    fd, name = tempfile.mkstemp(prefix=".", suffix=".partial", dir=directory)
    partial = Path(name)
    try:
        with os.fdopen(fd, "wb") as out:
            pq.write_table(table, out, compression="zstd")
            out.flush()
            os.fsync(out.fileno())
        digest = file_hash(partial)
        if final.exists() and file_hash(final) == digest:
            partial.unlink()
        else:
            partial.chmod(0o644)
            partial.replace(final)
            _fsync_dir(directory)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    return Partition(dataset.name, path, digest, table.num_rows)


def split(table: pa.Table, key: str) -> Iterator[tuple[str, pa.Table]]:
    """The rows of `table` grouped by the date in `key`, in date order."""
    if table.column(key).null_count:
        raise LakeError(f"rows with no {key}")
    table = table.sort_by(key)
    start = 0
    for group in pc.value_counts(table.column(key)).to_pylist():
        yield group["values"].isoformat(), table.slice(start, group["counts"])
        start += group["counts"]


def land(lake: Path, dataset: Dataset, table: pa.Table, snapshot: date) -> list[Partition]:
    """Write `table` into the partitions of `dataset` its rows belong to. A dataset with no
    partition column lands whole, as the snapshot of `snapshot`."""
    if dataset.key is None:
        return [write_partition(lake, dataset, snapshot.isoformat(), table)]
    return [
        write_partition(lake, dataset, value, rows) for value, rows in split(table, dataset.key)
    ]


def pull(lake: Path, client: Sharadar, session: date) -> list[Partition]:
    """Land one session: its SEP prices, the SF1 rows the vendor changed in the week to it,
    and the tickers table as it stands. Nothing is written unless every fetch succeeds.
    Raises NotYet if the vendor has no prices for the session."""
    sep = client.query(SEP, {"date": session})
    if sep.num_rows == 0:
        raise NotYet(f"the vendor has no SEP prices for {session} yet")
    sf1 = client.query(SF1, {"lastupdated.gte": session - SF1_LOOKBACK, "lastupdated.lte": session})
    sf1 = sf1.filter(pc.greater_equal(sf1.column("datekey"), pa.scalar(SINCE)))
    tickers = client.tickers()
    if tickers.num_rows == 0:
        raise LakeError("the vendor returned no tickers")
    return [
        *land(lake, SEP, sep, session),
        *land(lake, SF1, sf1, session),
        *land(lake, TICKERS, tickers, session),
    ]


def _sql(text: object) -> str:
    """`text` for inside a single-quoted SQL literal."""
    return str(text).replace("'", "''")


def land_bulk(
    lake: Path, dataset: Dataset, archive: Path, since: date, work: Path
) -> list[Partition]:
    """Land the vendor's full export of `dataset`, keeping rows dated from `since` on.

    The export is too big for memory, so DuckDB first splits it into partitions on disk under
    `work`, spilling as it goes. Each partition is then written as a pull writes it, so its bytes
    match what a daily pull of the same rows lands.

    The split runs a year of the partition key at a time, because a partitioned write holds a
    buffer per partition and cannot spill them; see BULK_OPEN_FILES. Every copy after the first
    appends, so a year adds to what the last one staged instead of replacing it.
    """
    if dataset.key is None or dataset.since is None:
        raise ValueError(f"{dataset.name} has no bulk export")
    work.mkdir(parents=True, exist_ok=True)
    source = work / f"{dataset.name}.csv"
    try:
        with zipfile.ZipFile(archive) as z:
            members = [m for m in z.infolist() if not m.is_dir()]
            if len(members) != 1:
                found = [m.filename for m in members]
                raise LakeError(f"{dataset.name}: the export should hold one CSV, not {found}")
            with z.open(members[0]) as src, open(source, "wb") as dst:
                shutil.copyfileobj(src, dst, CHUNK)
    except zipfile.BadZipFile as exc:
        raise LakeError(f"{dataset.name}: the export is not a readable zip: {exc}") from None
    with open(source, newline="") as f:
        header = next(csv.reader(f), [])
    schema = dataset.schema(header)
    columns = ", ".join(
        f"'{n}': '{_DUCKDB_TYPES[t]}'" for n, t in zip(header, schema.types, strict=True)
    )
    select = ", ".join(f'"{n}" AS "{f.name}"' for n, f in zip(header, schema, strict=True))
    key = dataset.vendor(dataset.key)
    export = (
        f"""read_csv('{_sql(source)}', header = true, delim = ',', quote = '"', """
        f"""escape = '"', columns = {{{columns}}})"""
    )
    kept = (
        f"""WHERE "{dataset.vendor(dataset.since)}" >= DATE '{since.isoformat()}' """
        f"""AND "{key}" IS NOT NULL"""
    )
    staged = work / dataset.name
    shutil.rmtree(staged, ignore_errors=True)
    with duckdb.connect() as db:
        db.execute(f"SET temp_directory = '{_sql(work / 'spill')}'")
        db.execute(f"SET memory_limit = '{BULK_MEMORY}'")
        db.execute("SET preserve_insertion_order = false")
        db.execute(f"SET threads = {BULK_THREADS}")
        db.execute(f"SET partitioned_write_max_open_files = {BULK_OPEN_FILES}")
        db.execute(f"SET partitioned_write_flush_threshold = {BULK_FLUSH_ROWS}")
        span = f'SELECT min("{key}"), max("{key}") FROM {export} {kept}'
        first, last = db.execute(span).fetchone()
        for staged_years, year in enumerate(range(first.year, last.year + 1) if first else []):
            db.execute(f"""
                COPY (
                    SELECT {select}
                    FROM {export}
                    {kept}
                      AND "{key}" >= DATE '{year}-01-01'
                      AND "{key}" < DATE '{year + 1}-01-01'
                ) TO '{_sql(staged)}'
                (FORMAT parquet, PARTITION_BY ("{dataset.key}"),
                 WRITE_PARTITION_COLUMNS true{", APPEND" if staged_years else ""})
            """)
    source.unlink()
    partitions = []
    for directory in sorted(staged.iterdir()) if staged.exists() else []:
        parts = [pq.ParquetFile(f).read() for f in sorted(directory.glob("*.parquet"))]
        table = pa.concat_tables(parts).select(schema.names).cast(schema)
        value = directory.name.partition("=")[2]
        partitions.append(write_partition(lake, dataset, value, table))
        shutil.rmtree(directory)
    return partitions


# The lake keeps every day's version of an SF1 row; the view shows the latest.
_VIEW_FILTER = {
    SF1.name: "QUALIFY row_number() OVER "
    "(PARTITION BY ticker, dimension, datekey ORDER BY lastupdated DESC) = 1",
}


def connect(lake: Path = DEFAULT_DIR) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB with a view over the lake for each dataset that has landed:
    `sep`, every session's prices; `sf1`, the latest version of each fundamentals row; and
    `tickers`, the newest snapshot as of this call.

    A query that runs out of memory spills to DuckDB's temp directory, which defaults to `.tmp`
    under the working directory. The service image's working directory is not writable, so spill
    beside the lake, which the service owns and which has the room for it."""
    conn = duckdb.connect()
    if os.access(lake, os.W_OK):
        conn.execute(f"SET temp_directory = '{_sql(lake / SPILL)}'")
    for dataset in DATASETS:
        files = sorted(lake.glob(f"{dataset.name}/{dataset.partition}=*/{FILE}"))
        if not files:
            continue
        if dataset.key is None:
            source = f"read_parquet('{_sql(files[-1])}')"
        else:
            pattern = lake / dataset.name / f"{dataset.partition}=*" / FILE
            source = f"read_parquet('{_sql(pattern)}', hive_partitioning = false)"
        view_filter = _VIEW_FILTER.get(dataset.name, "")
        conn.execute(f"CREATE VIEW {dataset.name} AS SELECT * FROM {source} {view_filter}")
    return conn


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m agb.lake",
        description="Run one DuckDB query over the lake's views: sep, sf1 and tickers. "
        "Prints tab-separated rows under a header.",
    )
    parser.add_argument("sql")
    parser.add_argument(
        "--lake", type=Path, default=Path(os.environ.get("AGB_LAKE_DIR", DEFAULT_DIR))
    )
    args = parser.parse_args(argv)
    relation = connect(args.lake).sql(args.sql)
    if relation is None:
        return
    print("\t".join(relation.columns))
    for row in relation.fetchall():
        print("\t".join("" if value is None else str(value) for value in row))


if __name__ == "__main__":
    main()
