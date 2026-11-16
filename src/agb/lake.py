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

A pull lands every session since the last one the lake holds, not only its own, so a session
missed to an outage lands with the next pull that runs. A session is landed only once it looks
complete: it has at least FLOOR of the rows the session before it had, so a session the vendor
is still publishing is left for a later try rather than landed short. SF1 is fetched from a
week before the last pull that landed, so an outage loses no fundamentals.

A request the vendor did not answer, or answered with a 429 or 5xx, is made again with a
doubling backoff, or after the vendor's own `Retry-After`. A refusal (a 403 for a bad key) is
not. Each query pages on the vendor's cursor; see `Sharadar.query`.

SF1 keeps the vendor's `date` column, the SEC filing date, under the name `datekey`, so research
can use only what had been filed by a given day, and its `lastupdated` column, so research can
use each filing as the vendor showed it on that day rather than as since restated. Delisted
tickers are never filtered out.

The vendor adjusts SEP prices backwards: a split or dividend rescales `close` and `closeadj` on
every session before its ex-date, as the vendor serves them from then on. So each pull fetches
from the lake's last session, the overlap day, as well as the sessions after it. A ticker the
vendor now prices differently on that day has had an action since it landed, and its whole
history is fetched again and written over the partitions the lake holds, so its adjusted series
stays on one basis. `inconsistent` flags a ticker whose stored series still has two bases
meeting.

`connect` opens DuckDB over the lake, and `python -m agb.lake "SQL"` runs one query;
`python -m agb.lake --check` lists the tickers `inconsistent` flags.
"""

import argparse
import csv
import hashlib
import http.client
import io
import os
import re
import shutil
import sys
import tempfile
import time
import urllib.error
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

from agb import nyse

BASE_URL = "https://api.sharadar.com/v1.0/data"
PAGE = 10_000  # the most rows the API returns for one request
TIMEOUT = 300  # seconds
# The backfill keeps SEP from this session and SF1 from this filing date on.
SINCE = date(1998, 1, 1)
# Each pull re-lands SF1 from this long before the last pull that landed, to pick up rows the
# vendor stamped with a day after that pull ran, or on a weekend.
SF1_LOOKBACK = timedelta(days=7)
# A pull fills a gap back to the lake's last session, but not one longer than this: a lake that
# far behind needs the backfill, which is one download rather than thousands of pages.
MAX_GAP = timedelta(days=45)
# A session is complete once it has at least this share of the rows of the session before it.
# The count of listed stocks moves by a few a day; a session the vendor is still publishing is
# short by far more.
FLOOR = 0.9
# The SEP prices the vendor adjusts backwards: a pull compares them on the overlap day to tell
# that a split or dividend has rescaled a ticker's history since it landed.
ADJUSTED = ("close", "closeadj")
# The consistency check's allowance for the vendor's rounding of each row's prices: this share
# of the close, and this much of a dollar.
RELATIVE_SLACK = 1e-4
ROUNDING = 0.01
# How many times a request is made before its failure stands, the wait before the second
# attempt, doubling for each one after it, and the longest `Retry-After` waited out. Asked to
# wait longer, the pull fails and the next day's pull lands the session.
ATTEMPTS = 3
BACKOFF = 10.0  # seconds
RETRY_AFTER_CAP = 120.0  # seconds
# The statuses worth another attempt: the vendor's rate limit, a request timeout and its own
# failures. Any other status is a refusal that asking again would not change.
RETRY_STATUS = frozenset({408, 429, 500, 502, 503, 504})
# The response header that carries the next page's cursor, and the parameter that asks for it,
# as the vendor's table API (Nasdaq Data Link's datatables) names them for a CSV answer.
CURSOR_HEADER = "cursor_id"
CURSOR_PARAM = "qopts.cursor_id"
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
    """The vendor has not yet published, or not yet finished publishing, the session a pull
    asked for. `landed` holds the partitions of the earlier sessions the pull did land."""

    def __init__(self, message: str, landed: list["Partition"] | None = None) -> None:
        super().__init__(message)
        self.landed = landed or []


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
    # One filing can report more than one period, as when a late filer catches up on two
    # quarters in a day, so the period it reports is part of the key.
    primary_key=("ticker", "dimension", "datekey", "reportperiod"),
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
    """The vendor's table API. `opener` fetches a URL, following redirects; tests pass a fake,
    and a `sleep` that does not wait. The key goes in each URL, so no error message this
    raises includes a URL with it."""

    def __init__(
        self,
        api_key: str,
        opener: Opener = urlopen,
        base_url: str = BASE_URL,
        page: int = PAGE,
        *,
        attempts: int = ATTEMPTS,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key:
            raise LakeError("SHARADAR_API_KEY is not set in the pipeline's env file")
        self.api_key = api_key
        self.opener = opener
        self.base_url = base_url
        self.page = page
        self.attempts = attempts
        self.sleep = sleep

    def _fetch(self, table: str, params: Mapping[str, object], out: BinaryIO) -> str | None:
        """Write the answer to `out`, trying again on a transient failure. Returns the cursor
        header the answer carried: None when it had none, "" when it was empty."""
        url = f"{self.base_url}/{table}?{urlencode({**params, 'api_key': self.api_key})}"
        wait = BACKOFF
        for attempt in range(1, self.attempts + 1):
            try:
                with self.opener(url) as response:
                    shutil.copyfileobj(response, out, CHUNK)
                    headers = getattr(response, "headers", None)
                    return None if headers is None else headers.get(CURSOR_HEADER)
            except (OSError, http.client.HTTPException) as exc:
                detail = str(exc).replace(self.api_key, "***")
                after = _wait_for(exc, wait)
                if attempt == self.attempts or after is None:
                    tries = f" after {attempt} attempts" if attempt > 1 else ""
                    raise LakeError(
                        f"GET {table}?{urlencode(params)} failed{tries}: {detail}"
                    ) from None
                self.sleep(after)
                wait *= 2
                out.seek(0)
                out.truncate()
        raise AssertionError("unreachable: the loop returns or raises")

    def query(self, dataset: Dataset, filters: Mapping[str, object]) -> pa.Table:
        """Every row of `dataset` that matches `filters`, fetched a page at a time.

        Each answer names the next page's cursor in its CURSOR_HEADER, and the last page names
        none, so paging ends when the vendor says the rows have run out, whatever the length of
        the page it ends on. A vendor that sends no cursor at all is paged by `skip` instead,
        and its rows are taken to have run out on the first page shorter than `page`."""
        pages: list[pa.Table] = []
        cursor: str | None = None
        while True:
            params: dict[str, object] = {**filters, "sort": dataset.sort, "limit": self.page}
            if cursor:
                params[CURSOR_PARAM] = cursor
            else:
                params["skip"] = sum(page.num_rows for page in pages)
            out = io.BytesIO()
            following = self._fetch(dataset.api, params, out)
            data = out.getvalue()
            if data.lstrip()[:1] in (b"{", b"<"):
                shown = data[:300].decode(errors="replace").replace(self.api_key, "***")
                raise LakeError(
                    f"GET {dataset.api}?{urlencode(params)} did not return CSV: {shown}"
                )
            pages.append(parse(dataset, data))
            if cursor or following is not None:
                # The vendor pages by cursor: it has said where the next page starts, or that
                # there is none.
                if not following:
                    return pa.concat_tables(pages)
                if following == cursor:
                    raise LakeError(f"GET {dataset.api}: the vendor's cursor did not move")
                cursor = following
            elif pages[-1].num_rows < self.page:
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


def _wait_for(exc: BaseException, backoff: float) -> float | None:
    """How long to wait before trying a failed request again, or None if it should not be.
    A status other than RETRY_STATUS is a refusal. The vendor's own `Retry-After` wins over the
    backoff when it sent one, unless it asks for longer than RETRY_AFTER_CAP."""
    if not isinstance(exc, urllib.error.HTTPError):
        return backoff  # no answer: a timeout, a refused or dropped connection
    if exc.code not in RETRY_STATUS:
        return None
    after = _seconds(exc.headers.get("Retry-After") if exc.headers else None)
    if after is None:
        return backoff
    return after if after <= RETRY_AFTER_CAP else None


def _seconds(value: object) -> float | None:
    """A `Retry-After` header in its number-of-seconds form. The HTTP-date form is not read,
    and leaves the backoff in charge."""
    if value is None:
        return None
    try:
        seconds = float(str(value).strip())
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


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


def _last(lake: Path, dataset: Dataset, before: date) -> tuple[date, Path] | None:
    """The newest partition of `dataset` dated before `before`, and its file."""
    cutoff = before.isoformat()
    for path in sorted(lake.glob(f"{dataset.name}/{dataset.partition}=*/{FILE}"), reverse=True):
        value = path.parent.name.partition("=")[2]
        if value < cutoff:
            return date.fromisoformat(value), path
    return None


def _restated(stored: pa.Table, fetched: pa.Table) -> list[str]:
    """The tickers whose prices on a session the lake holds the vendor now gives on another
    basis: a `close` or `closeadj` in `fetched` that differs from the one in `stored`. The vendor
    adjusts backwards, so a split or dividend since that session rescales its prices. A price
    column one of them lacks is left out of the comparison."""
    compared = [c for c in ADJUSTED if c in stored.schema.names and c in fetched.schema.names]
    old = stored.select(["ticker", *compared])
    new = fetched.select(["ticker", *compared])
    new = new.rename_columns(["ticker", *(f"new_{c}" for c in compared)])
    both = old.join(new, "ticker", join_type="inner")
    moved = pa.array([False] * both.num_rows, pa.bool_())
    for column in compared:
        before, after = both.column(column), both.column(f"new_{column}")
        differs = pc.fill_null(
            pc.not_equal(before, after), pc.xor(pc.is_null(before), pc.is_null(after))
        )
        moved = pc.or_(moved, differs)
    return sorted(both.filter(moved).column("ticker").to_pylist())


def _history(client: Sharadar, tickers: list[str], through: date) -> pa.Table | None:
    """The SEP prices of each of `tickers` from SINCE to `through`, all on today's basis."""
    if not tickers:
        return None
    return pa.concat_tables(
        [client.query(SEP, {"ticker": t, "date.gte": SINCE, "date.lte": through}) for t in tickers],
        promote_options="default",
    )


def restate(lake: Path, history: pa.Table) -> list[Partition]:
    """Replace, in each SEP partition the lake holds, the rows of the tickers in `history` with
    `history`'s rows for that session. A session the lake does not hold is not created."""
    tickers = pc.unique(history.column("ticker"))
    partitions = []
    for value, rows in split(history, SEP.key):
        path = lake / SEP.name / f"{SEP.partition}={value}" / FILE
        if not path.exists():
            continue
        old = pq.read_table(path)
        kept = old.filter(pc.invert(pc.is_in(old.column("ticker"), value_set=tickers)))
        table = pa.concat_tables([kept, rows], promote_options="default")
        partitions.append(write_partition(lake, SEP, value, table.select(old.schema.names)))
    return partitions


def pull(lake: Path, client: Sharadar, session: date) -> list[Partition]:
    """Land `session` and every session since the last one the lake holds: their SEP prices,
    the SF1 rows the vendor changed from a week before the last pull to `session`, and the
    tickers table as it stands. Nothing is written unless every fetch succeeds.

    Sessions land in order, each once it has at least FLOOR of the rows of the session before
    it. The first that is missing or short stops the pull, which raises NotYet carrying the
    partitions of the sessions before it that did land; SF1 and the tickers land only with
    `session` itself. Raises LakeError if the lake's last session is more than MAX_GAP before
    `session`.

    The fetch starts on the lake's last session, the overlap day, which is not landed again. A
    ticker the vendor now prices differently on that day has had a split or dividend since:
    the vendor adjusts backwards, so its whole history moved. That ticker's history is fetched
    again and restated in every partition the lake holds, so its adjusted series stays on one
    basis. The restated partitions are among those returned, or those NotYet carries."""
    previous = _last(lake, SEP, session)
    if previous is not None and session - previous[0] > MAX_GAP:
        raise LakeError(
            f"the lake's last session before {session} is {previous[0]}, more than"
            f" {MAX_GAP.days} days back; land the gap with the backfill"
        )
    if previous is None:
        expected = [session]
        sep = client.query(SEP, {"date": session})
        floor = 0
        restated: list[str] = []
    else:
        start = previous[0] + timedelta(days=1)
        calendar = nyse.NyseCalendar(previous[0] - timedelta(days=7), session + timedelta(days=7))
        expected = [*calendar.sessions(start, session), session]
        sep = client.query(SEP, {"date.gte": previous[0], "date.lte": session})
        stored = pq.read_table(previous[1])
        floor = stored.num_rows
        overlap = sep.filter(pc.equal(sep.column(SEP.key), pa.scalar(previous[0])))
        restated = _restated(stored, overlap)
        sep = sep.filter(pc.greater(sep.column(SEP.key), pa.scalar(previous[0])))
    by_session = {date.fromisoformat(value): rows for value, rows in split(sep, SEP.key)}
    complete: list[tuple[date, pa.Table]] = []
    short: str | None = None
    for day in sorted({*expected, *by_session}):
        rows = by_session.get(day)
        count = 0 if rows is None else rows.num_rows
        if count == 0:
            short = f"the vendor has no SEP prices for {day} yet"
        elif count < FLOOR * floor:
            short = (
                f"the vendor has {count} SEP prices for {day}, short of {FLOOR:.0%} of the"
                f" {floor} the session before it had; it is still publishing"
            )
        if short:
            break
        complete.append((day, rows))
        floor = count
    # A restatement lands with the first session after the overlap day, never without one: the
    # next pull's overlap day is then the newest landed session, on the vendor's new basis, and
    # would show no change.
    history = _history(client, restated, previous[0]) if complete and previous else None
    if short is None:
        last_pull = _last(lake, TICKERS, session)
        since = min(session, last_pull[0]) if last_pull else session
        sf1 = client.query(
            SF1, {"lastupdated.gte": since - SF1_LOOKBACK, "lastupdated.lte": session}
        )
        sf1 = sf1.filter(pc.greater_equal(sf1.column("datekey"), pa.scalar(SINCE)))
        tickers = client.tickers()
        if tickers.num_rows == 0:
            raise LakeError("the vendor returned no tickers")
    landed = [p for day, rows in complete for p in land(lake, SEP, rows, day)]
    if history is not None:
        landed += restate(lake, history)
    if short is not None:
        raise NotYet(short, landed)
    return [*landed, *land(lake, SF1, sf1, session), *land(lake, TICKERS, tickers, session)]


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
_SF1_PERIOD = ", ".join(SF1.primary_key)
_VIEW_FILTER = {
    SF1.name: (
        f"QUALIFY row_number() OVER (PARTITION BY {_SF1_PERIOD} ORDER BY lastupdated DESC) = 1"
    ),
}
# `sf1_as_of(d)` is SF1 as it was known on day `d`: the rows filed by then, each in the latest
# version the vendor had stamped by then. A row the lake first saw after `d` is served in the
# earliest version it has, since nothing older was ever landed: the backfill holds each row only
# as the vendor showed it on the day of the export, so a row filed before the lake began has no
# earlier version to fall back to.
_SF1_AS_OF = f"""
CREATE MACRO sf1_as_of(as_of) AS TABLE
SELECT * FROM {{source}}
WHERE datekey <= as_of
QUALIFY row_number() OVER (
    PARTITION BY {_SF1_PERIOD}
    ORDER BY CASE WHEN lastupdated <= as_of THEN lastupdated END DESC NULLS LAST, lastupdated
) = 1
"""


def connect(lake: Path = DEFAULT_DIR) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB with a view over the lake for each dataset that has landed:
    `sep`, every session's prices; `sf1`, the latest version of each fundamentals row; and
    `tickers`, the newest snapshot as of this call. A query that stands on a past day reads
    `sf1_as_of(day)` instead of `sf1`, the fundamentals as they were known that day.

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
        if dataset is SF1:
            conn.execute(_SF1_AS_OF.format(source=source))
    return conn


# Each row's two adjustment factors: the dividend factor `closeadj / close`, which the vendor's
# backward adjustment keeps in (0, 1] and never lets fall from one session to the next, and the
# split factor `closeunadj / close`, which moves only on a split's ex-date, where it takes up the
# jump in the unadjusted price. A row's slack is its prices' rounding as a share of its close.
_INCONSISTENT = """
WITH f AS (
    SELECT ticker, date, close, closeunadj,
        closeadj / close AS dividend,
        closeunadj / close AS split,
        ? + ? / close AS slack
    FROM sep
    WHERE close > 0 AND closeadj IS NOT NULL AND closeunadj > 0
), d AS (
    SELECT *,
        lag(dividend) OVER w AS prior_dividend,
        lag(split) OVER w AS prior_split,
        lag(close) OVER w AS prior_close,
        lag(closeunadj) OVER w AS prior_unadj,
        lag(slack) OVER w AS prior_slack
    FROM f
    WINDOW w AS (PARTITION BY ticker ORDER BY date)
)
SELECT DISTINCT ticker FROM d
WHERE dividend <= 0
    OR dividend > 1 + slack
    OR dividend < prior_dividend - slack - prior_slack
    OR (
        abs(ln(split / prior_split)) > slack + prior_slack
        AND abs(ln(close / prior_close)) > abs(ln(closeunadj / prior_unadj))
    )
ORDER BY ticker
"""


def inconsistent(lake: Path = DEFAULT_DIR) -> list[str]:
    """The tickers whose stored SEP prices are not on one adjustment basis.

    The vendor adjusts backwards, so one consistent series has a dividend factor
    `closeadj / close` in (0, 1] that never falls from one session to the next, and a split
    factor `closeunadj / close` that changes only on a split's ex-date, where the split-adjusted
    close moves less than the unadjusted one. A ticker flagged here has two bases meeting: the
    newer rows rescaled for a dividend the older ones were not, or a jump in `close` where the
    unadjusted price did not jump, which is a split the older rows missed. ROUNDING allows for
    the vendor's rounding of prices.

    A lake that missed a refresh across the whole of a ticker's history, so that every row
    stands as the vendor showed it on its own day, has no seam to find: each row is consistent
    with its neighbours on its own basis. Re-landing history with the backfill is what repairs
    that."""
    if not any(lake.glob(f"{SEP.name}/{SEP.partition}=*/{FILE}")):
        return []
    with connect(lake) as conn:
        rows = conn.execute(_INCONSISTENT, [RELATIVE_SLACK, ROUNDING]).fetchall()
    return [str(ticker) for (ticker,) in rows]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m agb.lake",
        description="Run one DuckDB query over the lake's views: sep, sf1 and tickers. "
        "Prints tab-separated rows under a header.",
    )
    parser.add_argument("sql", nargs="?")
    parser.add_argument(
        "--lake", type=Path, default=Path(os.environ.get("AGB_LAKE_DIR", DEFAULT_DIR))
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="instead of a query, list the tickers whose SEP prices are not on one adjustment"
        " basis, and exit 1 if there are any",
    )
    args = parser.parse_args(argv)
    if args.check:
        flagged = inconsistent(args.lake)
        for ticker in flagged:
            print(ticker)
        print(f"{len(flagged)} tickers inconsistent", file=sys.stderr)
        raise SystemExit(1 if flagged else 0)
    if args.sql is None:
        parser.error("give a query, or --check")
    relation = connect(args.lake).sql(args.sql)
    if relation is None:
        return
    print("\t".join(relation.columns))
    for row in relation.fetchall():
        print("\t".join("" if value is None else str(value) for value in row))


if __name__ == "__main__":
    main()
