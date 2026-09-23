"""The research dataset: one row per name per month-end, with the shared features and the
label the ranker learns.

Each month-end's features are computed by `agb.features.history`, over that month's universe,
exactly as the live pipeline computes them for the rebalance. The label is the name's return
over the month the features were built to predict, bought at the close of the session after
the snapshot and sold at the close of the session after the next month's last session, the
same window `features.forward_window` gives the live basket. Within each month the returns
are graded into quintiles by `labels.grades`.

The panel carries the lake partition digest through its last exit session, so a trial can
name the exact bytes it learned from, and `partition_hash` mismatches tell a reproduction the
lake has moved."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from agb import cost, features, lake
from agb.research import labels

DATES = ("as_of", "snapshot_date", "entry", "exit")
KEYS = (*DATES, "ticker")
LABELS = ("forward_return", "grade")
COLUMNS = (*KEYS, *features.FEATURES, *LABELS)
_META_HASH = b"agb.partition_hash"
_META_COST = b"agb.cost_model_version"


@dataclass(frozen=True)
class Month:
    """One rebalance month: its features' date and the label window that follows it."""

    as_of: date
    snapshot_date: date
    entry: date
    exit: date


@dataclass(frozen=True)
class Panel:
    frame: pd.DataFrame
    partition_hash: str
    cost_model_version: str = cost.CURRENT.version

    @property
    def months(self) -> list[Month]:
        """The panel's months, oldest first."""
        keys = self.frame[list(DATES)].drop_duplicates().sort_values("as_of")
        return [Month(*row) for row in keys.itertuples(index=False, name=None)]

    def select(self, months: Sequence[Month]) -> pd.DataFrame:
        """The rows of the given months, in month then ticker order."""
        wanted = {month.as_of for month in months}
        rows = self.frame[self.frame["as_of"].isin(wanted)]
        return rows.sort_values(["as_of", "ticker"], kind="stable").reset_index(drop=True)

    def save(self, path: Path) -> None:
        table = pa.Table.from_pandas(self.frame, preserve_index=False)
        metadata = {
            **(table.schema.metadata or {}),
            _META_HASH: self.partition_hash.encode(),
            _META_COST: self.cost_model_version.encode(),
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table.replace_schema_metadata(metadata), path)

    @classmethod
    def load(cls, path: Path) -> "Panel":
        table = pq.read_table(path)
        metadata = table.schema.metadata or {}
        frame = table.to_pandas()
        frame["sector"] = pd.Categorical(frame["sector"], categories=features.SECTORS)
        return cls(
            frame=frame,
            partition_hash=metadata[_META_HASH].decode(),
            cost_model_version=metadata[_META_COST].decode(),
        )


def build(
    lake_dir: Path = lake.DEFAULT_DIR,
    start: date = lake.SINCE,
    end: date | None = None,
    *,
    db: duckdb.DuckDBPyConnection | None = None,
) -> Panel:
    """The panel over every month-end from `start` to `end` whose label the lake can already
    settle. Months whose exit session has not landed yet are left out."""
    conn = db if db is not None else lake.connect(lake_dir)
    frames: list[pd.DataFrame] = []
    last_exit: date | None = None
    try:
        for month_features in features.history(start, end, lake_dir, db=conn):
            window = features.forward_window(month_features.snapshot_date, db=conn)
            if window is None:
                break
            entry, exit_ = window
            returns = features.forward_returns(month_features, lake_dir, db=conn) or {}
            month = Month(month_features.as_of, month_features.snapshot_date, entry, exit_)
            frames.append(_rows(month, month_features, returns))
            last_exit = exit_
        partition_hash = lake.partition_digest(lake_dir, through=last_exit, hashes={})
    finally:
        if db is None:
            conn.close()
    frame = pd.concat(frames, ignore_index=True) if frames else _empty()
    return Panel(frame=frame, partition_hash=partition_hash)


def _rows(month: Month, month_features: features.Features, returns: dict[str, float]):
    ranked = month_features.ranked
    tickers = ranked.column("ticker").to_pylist()
    forward = [returns.get(ticker) for ticker in tickers]
    grade = labels.grades(forward)
    frame = pd.DataFrame(
        {
            "as_of": [month.as_of] * len(tickers),
            "snapshot_date": [month.snapshot_date] * len(tickers),
            "entry": [month.entry] * len(tickers),
            "exit": [month.exit] * len(tickers),
            "ticker": tickers,
            "sector": pd.Categorical(
                ranked.column("sector").to_pylist(), categories=features.SECTORS
            ),
            **{
                name: pd.array(ranked.column(name).to_pylist(), dtype="float64")
                for name in features.NUMERIC
            },
            "forward_return": pd.array(forward, dtype="float64"),
            "grade": pd.array(grade, dtype="float64"),
        }
    )
    # A name without a settled return has no grade and nothing to teach.
    frame = frame[frame["grade"].notna()].reset_index(drop=True)
    frame["grade"] = frame["grade"].astype("int64")
    return frame.astype({"ticker": "str"})[list(COLUMNS)]


def _empty() -> pd.DataFrame:
    frame = pd.DataFrame({column: [] for column in COLUMNS})
    frame["sector"] = pd.Categorical([], categories=features.SECTORS)
    return frame
