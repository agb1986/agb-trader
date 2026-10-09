"""The committed fixture set the panel's known-disclosure recall check runs against.

Each entry is one impairment disclosure a curator has read in a real filing: the ticker, the
date the panel must read the company as of, the rubric dimension the disclosure belongs to,
and a phrase from the filing's own words. Recall asks whether the Analyst found it: it reads
the company as of that date and looks for a verified quote carrying the phrase in that
dimension (docs/architecture/03, "Pre-flight validation").

An entry is a claim about the record, so the suite checks it before it counts. If the phrase is
not in the text the Analyst was given -- because the disclosure is elsewhere in the filing,
because EDGAR served nothing readable, or because the entry is simply wrong -- the entry is
reported unusable and is left out of the measurement rather than counted as a miss. Run `agb
panel-preflight fixtures` to see each entry's standing, and fix or drop the unusable ones.

The panel reads Item 1A and Item 7 of the latest annual filing, the latest quarterly filing in
full, and any current report filed since, so a phrase must be somewhere the Analyst can see
it: a disclosure that lives only in the notes to the financial statements or in Item 9A is not
a fixture, however real it is.
"""

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Disclosure:
    """One known disclosure: `phrase` is matched against the filing text and the Analyst's
    quotes under the whitespace-and-typography normalisation every quote is verified with, so
    it need not carry the surrounding sentence. `source` is where the curator saw it."""

    symbol: str
    as_of: date
    dimension: int
    phrase: str
    source: str


FIXTURES: tuple[Disclosure, ...] = (
    Disclosure(
        symbol="PLUG",
        as_of=date(2024, 6, 30),
        dimension=1,
        phrase="going concern",
        source="Plug Power FY2023 10-K (filed March 2024): substantial doubt about the "
        "company's ability to continue as a going concern, carried in the risk factors and "
        "the liquidity discussion",
    ),
    Disclosure(
        symbol="SMCI",
        as_of=date(2025, 3, 31),
        dimension=2,
        phrase="material weakness",
        source="Super Micro Computer FY2024 10-K (filed February 2025, after the delayed "
        "filing): material weaknesses in internal control over financial reporting, carried "
        "in the risk factors",
    ),
    Disclosure(
        symbol="MMM",
        as_of=date(2024, 6, 30),
        dimension=3,
        phrase="PFAS",
        source="3M FY2023 10-K (filed February 2024): the PFAS and Combat Arms litigation, "
        "carried in the risk factors and the legal-proceedings discussion",
    ),
)
