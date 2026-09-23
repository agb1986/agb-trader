"""The deterministic scrubber that blinds the panel's later roles to the company's identity.

The Analyst reads the named company's filings; its extract then passes through here before
the Prosecutor, Defender and Judge see it (docs/architecture/03, "Blinding"). The company's
name, its tickers and the aliases on its filings' cover pages are replaced with "the Company",
which is how filings already refer to themselves. It is plain string substitution: no model,
no randomness, so the same extract and aliases always scrub to the same text.

The rules, all part of SCRUBBER_VERSION:

- Names match case-insensitively on word boundaries. Each name also yields its short form,
  with EDGAR's state tag ("/DE/") and corporate suffixes ("Inc.", "Corp", ...) removed.
- Tickers match case-sensitively, since "ON" or "ALL" in running text is usually a word. A
  one-letter ticker is replaced only in parentheses or quotes, as in "(NYSE: A)".
- Longer aliases win over shorter ones that they contain. A leading "the" is absorbed, and so
  are corporate suffixes after a name, so "the Acme Corporation" becomes "the Company" rather
  than "the the Company Corporation". A suffix's abbreviating period goes with it unless it
  also ends the sentence.
- "The Company" is capitalised at the start of the text and of a sentence.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

SCRUBBER_VERSION = "1"
REPLACEMENT = "the Company"

_STATE_TAG = re.compile(r"\s*/[A-Za-z]{2,}/\s*$")
_SUFFIXES = re.compile(
    r"(?:[\s,]+(?:inc|incorporated|corp|corporation|co|company|ltd|limited|llc|l\.l\.c|lp|l\.p|"
    r"plc|n\.v|nv|s\.a|sa|ag|se|holdings?|group)\.?)+$",
    re.IGNORECASE,
)
# The suffixes absorbed after a name in running text. Short or ambiguous ones ("co", "sa")
# are left out: after a name they are more often a word than a suffix.
_TRAILING = (
    r"(?:,?\s+(?:inc|incorporated|corp|corporation|company|ltd|limited|llc|plc|holdings?|group)\b)*"
)
# The period of an abbreviated suffix, when it does not also end the sentence: "Acme, Inc. has"
# becomes "the Company has", but "made by Acme, Inc. The" keeps its full stop.
_ABBREVIATION_DOT = r"(?:(?<=inc|orp|ltd|llc|plc)\.(?=(?-i:\s+[a-z])|[,;:)]))?"
_SENTENCE_END = re.compile(r"(?:[.!?]\s+|\n\s*)$")
# A name alias shorter than this is not scrubbed as a word: it would match ordinary text.
MIN_NAME = 3


def name_forms(name: str) -> set[str]:
    """A registrant name and its short form: 'ACME CORP /DE/' gives 'ACME CORP' and 'ACME'."""
    full = _STATE_TAG.sub("", name).strip(" ,.")
    forms = {full} if full else set()
    short = _SUFFIXES.sub("", full).strip(" ,.")
    if short:
        forms.add(short)
    return {form for form in forms if len(form) >= MIN_NAME}


class Scrubber:
    def __init__(self, names: Iterable[str], tickers: Iterable[str]) -> None:
        self.names = sorted({form for name in names for form in name_forms(name)})
        self.tickers = sorted({t.strip() for t in tickers if t.strip()})
        patterns = [(form, _name_pattern(form)) for form in self.names]
        patterns += [(ticker, _ticker_pattern(ticker)) for ticker in self.tickers]
        # Longest alias first, then alphabetical: the alternation tries them in this order.
        patterns.sort(key=lambda item: (-len(item[0]), item[0]))
        self._pattern = (
            re.compile(r"(?:\bthe\s+)?(?:" + "|".join(p for _, p in patterns) + ")", re.IGNORECASE)
            if patterns
            else None
        )

    @property
    def aliases(self) -> list[str]:
        return [*self.names, *self.tickers]

    def scrub(self, text: str) -> str:
        if self._pattern is None:
            return text

        leading = len(text) - len(text.lstrip())

        def replace(match: re.Match[str]) -> str:
            start = match.start()
            tail = text[max(0, start - 16) : start]
            opens = start <= leading or _SENTENCE_END.search(tail)
            return "The Company" if opens else REPLACEMENT

        return self._pattern.sub(replace, text)


def _name_pattern(name: str) -> str:
    words = [re.escape(word) for word in name.split()]
    body = r"\s+".join(words)
    start = r"\b" if name[0].isalnum() else ""
    end = r"\b" if name[-1].isalnum() else ""
    return f"{start}{body}{end}{_TRAILING}{_ABBREVIATION_DOT}"


def _ticker_pattern(ticker: str) -> str:
    # The whole pattern is case-insensitive, so a ticker switches it off for itself.
    escaped = re.escape(ticker)
    if len(ticker) == 1:
        return f'(?-i:(?:(?<=\\()|(?<=: )){escaped}(?=\\))|(?<=["“]){escaped}(?=["”]))'
    return f"(?-i:(?<![A-Za-z0-9.]){escaped}(?![A-Za-z0-9]))"
