"""The committed disclosure fixtures: well formed before the recall check relies on them."""

import dataclasses
from datetime import date

import pytest

from agb.disclosures import FIXTURES, Disclosure


def test_there_are_fixtures_and_each_is_a_disclosure():
    assert FIXTURES
    assert all(isinstance(entry, Disclosure) for entry in FIXTURES)


def test_each_company_is_named_once():
    symbols = [entry.symbol for entry in FIXTURES]
    assert len(set(symbols)) == len(symbols)


@pytest.mark.parametrize("entry", FIXTURES, ids=lambda entry: entry.symbol)
def test_an_entry_is_complete_and_read_as_of_a_past_date(entry):
    assert entry.symbol == entry.symbol.upper() and entry.symbol.isalpha()
    assert entry.phrase.strip() and entry.phrase == entry.phrase.strip()
    assert entry.source.strip()
    assert 1 <= entry.dimension <= 6
    assert date(2015, 1, 1) <= entry.as_of < date.today()


def test_the_dimensions_cover_more_than_one_kind_of_disclosure():
    assert len({entry.dimension for entry in FIXTURES}) > 1


def test_an_entry_cannot_be_changed():
    with pytest.raises(dataclasses.FrozenInstanceError):
        FIXTURES[0].phrase = "something else"  # type: ignore[misc]
