import re

from agb.scrubber import Scrubber, name_forms

EXTRACT = """Acme Widgets Corp (NASDAQ: ACME) depends on one customer. ACME WIDGETS CORP's lender
waived the covenant. acme widgets corp restated 2024. The Acme Widgets Corporation was
formerly Acme Holdings Inc. Its warrants trade as ACMEW. Acme said the ACME ticker may move.
Pinnacle Acmeist Corp and ACMES are other companies."""


def scrubber():
    return Scrubber(["ACME WIDGETS CORP /DE/", "Acme Holdings Inc"], ["ACME", "ACMEW"])


def test_name_forms_drop_the_state_tag_and_suffixes():
    assert name_forms("ACME WIDGETS CORP /DE/") == {"ACME WIDGETS CORP", "ACME WIDGETS"}
    assert name_forms("NewCo Robotics, Inc.") == {"NewCo Robotics, Inc", "NewCo Robotics"}
    assert name_forms("3M CO") == {"3M CO"}
    assert name_forms("") == set()


def test_the_name_and_ticker_are_absent_from_the_scrubbed_extract():
    out = scrubber().scrub(EXTRACT)

    for alias in ("Acme Widgets", "Acme Holdings", "ACMEW"):
        assert alias.lower() not in out.lower()
    assert not re.search(r"\bACME\b", out)
    assert out.startswith("The Company (NASDAQ: the Company) depends on one customer.")
    assert "The Company's lender" in out
    assert "The Company restated 2024" in out
    assert "The Company was\nformerly the Company." in out
    assert "trade as the Company." in out
    # A word containing an alias, and other companies, are left alone.
    assert "Pinnacle Acmeist Corp and ACMES" in out
    # The former name's short form is an alias too.
    assert "The Company said the Company ticker may move." in out
    assert "Corporation" not in out


def test_the_scrubber_is_deterministic():
    runs = {scrubber().scrub(EXTRACT) for _ in range(5)}
    runs.add(
        Scrubber(["Acme Holdings Inc", "ACME WIDGETS CORP /DE/"], ["ACMEW", "ACME"]).scrub(EXTRACT)
    )

    assert len(runs) == 1


def test_tickers_match_case_sensitively_and_one_letter_tickers_only_when_marked():
    s = Scrubber(["Agilent Technologies Inc"], ["A", "ON"])

    out = s.scrub('Agilent Technologies (NYSE: A) is "A". A plan was ON track; carry on.')

    assert out == (
        'The Company (NYSE: the Company) is "the Company". A plan was the Company track; carry on.'
    )


def test_no_aliases_leaves_the_text_alone():
    assert Scrubber([], []).scrub("Acme (ACME)") == "Acme (ACME)"


def test_a_suffix_period_goes_with_the_name_unless_it_ends_the_sentence():
    scrubber = Scrubber(["NewCo Robotics, Inc."], ["NEWC"])

    assert scrubber.scrub("NewCo Robotics, Inc. has a deficit.") == "The Company has a deficit."
    assert (
        scrubber.scrub("Made by NewCo Robotics, Inc. The next") == "Made by the Company. The next"
    )
    assert scrubber.scrub("NewCo Robotics, Inc., a Delaware corporation") == (
        "The Company, a Delaware corporation"
    )
