"""The NYSE calendar: holidays, early closes and daylight saving, pinned to known dates."""

from datetime import UTC, date, datetime

import pytest

from agb.nyse import NEW_YORK, NyseCalendar, trading_date


@pytest.fixture(scope="module")
def cal():
    return NyseCalendar(date(2023, 1, 1), date(2027, 12, 31))


@pytest.mark.parametrize(
    "day",
    [
        date(2026, 4, 3),  # Good Friday
        date(2026, 7, 3),  # Independence Day, observed on the Friday
        date(2026, 11, 26),  # Thanksgiving
        date(2026, 12, 25),  # Christmas
        date(2027, 1, 1),  # New Year's Day
        date(2027, 5, 31),  # Memorial Day
        date(2026, 10, 3),  # a Saturday
    ],
)
def test_holidays_and_weekends_are_not_sessions(cal, day):
    assert not cal.is_session(day)


@pytest.mark.parametrize(
    "session, close_et",
    [
        (date(2026, 11, 27), "13:00"),  # the day after Thanksgiving
        (date(2026, 12, 24), "13:00"),  # Christmas Eve
        (date(2023, 7, 3), "13:00"),  # the day before Independence Day
        (date(2026, 11, 25), "16:00"),
        (date(2026, 12, 23), "16:00"),
    ],
)
def test_close_times_include_early_closes(cal, session, close_et):
    assert cal.close_at(session).astimezone(NEW_YORK).strftime("%H:%M") == close_et
    assert cal.is_early_close(session) == (close_et != "16:00")


def test_times_are_utc_and_follow_daylight_saving(cal):
    # 2026-03-08 and 2026-11-01 are the Sundays the clocks change in New York.
    assert cal.open_at(date(2026, 3, 6)) == datetime(2026, 3, 6, 14, 30, tzinfo=UTC)
    assert cal.open_at(date(2026, 3, 9)) == datetime(2026, 3, 9, 13, 30, tzinfo=UTC)
    assert cal.close_at(date(2026, 10, 30)) == datetime(2026, 10, 30, 20, 0, tzinfo=UTC)
    assert cal.close_at(date(2026, 11, 2)) == datetime(2026, 11, 2, 21, 0, tzinfo=UTC)
    assert cal.open_at(date(2026, 3, 9)).tzinfo is UTC


def test_moc_cutoff_is_15_45_or_earlier_on_an_early_close(cal):
    assert cal.moc_cutoff(date(2026, 10, 1)).astimezone(NEW_YORK).strftime("%H:%M") == "15:45"
    assert cal.moc_cutoff(date(2023, 7, 3)) == datetime(2023, 7, 3, 16, 45, tzinfo=UTC)


@pytest.mark.parametrize(
    "day, following",
    [
        (date(2026, 11, 25), date(2026, 11, 27)),  # over Thanksgiving
        (date(2026, 12, 24), date(2026, 12, 28)),  # over Christmas and a weekend
        (date(2026, 12, 25), date(2026, 12, 28)),  # from a holiday
        (date(2026, 4, 2), date(2026, 4, 6)),  # over Good Friday
    ],
)
def test_next_session_skips_closures(cal, day, following):
    assert cal.next_session(day) == following


@pytest.mark.parametrize(
    "year, month, last",
    [
        (2026, 9, date(2026, 9, 30)),
        (2026, 10, date(2026, 10, 30)),  # the 31st is a Saturday
        (2027, 5, date(2027, 5, 28)),  # the 31st is Memorial Day
        (2025, 11, date(2025, 11, 28)),  # an early close
        (2026, 12, date(2026, 12, 31)),
    ],
)
def test_last_session_of_month(cal, year, month, last):
    assert cal.last_session_of_month(year, month) == last


def test_sessions_skip_holidays(cal):
    assert cal.sessions(date(2026, 11, 24), date(2026, 11, 30)) == [
        date(2026, 11, 24),
        date(2026, 11, 25),
        date(2026, 11, 27),
        date(2026, 11, 30),
    ]


def test_trading_date_is_the_new_york_date():
    # 02:00 UTC on the 2nd is still the evening of the 1st in New York.
    assert trading_date(datetime(2026, 10, 2, 2, 0, tzinfo=UTC)) == date(2026, 10, 1)
    assert trading_date(datetime(2026, 10, 2, 4, 30, tzinfo=UTC)) == date(2026, 10, 2)
