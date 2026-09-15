"""The NYSE trading calendar, in America/New_York.

A thin wrapper over exchange_calendars' XNYS calendar, which knows the exchange's holidays,
early closes and ad hoc closures. It hands back plain dates and UTC datetimes, so nothing
else in the codebase needs pandas to ask when the market is open.
"""

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as xc
import pandas as pd

NEW_YORK = ZoneInfo("America/New_York")

# Market-on-close orders go in this long before the close: 15:45 ET on a full day, five
# minutes of pad on Alpaca's cutoff about ten minutes before the close. On an early close
# the cutoff moves with the close.
MOC_SUBMISSION_LEAD = timedelta(minutes=15)


def trading_date(at: datetime) -> date:
    """The New York calendar date at an instant."""
    return at.astimezone(NEW_YORK).date()


def _utc(ts: pd.Timestamp) -> datetime:
    return ts.to_pydatetime().astimezone(UTC)


class NyseCalendar:
    """NYSE sessions between `start` and `end`, inclusive. Asking outside that range raises."""

    def __init__(self, start: date, end: date) -> None:
        self.start, self.end = start, end
        self._cal = xc.get_calendar("XNYS", start=start.isoformat(), end=end.isoformat())
        self._early = {ts.date() for ts in self._cal.early_closes}

    def covers(self, start: date, end: date) -> bool:
        return self.start <= start and end <= self.end

    def sessions(self, start: date, end: date) -> list[date]:
        return [ts.date() for ts in self._cal.sessions_in_range(start.isoformat(), end.isoformat())]

    def is_session(self, day: date) -> bool:
        return bool(self._cal.is_session(day.isoformat()))

    def is_early_close(self, session: date) -> bool:
        return session in self._early

    def open_at(self, session: date) -> datetime:
        return _utc(self._cal.session_open(session.isoformat()))

    def close_at(self, session: date) -> datetime:
        return _utc(self._cal.session_close(session.isoformat()))

    def moc_cutoff(self, session: date) -> datetime:
        """The latest moment to submit a market-on-close order for this session's close."""
        return self.close_at(session) - MOC_SUBMISSION_LEAD

    def next_session(self, day: date) -> date:
        """The first session strictly after `day`, which need not be a session itself."""
        return self._cal.date_to_session((day + timedelta(days=1)).isoformat(), "next").date()

    def last_session_of_month(self, year: int, month: int) -> date:
        first_of_next = date(year + month // 12, month % 12 + 1, 1)
        last_day = first_of_next - timedelta(days=1)
        return self._cal.date_to_session(last_day.isoformat(), "previous").date()
