"""`agb.rows.one` reads the one row a statement is certain to return."""

import pytest

from agb.rows import one


class Cursor:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


def test_one_returns_the_row():
    assert one(Cursor((7,))) == (7,)


def test_one_raises_when_no_row_came_back():
    with pytest.raises(LookupError):
        one(Cursor(None))
