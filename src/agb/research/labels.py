"""The labels: each month's forward returns graded into quintiles.

A month is one query group. Within it every name's forward one-month return is ranked against
the others', with the shared percentile rank, and the rank is cut into five grades, 0 for the
worst fifth to 4 for the best. The ranker learns the order of names within a month and never
the level of returns across months, so a bull year and a bear year contribute the same."""

import math
from collections.abc import Sequence

from agb import features

GRADES = 5


def grades(returns: Sequence[float | None]) -> list[int | None]:
    """Each return's quintile grade among the others, 0 to GRADES - 1, in the returns' order.
    A missing return gets no grade."""
    ranks = features.percentile_rank(returns)
    # The ranks sit on [0, 1]; a rank at a fifth's exact boundary rounds into the upper fifth,
    # and the small nudge keeps floating point from dropping one down.
    return [
        None if rank is None else min(math.floor(rank * GRADES + 1e-9), GRADES - 1)
        for rank in ranks
    ]
