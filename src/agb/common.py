"""Small helpers the operator CLI, the health check and the bot all use.

They live here, below all three, so that none of them has to import another's entry point for
a formatting function or a setting. It imports nothing but the standard library and `roles`."""

import os
from datetime import timedelta

from agb.roles import SERVICES


def format_age(age: timedelta) -> str:
    seconds = max(0, int(age.total_seconds()))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def expected_services() -> tuple[str, ...]:
    """The services `agb status` insists on: AGB_EXPECTED_SERVICES, separated by spaces or
    commas, or every service when it is unset or empty. The workstation's `make status` leaves
    the bot out unless the telegram profile runs there; the server expects all of them."""
    names = os.environ.get("AGB_EXPECTED_SERVICES", "").replace(",", " ").split()
    return tuple(names) or SERVICES
