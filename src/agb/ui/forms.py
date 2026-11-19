"""The forms and the write paths: the confirmation pages, the six writes and the outcome page
they redirect to, and the tables of routes the server serves."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

import psycopg

from agb import approval, killswitch
from agb.alerts import acknowledge
from agb.health import et
from agb.ui.shell import (
    ACTOR,
    CONFIRM_LIQUIDATE,
    CONFIRM_PAUSE,
    CONFIRM_RESUME,
    SURFACE,
    Html,
    banner,
    calendar,
    esc,
    join,
    note,
    page,
    section,
    table,
    tag,
)
from agb.ui.views import (
    alerts,
    gate,
    kill_switch,
    log,
    panel,
    performance,
    positions,
    promotion,
    rebalance,
    validation,
)

# What a ticked cause is called in the form the resume confirmation posts: `cause.<name>`.
CAUSE_FIELD = "cause."


def confirm_form(action: str, button: str, ticks: Sequence[str] = ()) -> Html:
    """The confirmed write as a plain form posting to one of `WRITES`, with a way back. The ticks
    are unticked and carry no value: nothing here is chosen for the operator, and an untouched
    form clears nothing."""
    body: list[object] = [
        Html(
            f'<label><input type="checkbox" name="{esc(CAUSE_FIELD + cause)}"> {esc(cause)}</label>'
        )
        for cause in ticks
    ]
    body.append(tag("button", button, type="submit"))
    body.append(Html(' <a href="/killswitch">Cancel</a>'))
    return Html(f'<form class="write" method="post" action="{esc(action)}">{join(body)}</form>')


def confirm_pause(conn: psycopg.Connection, now: datetime) -> str:
    already = next(
        ((at, actor) for cause, at, actor, _ in killswitch.causes(conn) if cause == "pause"), None
    )
    blocks: list[object] = [
        note("Nothing has been written yet. Confirming does these things:"),
        Html(
            "<ul><li>opens the <b>pause</b> halt cause, so no further order goes out, whatever "
            "else is open;</li><li>asks execution to set the broker's suspend_trade, and to "
            "cancel the sent orders of a basket still being submitted, which halts that "
            "basket;</li><li>records the command against this surface.</li></ul>"
        ),
        note(
            f"The pause cause is open already, since {et(already[0])} by {already[1]}: "
            "confirming records the command and changes nothing else."
            if already
            else "The pause cause is not open."
        ),
        confirm_form("/pause", "Confirm pause"),
    ]
    return page("Confirm pause", "/killswitch", [banner(conn, now), section("Pause?", blocks)])


def confirm_resume(conn: psycopg.Connection, now: datetime) -> str:
    latched = killswitch.causes(conn)
    if latched:
        blocks: list[object] = [
            note(
                "Nothing has been written yet. Tick each cause to clear; only those are "
                "cleared, one clear each, and an unticked cause stays open and keeps trading "
                "halted. Nothing is ticked for you, and ticking none clears nothing."
            ),
            table(
                ["Open cause", "Since", "By", "Reason"],
                [(cause, et(at), actor, reason or "") for cause, at, actor, reason in latched],
                caption="Open halt causes",
            ),
            note(
                "The broker's suspend_trade is asked off once the pause cause is cleared, and "
                "stays on while it is open. A halted rebalance is not resubmitted."
            ),
            confirm_form("/resume", "Confirm resume", [cause for cause, *_ in latched]),
        ]
    else:
        blocks = [
            note("No halt cause is open, so there is nothing to clear."),
            tag("p", tag("a", "Back", href="/killswitch")),
        ]
    return page("Confirm resume", "/killswitch", [banner(conn, now), section("Resume?", blocks)])


def confirm_liquidate(conn: psycopg.Connection, now: datetime) -> str:
    minutes = int(killswitch.TOKEN_VALID.total_seconds() // 60)
    expires = killswitch.outstanding(conn, now)
    blocks: list[object] = [
        note("Nothing has been written yet. Confirming is step one of two, and does only this:"),
        Html(
            f"<ul><li>issues a token good for {minutes} minutes, and records the request;</li>"
            "<li>answers with what closing every position would mean: the positions held and "
            "their value, and the account's equity.</li></ul>"
        ),
        note(
            "It closes nothing and opens no halt cause. Step two is that token typed back on "
            "the kill switch page, which opens the liquidation cause and has execution cancel "
            "open orders and close every position at market."
        ),
        note(
            f"A token is outstanding until {et(expires)}; a new one supersedes it."
            if expires
            else "No token is outstanding."
        ),
        confirm_form("/liquidate", "Confirm: issue a token"),
    ]
    return page(
        "Confirm liquidate",
        "/killswitch",
        [banner(conn, now), section("Ask to liquidate?", blocks)],
    )


# The write paths. Each takes the fields the form posted and returns the `Outcome`: the reply
# the shared module produced, which is the same text the bot sends to Telegram. The server does
# not render it in answer to the POST: it holds it and redirects to `OUTCOME`, a GET, so a
# reload re-reads the outcome and never posts the write again.

# Where the server redirects a write to, with the held outcome's token in the query.
OUTCOME = "/outcome"


@dataclass(frozen=True)
class Outcome:
    """What a write did: the page it belongs to, the heading and the shared module's reply."""

    path: str
    heading: str
    reply: str


def _source(text: str) -> approval.Source:
    """Who the write is attributed to and what they typed, as the shared modules take it. A row
    written from here differs from the bot's only in the actor and the surface."""
    return approval.Source(ACTOR, SURFACE, text)


def _answer(
    conn: psycopg.Connection, verb: str, fields: Mapping[str, str], now: datetime
) -> Outcome:
    """Approve or reject the rebalance whose reference was typed. `agb.approval` checks the
    reference, refuses a wrong, missing or late one and records that refusal, and settles a race
    with the bot on the basket's own sequence, so the loser is told who answered and when.

    The reference is read from its own field and from nowhere else. Whatever the reason box
    holds is only a reason, so a reference typed there leaves the reference empty and is
    refused as missing. A reference box that holds more than one word is not a reference either:
    it is looked up whole, found missing, refused and recorded."""
    reference = fields.get("reference", "").strip()
    words = [reference, *fields.get("reason", "").split()] if reference else []
    source = _source(" ".join([f"/{verb}", *words]))
    reply = approval.decide(conn, verb, words, source, now, calendar(now))
    return Outcome("/rebalance", "Your answer", reply)


def approve(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> Outcome:
    return _answer(conn, "approve", fields, now)


def reject(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> Outcome:
    return _answer(conn, "reject", fields, now)


def pause(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> Outcome:
    return Outcome("/killswitch", "Kill switch", killswitch.pause(conn, _source("/pause")))


def resume(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> Outcome:
    """Clear the causes the confirmation page ticked, and no others. A form that ticks none is
    refused and recorded, and a cause that opened after the page was drawn is not on it, so it
    stays open."""
    ticked = sorted(name[len(CAUSE_FIELD) :] for name in fields if name.startswith(CAUSE_FIELD))
    source = _source(" ".join(["/resume", *ticked]))
    return Outcome("/killswitch", "Kill switch", killswitch.resume(conn, source, ticked))


def liquidate(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> Outcome:
    words = fields.get("token", "").split()
    source = _source(" ".join(["/liquidate", *words]))
    reply = killswitch.liquidate(conn, source, words[0] if words else None, now)
    return Outcome("/killswitch", "Kill switch", reply)


def ack(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> Outcome:
    """Acknowledge the alert whose id was typed. `agb.alerts` checks it, refuses and records a
    missing, malformed or repeated one, and settles a race with the bot on the alert itself."""
    word = fields.get("alert", "").strip()
    reply = acknowledge(conn, _source(f"/ack {word}".strip()), word or None, now)
    return Outcome("/alerts", "Acknowledge alert", reply)


def outcome(conn: psycopg.Connection, now: datetime, held: Outcome | None) -> str:
    """The page a write redirects to: the reply it produced, above the banner as it reads now.
    An outcome the server no longer holds, because it expired or the server restarted, says so
    and points at the log, where every write the operator made is a row."""
    if held is None:
        return page(
            "No outcome",
            "/",
            [
                banner(conn, now),
                section(
                    "No outcome",
                    [
                        note(
                            "That outcome is no longer held here: it is kept for a few minutes "
                            "after a write. What the write did is a row in the log."
                        ),
                        tag("p", tag("a", "Log", href="/")),
                    ],
                ),
            ],
        )
    blocks = [
        Html(f'<pre class="reply">{esc(held.reply)}</pre>'),
        tag("p", tag("a", "Back", href=held.path)),
    ]
    return page(held.heading, held.path, [banner(conn, now), section(held.heading, blocks)])


def failed(path: str, cause: str, wrote: bool) -> str:
    """The page for a request that raised something that was not the database's: the route and
    the cause by name, and no traceback, which goes to the log. A write may have been recorded
    before it raised, so its page says to look in the log and not to retry."""
    heading = "The write failed" if wrote else "The page failed"
    blocks = [
        note(f"{path} raised an error."),
        Html(f'<pre class="reply">{esc(cause)}</pre>'),
    ]
    if wrote:
        blocks.append(note("It may have been recorded before it failed: check the log first."))
    blocks.append(tag("p", tag("a", "Log", href="/")))
    return page(heading, path, [section(heading, blocks)])


# What the server routes: the pages, which only read, and the writes, which are these six and
# no others. A page's form posts to one of these and nowhere else, and the server answers a
# POST to a page with 405.
ROUTES = {
    "/": log,
    "/rebalance": rebalance,
    "/positions": positions,
    "/gate": gate,
    "/validation": validation,
    "/promotion": promotion,
    "/performance": performance,
    "/panel": panel,
    "/alerts": alerts,
    "/killswitch": kill_switch,
    CONFIRM_PAUSE: confirm_pause,
    CONFIRM_RESUME: confirm_resume,
    CONFIRM_LIQUIDATE: confirm_liquidate,
}
WRITES = {
    "/approve": approve,
    "/reject": reject,
    "/pause": pause,
    "/resume": resume,
    "/liquidate": liquidate,
    "/ack": ack,
}


def not_found(path: str) -> str:
    blocks = [note(f"There is no {path} here."), tag("p", tag("a", "Log", href="/"))]
    return page("Not found", path, [section("Not found", blocks)])


def not_allowed(path: str) -> str:
    """A POST to a page. The pages read; the writes are the routes in `WRITES`."""
    blocks = [
        note(f"{path} is a page, and pages are read with GET."),
        tag("p", tag("a", "Log", href="/")),
    ]
    return page("Not allowed", path, [section("Not allowed", blocks)])
