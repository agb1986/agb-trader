"""Who a role may say it is, and the ladder's upward checks, in the database (issue #189).

Each refusal is put to the database as the role that would make it, with the Python that
normally stands in front of the write left out, so what is tested is the database alone.
tests/test_approval_loop.py and tests/test_ladder.py cover the same moves made through the
code that is meant to make them.
"""

from dataclasses import replace

import pytest
import registry_fake
from psycopg import errors
from test_gate import BASE, SPEAKS_FOR, go_live, insert, move, state
from test_ladder import a_command, at_level, clean, earned_live

LADDER = (
    "INSERT INTO agb.ladder_event (level, actor, reason, attestation, command_id)"
    " VALUES (%s, %s, 'by hand', %s, %s)"
)
MODE = (
    "INSERT INTO agb.mode_event (mode, actor, reason, attestation, command_id)"
    " VALUES (%s, %s, 'by hand', %s, %s)"
)


def pending(db):
    """A basket put to the human, as the bot puts it."""
    basket_id = insert(db, replace(BASE, orders=()))
    move(db, basket_id, 2, "proposed", "pending", "bot", "telegram", actor="bot")
    return basket_id


# -- the human's surfaces speak for their operator ------------------------------------------


@pytest.mark.parametrize("to_state", ["approved", "rejected"])
@pytest.mark.parametrize(
    ("role", "actor", "surface"),
    [
        ("ui", "system", "ui"),
        ("ui", "telegram:7", "ui"),
        ("ui", "ui:operator", "telegram"),
        ("ui", "ada", "ui"),
        ("bot", "system", "telegram"),
        ("bot", "ui:operator", "telegram"),
        ("bot", "telegram:7", "ui"),
        ("bot", "bot", "telegram"),
        ("bot", "telegram:unknown", "telegram"),
    ],
)
def test_a_surface_may_not_decide_a_basket_as_anyone_but_its_operator(
    db, role, actor, surface, to_state
):
    basket_id = pending(db)

    with pytest.raises(errors.InsufficientPrivilege, match=f"basket_actor: {role} may not"):
        move(db, basket_id, 3, "pending", to_state, role, surface, actor=actor)
    assert state(db, basket_id)[:2] == ("pending", 2)


@pytest.mark.parametrize("to_state", ["approved", "rejected"])
@pytest.mark.parametrize("role", ["bot", "ui"])
def test_a_surface_decides_a_basket_as_its_operator(db, role, to_state):
    basket_id = pending(db)
    surface = {"bot": "telegram", "ui": "ui"}[role]

    move(db, basket_id, 3, "pending", to_state, role, surface, actor=SPEAKS_FOR[role])

    assert state(db, basket_id) == (to_state, 3, SPEAKS_FOR[role], surface)


@pytest.mark.parametrize("role", ["bot", "ui"])
def test_at_l1_in_live_a_surface_still_cannot_approve_as_the_system(db, role):
    """Finding S-01: at L1 in live a system approval of a pending basket is the scheduler's
    unattended one, so a surface that could write `system` could approve a live basket as if
    no human had been asked."""
    go_live(db)
    at_level(db, "L1")
    basket_id = pending(db)
    surface = {"bot": "telegram", "ui": "ui"}[role]

    with pytest.raises(errors.InsufficientPrivilege, match=f"basket_actor: {role} .* as system"):
        move(db, basket_id, 3, "pending", "approved", role, surface, actor="system")
    assert state(db, basket_id)[:2] == ("pending", 2)


def test_the_bot_puts_a_basket_to_the_human_as_itself_and_from_nowhere_else(db):
    basket_id = insert(db, replace(BASE, orders=()))

    with pytest.raises(errors.InsufficientPrivilege, match="the bot writes from telegram"):
        move(db, basket_id, 2, "proposed", "pending", "bot", "ui", actor="bot")
    move(db, basket_id, 2, "proposed", "pending", "bot", "telegram", actor="bot")

    assert state(db, basket_id) == ("pending", 2, "bot", "telegram")


# -- the ladder, climbed in the database -------------------------------------------------------


def test_the_database_refuses_a_level_that_skips_one(db):
    command_id = a_command(db, "level")

    with pytest.raises(
        errors.InsufficientPrivilege, match="one level at a time; L1 comes after L0"
    ):
        db.connect("cli").execute(LADDER, ("L2", "ada", "straight up", command_id))


def test_the_database_refuses_a_level_that_is_not_earned(db):
    command_id = a_command(db, "level")

    with pytest.raises(
        errors.InsufficientPrivilege, match="L1 is not earned yet: clean_rebalances outstanding"
    ):
        db.connect("cli").execute(LADDER, ("L1", "ada", "six months", command_id))


def test_the_database_lets_an_earned_level_be_climbed(db):
    clean(db, 6)
    command_id = a_command(db, "level")

    db.connect("cli").execute(LADDER, ("L1", "ada", "read all six", command_id))

    assert db.connect("cli").execute("SELECT level FROM agb.current_level").fetchall() == [("L1",)]


@pytest.mark.parametrize("table", ["ladder", "mode"])
def test_the_cli_may_not_write_the_ladder_or_the_mode_as_the_system(db, table):
    statement, value = (LADDER, "L0") if table == "ladder" else (MODE, "paper")

    with pytest.raises(
        errors.InsufficientPrivilege, match="only the roles that act unattended write as system"
    ):
        db.connect("cli").execute(statement, (value, "system", None, None))


@pytest.mark.parametrize("role", ["scheduler", "execution"])
def test_the_services_still_drop_the_mode_to_paper_as_the_system(db, role):
    go_live(db)

    db.connect(role).execute(MODE, ("paper", "system", None, None))

    assert db.connect("cli").execute("SELECT mode FROM agb.current_mode").fetchall() == [("paper",)]


# -- live, reached in the database -------------------------------------------------------------


def test_the_database_refuses_live_before_paper_has_earned_it(db):
    command_id = a_command(db, "mode")

    with pytest.raises(
        errors.InsufficientPrivilege, match="live is not earned yet: paper_rebalances,"
    ):
        db.connect("cli").execute(MODE, ("live", "ada", "sound", command_id))


def test_the_database_refuses_live_while_a_waived_model_is_promoted(db):
    earned_live(db)
    registry_fake.waive(db)
    command_id = a_command(db, "mode")

    with pytest.raises(
        errors.InsufficientPrivilege,
        match=f"the live model {registry_fake.FAILED_ID} is promoted under the paper-only",
    ):
        db.connect("cli").execute(MODE, ("live", "ada", "sound", command_id))


def test_the_database_lets_an_earned_live_be_reached(db):
    earned_live(db)
    command_id = a_command(db, "mode")

    db.connect("cli").execute(MODE, ("live", "ada", "sound", command_id))

    assert db.connect("cli").execute("SELECT mode FROM agb.current_mode").fetchall() == [("live",)]
