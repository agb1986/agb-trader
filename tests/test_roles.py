"""The database roles: which exist, and the variable that holds each one's password."""

from agb import roles


def test_the_owner_is_not_a_service_role():
    assert roles.OWNER == "agb_owner"
    assert roles.OWNER not in roles.SERVICE_ROLES
    assert roles.OWNER not in roles.SERVICES


def test_every_service_connects_as_the_role_of_its_name():
    assert set(roles.SERVICES) == {"scheduler", "pipeline", "execution", "bot", "ui", "backup"}
    assert roles.PIPELINE == "pipeline"
    assert roles.EXECUTION == "execution"
    assert roles.BACKUP == "backup"


def test_the_cli_and_the_research_import_have_their_own_roles():
    assert {roles.CLI, roles.RESEARCH} == {"cli", "research"}
    assert set(roles.SERVICE_ROLES) == {*roles.SERVICES, roles.CLI, roles.RESEARCH}


def test_all_roles_lists_the_owner_first_and_each_role_once():
    assert roles.ALL_ROLES[0] == roles.OWNER
    assert roles.ALL_ROLES[1:] == roles.SERVICE_ROLES
    assert len(set(roles.ALL_ROLES)) == len(roles.ALL_ROLES)


def test_each_role_has_one_password_variable():
    assert set(roles.PASSWORD_VARS) == set(roles.ALL_ROLES)
    assert len(set(roles.PASSWORD_VARS.values())) == len(roles.ALL_ROLES)


def test_the_password_variables_follow_the_env_file_naming():
    assert roles.PASSWORD_VARS[roles.OWNER] == "AGB_OWNER_PASSWORD"
    assert roles.PASSWORD_VARS["pipeline"] == "PIPELINE_DB_PASSWORD"
    assert roles.PASSWORD_VARS["cli"] == "CLI_DB_PASSWORD"
    for role in roles.SERVICE_ROLES:
        assert roles.PASSWORD_VARS[role] == f"{role.upper()}_DB_PASSWORD"
