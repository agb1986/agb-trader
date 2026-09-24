"""Database roles.

Each long-running service connects as the role of the same name, and the operator CLI and
the research profile's artifact import have their own roles. The owner role owns every table
and runs migrations; no service uses it. The backup service reads every table through
`pg_read_all_data`, granted by `migrate.bootstrap`.
"""

OWNER = "agb_owner"
PIPELINE = "pipeline"
EXECUTION = "execution"
BACKUP = "backup"
SERVICES = ("scheduler", PIPELINE, EXECUTION, "bot", "ui", BACKUP)
CLI = "cli"
# The workstation's research profile never connects to the server's database; this role
# exists for the one thing research writes there, the certification artifact import.
RESEARCH = "research"
SERVICE_ROLES = (*SERVICES, CLI, RESEARCH)
ALL_ROLES = (OWNER, *SERVICE_ROLES)

# The env/db.env variable that holds each role's password.
PASSWORD_VARS = {
    OWNER: "AGB_OWNER_PASSWORD",
    **{role: f"{role.upper()}_DB_PASSWORD" for role in SERVICE_ROLES},
}
