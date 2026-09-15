"""Database roles.

Each long-running service connects as the role of the same name, and the operator CLI has
its own role. The owner role owns every table and runs migrations; no service uses it. The
backup service reads every table through `pg_read_all_data`, granted by `migrate.bootstrap`.
"""

OWNER = "agb_owner"
BACKUP = "backup"
SERVICES = ("scheduler", "pipeline", "execution", "bot", "ui", BACKUP)
CLI = "cli"
SERVICE_ROLES = (*SERVICES, CLI)
ALL_ROLES = (OWNER, *SERVICE_ROLES)

# The env/db.env variable that holds each role's password.
PASSWORD_VARS = {
    OWNER: "AGB_OWNER_PASSWORD",
    **{role: f"{role.upper()}_DB_PASSWORD" for role in SERVICE_ROLES},
}
