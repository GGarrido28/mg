#!/usr/bin/env python3
"""
Admin script for managing PostgreSQL user privileges.
Run this to grant a user access to all databases and schemas.
"""
import logging
import psycopg2

from mg.db.config import _DO_HOST, _DO_PASSWORD, _DO_USER, _DO_PORT, DB_SCHEMAS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def grant_user_privileges(username: str):
    """Grant a user full privileges on all databases and schemas."""
    databases = list(DB_SCHEMAS.keys())

    # Create a connection to defaultdb to grant connect privileges
    conn = psycopg2.connect(
        dbname="defaultdb",
        user=_DO_USER,
        password=_DO_PASSWORD,
        host=_DO_HOST,
        port=_DO_PORT,
    )
    conn.autocommit = True
    cur = conn.cursor()

    logger.info(f"Granting connect privileges to {username}...")
    for db in databases:
        cur.execute(f"GRANT CONNECT ON DATABASE {db} TO {username};")

    conn.close()

    # Connect to each database and grant schema privileges
    for db in databases:
        logger.info(f"Processing database: {db}")
        conn = psycopg2.connect(
            dbname=db,
            user=_DO_USER,
            password=_DO_PASSWORD,
            host=_DO_HOST,
            port=_DO_PORT,
        )
        conn.autocommit = True
        cur = conn.cursor()

        for schema in DB_SCHEMAS[db]:
            logger.info(f"  Granting privileges on schema: {schema}")
            cur.execute(f"GRANT ALL PRIVILEGES ON SCHEMA {schema} TO {username};")
            cur.execute(
                f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {username};"
            )
            cur.execute(
                f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO {username};"
            )
            cur.execute(
                f"GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {schema} TO {username};"
            )
            cur.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON TABLES TO {username};"
            )
            cur.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON SEQUENCES TO {username};"
            )
            cur.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON FUNCTIONS TO {username};"
            )

        conn.close()

    logger.info(f"All privileges granted to {username} successfully!")


def create_user(username: str, password: str):
    """Create a new database user with CREATEDB and CREATEROLE privileges."""
    conn = psycopg2.connect(
        dbname="defaultdb",
        user=_DO_USER,
        password=_DO_PASSWORD,
        host=_DO_HOST,
        port=_DO_PORT,
    )
    conn.autocommit = True
    cur = conn.cursor()

    logger.info(f"Creating user {username}...")
    cur.execute(
        f"CREATE USER {username} WITH CREATEDB CREATEROLE PASSWORD '{password}';"
    )
    conn.close()
    logger.info(f"User {username} created successfully!")


if __name__ == "__main__":
    # import sys

    # if len(sys.argv) < 2:
    #     logger.error("Usage: python postgres_user.py <username>")
    #     logger.error("  Grants full privileges to the specified user on all databases/schemas")
    #     sys.exit(1)

    username = "gabe"
    grant_user_privileges(username)
