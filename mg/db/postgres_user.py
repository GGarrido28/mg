#!/usr/bin/env python3
"""
Admin script for managing PostgreSQL user privileges.
Run this to grant a user access to all databases and schemas.
"""
import logging
import re
import secrets
import string

import psycopg2
from psycopg2 import sql

from mg.db.config import _DO_HOST, _DO_PASSWORD, _DO_USER, _DO_PORT, DB_SCHEMAS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_identifier(name: str, identifier_type: str = "identifier") -> str:
    """Validate that a string is safe to use as a SQL identifier.

    Args:
        name: The identifier to validate
        identifier_type: Type of identifier for error messages

    Returns:
        The validated identifier

    Raises:
        ValueError: If the identifier contains invalid characters
    """
    if not name or not isinstance(name, str):
        raise ValueError(f"Invalid {identifier_type}: must be a non-empty string")

    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(
            f"Invalid {identifier_type} '{name}': must start with letter or underscore, "
            "and contain only alphanumeric characters and underscores"
        )

    # Note: 'source' is included as it's a reserved word in some SQL contexts
    sql_keywords = {'select', 'insert', 'update', 'delete', 'drop', 'truncate', 'alter', 'create', 'source'}
    if name.lower() in sql_keywords:
        raise ValueError(f"Invalid {identifier_type} '{name}': cannot use SQL keyword as identifier")

    return name


def grant_user_privileges(username: str):
    """Grant a user full privileges on all databases and schemas."""
    # Validate username to prevent SQL injection
    validate_identifier(username, "username")

    databases = list(DB_SCHEMAS.keys())

    # Validate all database and schema names
    for db in databases:
        validate_identifier(db, "database")
        for schema in DB_SCHEMAS[db]:
            validate_identifier(schema, "schema")

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
        cur.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {db} TO {user}").format(
                db=sql.Identifier(db),
                user=sql.Identifier(username)
            )
        )

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
            cur.execute(
                sql.SQL("GRANT ALL PRIVILEGES ON SCHEMA {schema} TO {user}").format(
                    schema=sql.Identifier(schema),
                    user=sql.Identifier(username)
                )
            )
            cur.execute(
                sql.SQL("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {user}").format(
                    schema=sql.Identifier(schema),
                    user=sql.Identifier(username)
                )
            )
            cur.execute(
                sql.SQL("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO {user}").format(
                    schema=sql.Identifier(schema),
                    user=sql.Identifier(username)
                )
            )
            cur.execute(
                sql.SQL("GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {schema} TO {user}").format(
                    schema=sql.Identifier(schema),
                    user=sql.Identifier(username)
                )
            )
            cur.execute(
                sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON TABLES TO {user}").format(
                    schema=sql.Identifier(schema),
                    user=sql.Identifier(username)
                )
            )
            cur.execute(
                sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON SEQUENCES TO {user}").format(
                    schema=sql.Identifier(schema),
                    user=sql.Identifier(username)
                )
            )
            cur.execute(
                sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON FUNCTIONS TO {user}").format(
                    schema=sql.Identifier(schema),
                    user=sql.Identifier(username)
                )
            )

        conn.close()

    logger.info(f"All privileges granted to {username} successfully!")


def generate_password(length: int = 24) -> str:
    """Generate a secure random password."""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def create_user(username: str) -> str:
    """Create a new database user with CREATEDB and CREATEROLE privileges.

    Returns:
        The auto-generated password for the new user.
    """
    # Validate username to prevent SQL injection
    validate_identifier(username, "username")

    # Auto-generate a secure password
    password = generate_password()

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
    # Use sql.Identifier for username and sql.Literal for password
    cur.execute(
        sql.SQL("CREATE USER {user} WITH CREATEDB CREATEROLE PASSWORD {password}").format(
            user=sql.Identifier(username),
            password=sql.Literal(password)
        )
    )
    conn.close()
    logger.info(f"User {username} created successfully!")

    print(f"\n{'='*50}")
    print(f"User '{username}' created with password:")
    print(f"  {password}")
    print(f"{'='*50}\n")

    return password


if __name__ == "__main__":
    # import sys

    # if len(sys.argv) < 2:
    #     logger.error("Usage: python postgres_user.py <username>")
    #     logger.error("  Grants full privileges to the specified user on all databases/schemas")
    #     sys.exit(1)

    # username = sys.argv[1]
    grant_user_privileges("do_droplet")

    # create_user("do_droplet")