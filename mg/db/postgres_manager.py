import psycopg2
from psycopg2 import sql
from psycopg2.extras import register_uuid
import logging
import json
import re
from datetime import datetime, date, time
from uuid import UUID
import socket
from time import sleep

from mg.db.config import POSTGRES_HOSTS

logging.basicConfig(level=logging.INFO)

# Register UUID adapter so psycopg2 can handle Python UUID objects
register_uuid()


class PostgresManager:
    @staticmethod
    def get_nested_config(config_dict, keys, default=None):
        """
        Safely access nested dictionary values.

        Args:
            config_dict (dict): The dictionary to access
            keys (list): A list of keys forming the path to the desired value
            default: Value to return if the path doesn't exist

        Returns:
            The value at the specified path or the default value
        """
        current = config_dict
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                return default
            current = current[key]
        return current

    @staticmethod
    def validate_identifier(name, identifier_type="identifier"):
        """
        Validate that a string is safe to use as a SQL identifier.

        Args:
            name (str): The identifier to validate
            identifier_type (str): Type of identifier for error messages (e.g., "table", "column")

        Returns:
            str: The validated identifier

        Raises:
            ValueError: If the identifier contains invalid characters
        """
        if not name or not isinstance(name, str):
            raise ValueError(f"Invalid {identifier_type}: must be a non-empty string")

        # Allow alphanumeric, underscores, and dots (for schema.table notation)
        # PostgreSQL identifiers can also start with underscore or letter
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValueError(
                f"Invalid {identifier_type} '{name}': must start with letter or underscore, "
                "and contain only alphanumeric characters and underscores"
            )

        # Check for SQL keywords that could be problematic (basic check)
        sql_keywords = {'select', 'insert', 'update', 'delete', 'drop', 'truncate', 'alter', 'create'}
        if name.lower() in sql_keywords:
            raise ValueError(f"Invalid {identifier_type} '{name}': cannot use SQL keyword as identifier")

        return name

    @staticmethod
    def verify_config_exists(host, database, schema):
        """
        Verify that the requested configuration exists in POSTGRES_HOSTS.

        Args:
            host (str): The host key
            database (str): The database key
            schema (str): The schema key

        Returns:
            bool: True if the configuration exists, False otherwise

        Example:
            >>> PostgresManager.verify_config_exists('digital_ocean', 'nfl', 'core')
            True
            >>> PostgresManager.verify_config_exists('nonexistent_host', 'db', 'schema')
            False
        """
        try:
            if (
                host in POSTGRES_HOSTS
                and database in POSTGRES_HOSTS[host]
                and schema in POSTGRES_HOSTS[host][database]
            ):
                return True
            return False
        except (KeyError, TypeError):
            return False

    def __init__(self, host, database, schema, return_logging=False):
        # First validate the configuration exists
        if not PostgresManager.verify_config_exists(host, database, schema):
            # Generate detailed error message
            if host not in POSTGRES_HOSTS:
                raise ValueError(
                    f"Host '{host}' not found in POSTGRES_HOSTS configuration"
                )

            host_config = POSTGRES_HOSTS.get(host)
            if database not in host_config:
                raise ValueError(
                    f"Database '{database}' not found in '{host}' configuration"
                )

            db_config = host_config.get(database)
            if schema not in db_config:
                raise ValueError(
                    f"Schema '{schema}' not found in '{host}.{database}' configuration"
                )

        # Now that we've validated the path exists, get the configuration
        schema_config = PostgresManager.get_nested_config(
            POSTGRES_HOSTS, [host, database, schema], {}
        )

        # Set up instance variables
        self.url = schema_config.get("url")
        self.key = schema_config.get("key")
        self.database = database
        self.schema = schema
        self.host = schema_config.get("host")
        self.user = schema_config.get("user")
        self.password = schema_config.get("password")
        self.port = schema_config.get("port")
        self.return_logging = return_logging

        # Validate database and schema names to prevent injection in search_path
        self.validate_identifier(self.database, "database")
        self.validate_identifier(self.schema, "schema")

        # Validate essential connection parameters
        for param_name, param_value in [
            ("host", self.host),
            ("user", self.user),
            ("password", self.password),
            ("port", self.port),
        ]:
            if param_value is None:
                raise ValueError(
                    f"Required connection parameter '{param_name}' is missing for '{host}.{database}.{schema}'"
                )

        # Add more verbose network diagnostics
        if self.return_logging:
            logging.info(
                f"Attempting basic network connection to {self.host}:{self.port}"
            )
        if not self.test_db_connection(self.host, self.port):
            # Try to get more network diagnostics
            try:
                import socket

                logging.error(
                    f"DNS lookup for {self.host}: {socket.gethostbyname(self.host)}"
                )
                logging.error(
                    f"Current IP: {socket.gethostbyname(socket.gethostname())}"
                )
            except Exception as e:
                logging.error(f"Network diagnostic failed: {e}")
            raise ConnectionError(
                f"Cannot establish basic connection to {self.host}:{self.port}"
            )

        if self.return_logging:
            logging.info(
                "Basic network connectivity successful, attempting database connection"
            )
        self.connect_with_retries()
        # Default to autocommit=True for simple queries (execute(), check_table_exists(), etc.)
        # Methods that need transactions (insert_rows) temporarily disable autocommit
        # and restore it when done via _get_and_set_autocommit() / _set_autocommit_safely()
        self.connection.set_session(autocommit=True)

        # Add logging to debug connection parameters
        if self.return_logging:
            logging.info(f"Connected to: {self.host}:{self.port}")
            logging.info(f"Database: {self.database}")
            logging.info(f"Schema: {self.schema}")

    def get_cursor(self):
        """Get a new cursor, creating a new connection if necessary."""
        try:
            # Test if connection is still alive
            self.connection.isolation_level
            cursor = self.connection.cursor()
            return cursor
        except (psycopg2.OperationalError, psycopg2.InterfaceError, AttributeError):
            logging.info("Connection lost, reconnecting...")
            self.connect_with_retries()
            return self.connection.cursor()

    def test_db_connection(self, host, port):
        try:
            sock = socket.create_connection((host, port), timeout=10)
            sock.close()
            return True
        except Exception as e:
            logging.error(f"Connection test failed: {e}")
            return False

    def connect_with_retries(self, max_retries=5):
        for attempt in range(max_retries):
            try:
                # logging.info(f"Connection attempt {attempt + 1}/{max_retries}")
                self.connection = psycopg2.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    database=self.database,
                    options=f"-c search_path={self.schema}",
                    connect_timeout=10,
                    sslmode="require",
                    # Try without SSL verification first
                    sslrootcert=None,
                )
                if self.return_logging:
                    logging.info("Database connection successful!")
                return True
            except psycopg2.OperationalError as e:
                logging.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    sleep_time = 2**attempt  # Exponential backoff
                    logging.info(f"Retrying in {sleep_time} seconds...")
                    sleep(sleep_time)
                else:
                    logging.error("All connection attempts failed")
                    raise ConnectionError("Cannot establish database connection")
        return False

    def execute_query(self, query, params=None):
        cursor = self.get_cursor()
        if self.return_logging:
            logging.info(query)
        try:
            cursor.execute(query, params)
            if cursor.description:  # Check if the query returns results
                columns = list(cursor.description)
                result = cursor.fetchall()
                results = []
                for row in result:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        row_dict[col.name] = row[i]
                    results.append(row_dict)
                return results
            return []
        except Exception as e:
            logging.warning(e)
            return []
        finally:
            cursor.close()

    def get_table_primary_key(self, table):
        # Validate table name
        self.validate_identifier(table, "table")

        cursor = self.get_cursor()
        q = """
            SELECT column_name
            FROM information_schema.table_constraints
            JOIN information_schema.key_column_usage
                    USING (constraint_catalog, constraint_schema, constraint_name,
                            table_catalog, table_schema, table_name)
            WHERE constraint_type = 'PRIMARY KEY'
            AND (table_schema, table_name) = (%s, %s)
            ORDER BY ordinal_position;"""
        try:
            cursor.execute(q, (self.schema, table))
            result = cursor.fetchall()
            results = [row[0] for row in result]

            if len(results) > 0:
                return results

            # Check if table exists and user has access via pg_catalog (more reliable)
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                );""", (self.schema, table))
            table_exists_pg = cursor.fetchone()[0]

            # Check if table is visible in information_schema (permission-dependent)
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                );""", (self.schema, table))
            table_visible_info_schema = cursor.fetchone()[0]

            if not table_exists_pg:
                msg = f"Table {self.schema}.{table} does not exist."
                logging.error(msg)
            elif not table_visible_info_schema:
                msg = f"Table {self.schema}.{table} exists but current user lacks SELECT privilege. Grant access with: GRANT SELECT ON {self.schema}.{table} TO <username>;"
                logging.error(msg)
            else:
                msg = f"Table {self.schema}.{table} does not have a primary key."
                logging.warning(msg)
            return None
        finally:
            cursor.close()

    def determine_column_type(self, values):
        """
        Determines the appropriate PostgreSQL type for a column based on all the values.
        """
        type_map = {
            str: "TEXT",
            float: "REAL",
            bool: "BOOLEAN",
            dict: "JSON",
            list: "JSON",
            datetime: "TIMESTAMP",
            date: "DATE",
            time: "TIME",
            bytes: "BYTEA",
            UUID: "UUID",
        }

        non_none_values = [value for value in values if value is not None]
        encountered_types = set(type(value) for value in non_none_values)

        if len(encountered_types) == 0:
            return "TEXT"

        if len(encountered_types) == 1:
            encountered_type = next(iter(encountered_types))
            if encountered_type == int:
                max_value = max(non_none_values)
                min_value = min(non_none_values)
                if -32768 <= min_value <= 32767 and max_value <= 32767:
                    return "SMALLINT"
                elif -2147483648 <= min_value <= 2147483647 and max_value <= 2147483647:
                    return "INTEGER"
                else:
                    return "BIGINT"
            return type_map.get(encountered_type, "TEXT")

        # Handle multiple types, selecting the most encompassing PostgreSQL type
        if encountered_types <= {int, float}:
            return "REAL"
        elif encountered_types <= {str, int, float}:
            return "TEXT"
        elif encountered_types <= {datetime, str}:
            return "TIMESTAMP"
        elif encountered_types <= {date, str}:
            return "DATE"
        elif encountered_types <= {time, str}:
            return "TIME"
        else:
            return "TEXT"

    def check_duplicate_rows(self, rows, columns=[]):
        duplicates = False
        duplicate_rows = {}

        for row in rows:
            # Create a filtered version of the row based on the specified columns
            filtered_row = {key: row[key] for key in columns if key in row}
            if not filtered_row:
                continue
            for key, value in filtered_row.items():
                # Handle complex data types by converting them to a string representation
                if isinstance(value, (dict, list)):
                    filtered_row[key] = json.dumps(value)
                elif isinstance(value, (datetime, date)):
                    filtered_row[key] = value.isoformat()
                elif isinstance(value, UUID):
                    filtered_row[key] = str(value)

            # Generate a unique key for the row based on its contents
            row_key = json.dumps(filtered_row, sort_keys=True)

            # Track duplicates by incrementing the count if the row_key already exists
            if row_key in duplicate_rows:
                duplicates = True
                duplicate_rows[row_key] += 1
            else:
                duplicate_rows[row_key] = 1

        # Return only rows that have duplicates (count > 1)
        flagged_duplicates = {
            key: count for key, count in duplicate_rows.items() if count > 1
        }

        return duplicates, flagged_duplicates

    def get_all_columns(self, rows, columns=None):
        if columns is None:
            columns = []

        has_all_columns = True
        for row in rows:
            if list(row.keys()) != columns:
                has_all_columns = False
                for key in row:
                    if key not in columns:
                        columns.append(key)
        if not has_all_columns:
            for col in columns:
                for row in rows:
                    if col not in row:
                        row[col] = None
        return columns

    def insert_rows(
        self, target_table, columns, rows, contains_dicts=False, update=False, return_error_msg=False
    ):
        """Insert rows into a table using parameterized queries.

        Args:
            target_table (str): Table to insert rows into.
            columns (list): List of column names.
            rows (list): List of rows to insert (list of dicts if contains_dicts=True).
            contains_dicts (bool): Whether the rows contain dictionaries.
            update (bool): Whether to update existing rows (upsert).
            return_error_msg (bool): If True, return tuple (success, error_msg). If False, return only bool for backward compatibility.

        Returns:
            If return_error_msg=False (default):
                bool: True if successful, False otherwise
            If return_error_msg=True:
                tuple: (success: bool, error_message: str | None)
                    - success: True if successful, False otherwise
                    - error_message: None if successful, detailed error message if failed
        """
        # Initialize variables outside the try block
        old_autocommit = None
        query_str = None  # For error reporting

        # Validate table name
        self.validate_identifier(target_table, "table")

        try:
            # Safely get and modify the connection state
            old_autocommit = self._get_and_set_autocommit(False)

            with (
                self.connection
            ):  # This creates a transaction block that auto-commits/rollbacks
                with self.connection.cursor() as cursor:
                    check_dupes, dupe_rows = self.check_duplicate_rows(rows, columns)
                    if check_dupes:
                        logging.warning("Duplicate rows found in data.")
                        logging.warning(dupe_rows)
                        duplicate_rows = []
                        for key in dupe_rows:
                            for row in rows:
                                filtered_row = {k: row[k] for k in columns}
                                if json.dumps(filtered_row) == key:
                                    duplicate_rows.append(row)
                        for row in duplicate_rows:
                            logging.info(row)
                            rows.remove(row)

                    pk = self.get_table_primary_key(target_table)
                    if pk is None:
                        logging.warning(f"No primary key found for table {target_table} in schema {self.schema}")
                    check_dupe_keys, dupe_keys = self.check_duplicate_rows(rows, pk if pk is not None else [])
                    if check_dupe_keys:
                        logging.warning(f"Duplicate primary keys found in data.")
                        logging.warning(dupe_keys)
                        duplicate_rows = []
                        for key in dupe_keys:
                            for row in rows:
                                filtered_row = {k: row[k] for k in pk}
                                if json.dumps(filtered_row) == key:
                                    duplicate_rows.append(row)
                        for row in duplicate_rows:
                            logging.info(row)
                            rows.remove(row)

                    columns = list(columns)
                    columns = self.get_all_columns(rows, columns)

                    # Validate all column names
                    for col in columns:
                        self.validate_identifier(col, "column")

                    # Build column identifiers safely using psycopg2.sql
                    col_identifiers = [sql.Identifier(col.lower()) for col in columns]

                    # Prepare row values for parameterized insertion
                    prepared_rows = []
                    if contains_dicts:
                        for row in rows:
                            prepared_row = []
                            for col in columns:
                                value = row.get(col)
                                if isinstance(value, (dict, list)):
                                    value = json.dumps(value)
                                elif value == "":
                                    value = None
                                prepared_row.append(value)
                            prepared_rows.append(tuple(prepared_row))
                    else:
                        # Handle single dict case (legacy behavior)
                        if isinstance(rows, dict):
                            prepared_row = [rows.get(col) for col in columns]
                            prepared_rows.append(tuple(prepared_row))
                        else:
                            prepared_rows = [tuple(row) if isinstance(row, (list, tuple)) else (row,) for row in rows]

                    # Build the INSERT query using psycopg2.sql
                    # Create placeholders for each row
                    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))
                    values_template = sql.SQL("({})").format(placeholders)

                    # Build full query
                    base_query = sql.SQL("INSERT INTO {schema}.{table} ({columns}) VALUES ").format(
                        schema=sql.Identifier(self.schema),
                        table=sql.Identifier(target_table),
                        columns=sql.SQL(", ").join(col_identifiers)
                    )

                    # Build ON CONFLICT clause if update=True
                    conflict_clause = sql.SQL("")
                    if update:
                        if pk is None:
                            error_msg = f"Cannot perform upsert on table {target_table} - no primary key defined"
                            logging.error(error_msg)
                            return (False, error_msg) if return_error_msg else False

                        # Validate primary key columns
                        for p in pk:
                            self.validate_identifier(p, "primary key column")

                        pk_identifiers = [sql.Identifier(p) for p in pk]
                        update_cols = [col for col in columns if col not in pk]

                        if update_cols:
                            set_clause = sql.SQL(", ").join([
                                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col.lower()))
                                for col in update_cols
                            ])
                            conflict_clause = sql.SQL(" ON CONFLICT ({pk}) DO UPDATE SET {set_clause}").format(
                                pk=sql.SQL(", ").join(pk_identifiers),
                                set_clause=set_clause
                            )
                        else:
                            # All columns are primary keys, just do nothing on conflict
                            conflict_clause = sql.SQL(" ON CONFLICT ({pk}) DO NOTHING").format(
                                pk=sql.SQL(", ").join(pk_identifiers)
                            )

                    # Execute with executemany for multiple rows
                    if len(prepared_rows) == 1:
                        full_query = base_query + values_template + conflict_clause
                        query_str = full_query.as_string(self.connection)
                        if self.return_logging:
                            logging.info(query_str)
                        cursor.execute(full_query, prepared_rows[0])
                    else:
                        # For multiple rows, use executemany or build a multi-value insert
                        # executemany is simpler and safer
                        full_query = base_query + values_template + conflict_clause
                        query_str = full_query.as_string(self.connection)
                        if self.return_logging:
                            logging.info(f"{query_str} (executemany with {len(prepared_rows)} rows)")
                        cursor.executemany(full_query, prepared_rows)

                    logging.info(f"Rows inserted successfully into {target_table}")
            return (True, None) if return_error_msg else True
        except psycopg2.errors.UniqueViolation as e:
            # Handle unique constraint violations
            error_msg = f"Unique constraint violation: {e}"
            logging.warning(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.errors.ForeignKeyViolation as e:
            # Handle foreign key constraint violations
            error_msg = f"Foreign key constraint violation: {e}"
            logging.warning(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.errors.InFailedSqlTransaction as e:
            # Handle transactions that are already in a failed state
            error_msg = f"Transaction already failed: {e}"
            logging.warning(error_msg)
            if not self.connection.closed:
                self.connection.rollback()
            return (False, error_msg) if return_error_msg else False
        except psycopg2.errors.DeadlockDetected as e:
            # Handle deadlock situations
            error_msg = f"Deadlock detected: {e}. Retrying might solve this issue."
            logging.warning(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.OperationalError as e:
            # Handle connection issues (e.g., idle-in-transaction timeout)
            error_msg = f"Database connection error: {e}"
            logging.error(error_msg)
            self.connect_with_retries()
            return (False, error_msg) if return_error_msg else False
        except psycopg2.InterfaceError as e:
            # Handle connection already closed errors
            error_msg = f"Database interface error (connection closed): {e}"
            logging.error(error_msg)
            self.connect_with_retries()
            return (False, error_msg) if return_error_msg else False
        except psycopg2.errors.NumericValueOutOfRange as e:
            # Handle numeric overflow/underflow errors (e.g., value too large for smallint)
            error_msg = self._format_sql_error("SQL Data Type Error (Numeric Out of Range)", e, query_str)
            logging.error(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.errors.StringDataRightTruncation as e:
            # Handle string too long for column
            error_msg = self._format_sql_error("SQL Data Type Error (String Too Long)", e, query_str)
            logging.error(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.DataError as e:
            # Handle other data type errors
            error_msg = self._format_sql_error("SQL Data Type Error", e, query_str)
            logging.error(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.IntegrityError as e:
            # Handle other integrity constraint violations not caught above
            error_msg = self._format_sql_error("SQL Integrity Constraint Violation", e, query_str)
            logging.error(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.ProgrammingError as e:
            # Handle SQL syntax or programming errors
            error_msg = self._format_sql_error("SQL Programming Error", e, query_str)
            logging.error(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.DatabaseError as e:
            # Handle other database-related errors
            error_msg = self._format_sql_error("SQL Database Error", e, query_str)
            logging.error(error_msg)
            return (False, error_msg) if return_error_msg else False
        except Exception as e:
            error_msg = f"Unexpected error inserting rows: {e}"
            logging.error(error_msg, exc_info=True)
            return (False, error_msg) if return_error_msg else False
        finally:
            # Safely restore previous autocommit setting
            if old_autocommit is not None:
                self._set_autocommit_safely(old_autocommit)

    def _format_sql_error(self, error_category, exception, query=None):
        """
        Format SQL errors with detailed diagnostic information.

        Args:
            error_category: Category of the error (e.g., "SQL Data Type Error")
            exception: The psycopg2 exception object
            query: Optional SQL query that caused the error

        Returns:
            str: Formatted error message with SQL diagnostics
        """
        error_parts = [f"{error_category}: {type(exception).__name__}"]

        # Add SQL error code if available
        if hasattr(exception, 'pgcode') and exception.pgcode:
            error_parts.append(f"[SQL Error Code: {exception.pgcode}]")

        # Add the main error message
        error_parts.append(f"- {str(exception)}")

        # Add diagnostics if available
        if hasattr(exception, 'diag'):
            diag = exception.diag
            diagnostics = []

            if hasattr(diag, 'severity') and diag.severity:
                diagnostics.append(f"Severity: {diag.severity}")

            if hasattr(diag, 'message_primary') and diag.message_primary:
                diagnostics.append(f"Message: {diag.message_primary}")

            if hasattr(diag, 'message_detail') and diag.message_detail:
                diagnostics.append(f"Detail: {diag.message_detail}")

            if hasattr(diag, 'message_hint') and diag.message_hint:
                diagnostics.append(f"Hint: {diag.message_hint}")

            if hasattr(diag, 'column_name') and diag.column_name:
                diagnostics.append(f"Column: {diag.column_name}")

            if hasattr(diag, 'table_name') and diag.table_name:
                diagnostics.append(f"Table: {diag.table_name}")

            if hasattr(diag, 'schema_name') and diag.schema_name:
                diagnostics.append(f"Schema: {diag.schema_name}")

            if diagnostics:
                error_parts.append(f"Diagnostics: {' | '.join(diagnostics)}")

        # Optionally include truncated query for context (limit to 200 chars to avoid log spam)
        if query and len(query) > 0:
            query_preview = query[:200] + "..." if len(query) > 200 else query
            error_parts.append(f"Query preview: {query_preview}")

        return " | ".join(error_parts)

    def _has_valid_connection(self):
        """Check if there is a valid, open database connection.

        Returns:
            bool: True if there is a valid connection, False otherwise.
        """
        return (
            hasattr(self, "connection")
            and self.connection is not None
            and not getattr(self.connection, "closed", True)
        )

    def _ensure_clean_transaction_state(self):
        """Ensure the connection is not in a failed or pending transaction state.

        This method should be called after operations that may leave the connection
        in an inconsistent state, or before operations that require changing
        session-level settings like autocommit.
        """
        if not self._has_valid_connection():
            return

        try:
            # Check if we're in a transaction
            # status values: STATUS_READY (0), STATUS_BEGIN (1), STATUS_IN_TRANSACTION (2), STATUS_PREPARED (3)
            # INERROR states are 4+
            status = self.connection.get_transaction_status()

            # psycopg2 transaction status constants
            TRANSACTION_STATUS_IDLE = 0
            TRANSACTION_STATUS_INERROR = 4

            if status >= TRANSACTION_STATUS_INERROR:
                # Connection is in an error state, need to rollback
                self.connection.rollback()
            elif status != TRANSACTION_STATUS_IDLE:
                # Connection is in a transaction but not in error - commit or rollback
                # We'll commit to preserve any pending changes
                self.connection.commit()
        except Exception as e:
            logging.warning(f"Error ensuring clean transaction state: {e}")
            try:
                self.connection.rollback()
            except Exception:
                pass  # Best effort

    def _get_and_set_autocommit(self, new_value):
        """Safely get the current autocommit value and set a new one.

        Args:
            new_value (bool): New autocommit value to set

        Returns:
            bool or None: The previous autocommit value, or None if no valid connection
        """
        if not self._has_valid_connection():
            try:
                # Attempt to reconnect if no valid connection
                self.connect_with_retries()
            except Exception as e:
                logging.error(f"Failed to establish database connection: {e}")
                return None

        try:
            # Ensure we're not in a transaction before changing autocommit
            self._ensure_clean_transaction_state()

            # Get current value
            old_value = getattr(self.connection, "autocommit", True)
            # Set new value
            self.connection.autocommit = new_value
            return old_value
        except Exception as e:
            logging.warning(f"Error getting/setting autocommit: {e}")
            return None

    def _set_autocommit_safely(self, value):
        """Safely set the autocommit value on the connection.

        Args:
            value (bool): Autocommit value to set
        """
        if self._has_valid_connection():
            try:
                # Ensure we're not in a transaction before changing autocommit
                self._ensure_clean_transaction_state()
                self.connection.autocommit = value
            except Exception as e:
                logging.warning(f"Error setting autocommit to {value}: {e}")

    def execute(self, q, params=None, raise_exc=False):
        # Ensure clean transaction state before executing
        self._ensure_clean_transaction_state()
        cursor = self.get_cursor()
        if self.return_logging:
            logging.info(q)
        results = []
        try:
            if params:
                cursor.execute(q, params)
            else:
                cursor.execute(q)

            if cursor.description:  # Check if the query returns results
                results = cursor.fetchall()
                field_names = [i[0] for i in cursor.description]
                results = [dict(zip(field_names, row)) for row in results]
            return results
        except Exception as e:
            if self.return_logging:
                logging.warning(e)
            if raise_exc:
                raise
            return results
        finally:
            cursor.close()

    def update_automation_log(self, task, step, status=None, message=None):
        log = [
            {
                "task": task,
                "step": step,
                "status": status,
                "log_message": message,
                "disabled": False,
            }
        ]
        self.insert_rows(
            "automation_log", log[0].keys(), log, contains_dicts=True, update=True
        )

    def ensure_update_trigger_exists(self):
        # Use psycopg2.sql for safe schema identifier
        trigger_function_query = sql.SQL("""
        CREATE OR REPLACE FUNCTION {schema}.update_updated_at() RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """).format(schema=sql.Identifier(self.schema))
        try:
            cursor = self.get_cursor()
            cursor.execute(trigger_function_query)
            self.connection.commit()
            cursor.close()
            logging.info("Ensured update trigger function exists.")
        except Exception as e:
            logging.error(f"Error ensuring update trigger: {e}")
            self.connection.rollback()

    def create_table(self, dict_list, primary_keys=None, table_name=None, delete=False):
        """
        Creates a PostgreSQL table from a list of dictionaries.

        :param dict_list: List of dictionaries containing the data.
        :param primary_keys: List of keys to be used as primary keys. If None, an identity column will be created.
        :param table_name: The name of the table to be created.
        :param delete: If True, will drop and recreate the table if it exists.
        """
        if not dict_list:
            raise ValueError("The dictionary list is empty")

        if primary_keys and not isinstance(primary_keys, list):
            raise ValueError("Primary keys should be provided as a list")

        # Validate table name
        self.validate_identifier(table_name, "table")

        # Extract all column names and their values from the list of dictionaries
        columns = self.get_all_columns(dict_list)

        # Validate all column names
        for col in columns:
            self.validate_identifier(col, "column")

        columns = {key: [] for key in columns}
        for dictionary in dict_list:
            for key, value in dictionary.items():
                columns[key].append(value)

        # Determine the PostgreSQL data type for each column
        columns = {
            key: self.determine_column_type(values) for key, values in columns.items()
        }

        # Validate primary keys if provided
        if primary_keys:
            for pk in primary_keys:
                self.validate_identifier(pk, "primary key")

        # Build CREATE TABLE query using psycopg2.sql
        def build_create_table_query():
            field_parts = []

            if not primary_keys:
                # Add identity column if no primary keys provided
                field_parts.append(sql.SQL('"sql_id" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY'))
                for col, col_type in columns.items():
                    field_parts.append(sql.SQL("{} {}").format(
                        sql.Identifier(col),
                        sql.SQL(col_type)
                    ))
                return sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} ({fields})").format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier(table_name),
                    fields=sql.SQL(", ").join(field_parts)
                )
            else:
                # Make primary keys the first few columns
                priority_found = [item for item in columns if item in primary_keys]
                remaining = [item for item in columns if item not in primary_keys]
                ordered_columns = priority_found + remaining

                for col in ordered_columns:
                    field_parts.append(sql.SQL("{} {}").format(
                        sql.Identifier(col),
                        sql.SQL(columns[col])
                    ))

                pk_identifiers = [sql.Identifier(pk) for pk in primary_keys]
                return sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} ({fields}, PRIMARY KEY ({pks}))").format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier(table_name),
                    fields=sql.SQL(", ").join(field_parts),
                    pks=sql.SQL(", ").join(pk_identifiers)
                )

        create_table_query = build_create_table_query()

        def add_timestamps_and_trigger():
            cursor = self.get_cursor()
            try:
                # Add timestamp columns
                alter_created = sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL"
                ).format(schema=sql.Identifier(self.schema), table=sql.Identifier(table_name))
                alter_updated = sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD updated_at timestamp NULL"
                ).format(schema=sql.Identifier(self.schema), table=sql.Identifier(table_name))
                cursor.execute(alter_created)
                cursor.execute(alter_updated)

                # Create trigger
                trigger_query = sql.SQL(
                    "CREATE TRIGGER update_updated_at BEFORE UPDATE ON {schema}.{table} FOR EACH ROW EXECUTE FUNCTION {schema}.update_updated_at()"
                ).format(schema=sql.Identifier(self.schema), table=sql.Identifier(table_name))
                cursor.execute(trigger_query)
                logging.info(f"Timestamps added to {table_name}; trigger created.")
            finally:
                cursor.close()

        try:
            # Check if table already exists
            table_exists = self.check_table_exists(table_name)
            if table_exists:
                logging.info(f"Table '{table_name}' already exists.")
                if delete:
                    # Drop the table if it already exists
                    drop_query = sql.SQL("DROP TABLE {schema}.{table}").format(
                        schema=sql.Identifier(self.schema),
                        table=sql.Identifier(table_name)
                    )
                    cursor = self.get_cursor()
                    cursor.execute(drop_query)
                    cursor.close()
                    logging.info(f"Table '{table_name}' dropped successfully.")

                    # Execute the create table query
                    cursor = self.get_cursor()
                    cursor.execute(create_table_query)
                    cursor.close()
                    logging.info(f"Table '{table_name}' created successfully.")

                    self.ensure_update_trigger_exists()
                    add_timestamps_and_trigger()
                    self._ensure_clean_transaction_state()
                    return True
                else:
                    self._ensure_clean_transaction_state()
                    return False
            else:
                # Execute the create table query
                cursor = self.get_cursor()
                cursor.execute(create_table_query)
                cursor.close()
                logging.info(f"Table '{table_name}' created successfully.")

                self.ensure_update_trigger_exists()
                add_timestamps_and_trigger()
                self._ensure_clean_transaction_state()
                return True
        except Exception as e:
            self.connection.rollback()
            logging.info(f"Error creating table: {e}")
            return False, e

    def check_table_exists(self, table_name):
        """
        Checks if a table exists in the database.

        :param table_name: The name of the table to check.
        :return: True if the table exists, False otherwise.
        """
        # Validate table name
        self.validate_identifier(table_name, "table")

        try:
            cursor = self.get_cursor()
            # Use parameterized query for values
            query = """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = %s AND table_schema = %s
                )
            """
            cursor.execute(query, (table_name, self.schema))
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else False
        except Exception as e:
            logging.error(f"Error checking if table exists: {e}")
            return False

    def get_tables(self):
        """
        Retrieves a dictionary of all tables within the specified schema of the database,
        where each table name is the key and the value is a dictionary of columns and their data types.

        Returns:
            dict: A dictionary where keys are table names and values are dictionaries of column names and data types.
        """
        # Use parameterized query for schema value
        query = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position;
        """
        try:
            cursor = self.get_cursor()
            cursor.execute(query, (self.schema,))
            rows = cursor.fetchall()
            cursor.close()

            tables = {}
            for table_name, column_name, data_type in rows:
                if table_name not in tables:
                    tables[table_name] = {}
                tables[table_name][column_name] = data_type

            logging.info(
                f"Retrieved tables with columns and data types in schema '{self.schema}': {tables}"
            )
            return tables
        except Exception as e:
            logging.error(f"Error retrieving tables: {e}")
            return {}

    def dump_to_dummy_table(self, dict_list, table_name):
        """
        Dumps a list of dictionaries to a dummy table without requiring primary keys.

        :param dict_list: List of dictionaries containing the data.
        :param table_name: The name of the dummy table to be created or used.
        :return: bool: True if rows are inserted successfully, False otherwise.
        """
        if not dict_list:
            raise ValueError("The dictionary list is empty")

        # Validate table name
        self.validate_identifier(table_name, "table")

        # Extract all column names and their values from the list of dictionaries
        columns = self.get_all_columns(dict_list)
        columns = sorted(columns)

        # Validate all column names
        for col in columns:
            self.validate_identifier(col, "column")

        columns_data = {key: [] for key in columns}
        for dictionary in dict_list:
            for key, value in dictionary.items():
                columns_data[key].append(value)

        # Determine the PostgreSQL data type for each column
        columns_data = {
            key: self.determine_column_type(values)
            for key, values in columns_data.items()
        }

        # Build CREATE TABLE query using psycopg2.sql
        field_parts = [
            sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(col_type))
            for col, col_type in columns_data.items()
        ]
        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {schema}.{table} ({fields})").format(
            schema=sql.Identifier(self.schema),
            table=sql.Identifier(table_name),
            fields=sql.SQL(", ").join(field_parts)
        )

        try:
            # Ensure a new cursor is used to prevent any issues with closed cursors
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_query)
                logging.info(f"Dummy table '{table_name}' created successfully.")

                # Build INSERT query using psycopg2.sql
                col_identifiers = [sql.Identifier(col) for col in columns]
                placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))

                insert_query = sql.SQL("INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders})").format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier(table_name),
                    columns=sql.SQL(", ").join(col_identifiers),
                    placeholders=placeholders
                )

                # Prepare the data for insertion
                rows = []
                for row in dict_list:
                    rows.append(tuple(row.get(col) for col in columns))

                # Execute the insert statements
                cursor.executemany(insert_query, rows)
                self.connection.commit()
                logging.info(f"Data successfully dumped into dummy table '{table_name}'.")
                return True

        except Exception as e:
            self.connection.rollback()
            logging.error(f"Error dumping data to dummy table: {e}")
            return False

    def move_table_to_new_database(self, table_name, new_database, new_schema):
        """
        Moves a table from one database to another.

        :param table_name: The name of the table to move.
        :param new_database: The name of the new database to move the table to.
        :param new_schema: The name of the new schema to move the table to.
        :return: bool: True if the table is moved successfully, False otherwise.
        """
        # Validate identifiers
        self.validate_identifier(table_name, "table")
        self.validate_identifier(new_database, "database")
        self.validate_identifier(new_schema, "schema")

        try:
            cursor = self.get_cursor()

            # Check if the table exists in the current schema
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
                (table_name,)
            )
            if not cursor.fetchone()[0]:
                logging.error(f"Table '{table_name}' does not exist in the current schema.")
                cursor.close()
                return False

            # Check if the new schema exists
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s)",
                (new_schema,)
            )
            if not cursor.fetchone()[0]:
                logging.error(f"Schema '{new_schema}' does not exist.")
                cursor.close()
                return False

            # Check if the new database exists
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)",
                (new_database,)
            )
            if not cursor.fetchone()[0]:
                logging.error(f"Database '{new_database}' does not exist.")
                cursor.close()
                return False

            # Move the table to the new schema
            alter_schema_query = sql.SQL("ALTER TABLE {schema}.{table} SET SCHEMA {new_schema}").format(
                schema=sql.Identifier(self.schema),
                table=sql.Identifier(table_name),
                new_schema=sql.Identifier(new_schema)
            )
            cursor.execute(alter_schema_query)
            logging.info(f"Table '{table_name}' moved to schema '{new_schema}' successfully.")

            # Move the table to the new tablespace (Note: this is tablespace, not database)
            alter_tablespace_query = sql.SQL("ALTER TABLE {schema}.{table} SET TABLESPACE {tablespace}").format(
                schema=sql.Identifier(new_schema),
                table=sql.Identifier(table_name),
                tablespace=sql.Identifier(new_database)
            )
            cursor.execute(alter_tablespace_query)
            logging.info(f"Table '{table_name}' moved to tablespace '{new_database}' successfully.")

            cursor.close()
            return True

        except Exception as e:
            logging.error(f"Error moving table to new database: {e}")
            return False

    def move_table_to_schema(self, table_name, new_schema, remove=False):
        """
        Moves a table from one schema to another.

        :param table_name: The name of the table to move.
        :param new_schema: The name of the new schema to move the table to.
        :param remove: If True, drop the table after moving (note: this doesn't make sense after a schema move).
        :return: bool: True if the table is moved successfully, False otherwise.
        """
        # Validate identifiers
        self.validate_identifier(table_name, "table")
        self.validate_identifier(new_schema, "schema")

        try:
            cursor = self.get_cursor()

            # Check if the table exists in the current schema
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s AND table_schema = %s)",
                (table_name, self.schema)
            )
            if not cursor.fetchone()[0]:
                logging.error(f"Table '{table_name}' does not exist in the current schema.")
                cursor.close()
                return False

            # Check if the new schema exists
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s)",
                (new_schema,)
            )
            if not cursor.fetchone()[0]:
                logging.error(f"Schema '{new_schema}' does not exist.")
                cursor.close()
                return False

            # Move the table to the new schema
            alter_query = sql.SQL("ALTER TABLE {schema}.{table} SET SCHEMA {new_schema}").format(
                schema=sql.Identifier(self.schema),
                table=sql.Identifier(table_name),
                new_schema=sql.Identifier(new_schema)
            )
            cursor.execute(alter_query)
            logging.info(f"Table '{table_name}' moved to schema '{new_schema}' successfully.")

            # Remove the table from the new schema (if requested)
            if remove:
                drop_query = sql.SQL("DROP TABLE {schema}.{table}").format(
                    schema=sql.Identifier(new_schema),
                    table=sql.Identifier(table_name)
                )
                cursor.execute(drop_query)
                logging.info(f"Table '{table_name}' removed from schema '{new_schema}' successfully.")

            cursor.close()
            return True

        except Exception as e:
            logging.error(f"Error moving table to new schema: {e}")
            return False

    def close(self):
        try:
            if hasattr(self, "connection") and self.connection:
                self.connection.close()
                if self.return_logging:
                    logging.info("Connection closed")
        except Exception as e:
            logging.warning(f"Error closing connection: {e}")


if __name__ == "__main__":
    pg = PostgresManager("core", "defaultdb")
    pg.connect_with_retries()
    tables = pg.get_tables()
    print(tables)
