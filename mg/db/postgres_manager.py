import os
import psycopg2
import logging
import json
from datetime import datetime, date, time
import socket
from time import sleep

from mg.db.config import POSTGRES_HOSTS

logging.basicConfig(level=logging.INFO)


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
        cursor = self.get_cursor()
        q = f"""
            SELECT column_name
            FROM information_schema.table_constraints
            JOIN information_schema.key_column_usage
                    USING (constraint_catalog, constraint_schema, constraint_name,
                            table_catalog, table_schema, table_name)
            WHERE constraint_type = 'PRIMARY KEY'
            AND (table_schema, table_name) = ('{self.schema}', '{table}')
            ORDER BY ordinal_position;"""
        try:
            cursor.execute(q)
            result = cursor.fetchall()
            results = [row[0] for row in result]

            if len(results) > 0:
                return results

            # Check if table exists and user has access via pg_catalog (more reliable)
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = '{self.schema}' AND c.relname = '{table}'
                );""")
            table_exists_pg = cursor.fetchone()[0]

            # Check if table is visible in information_schema (permission-dependent)
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = '{self.schema}' AND table_name = '{table}'
                );""")
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
        """Insert rows into a table.

        Args:
            target_table (str): Table to insert rows into.
            columns (list): List of column names.
            rows (list): List of rows to insert.
            contains_dicts (bool): Whether the rows contain dictionaries.
            update (bool): Whether to update existing rows.
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
                    q = f"INSERT INTO {target_table} ("
                    for col in columns:
                        q += f'"{col.lower()}", '
                    q = q[:-2]
                    q += ") VALUES "

                    if contains_dicts:
                        new_rows = []
                        for row in rows:
                            new_row = ()
                            for col in columns:
                                s = row.get(col)
                                if isinstance(s, dict) or isinstance(s, list):
                                    s = json.dumps(s)
                                if "'" in str(s):
                                    s = str(s).replace("'", "''")
                                if "" == str(s):
                                    s = None
                                new_row = new_row + (s,)
                            new_rows.append(new_row)
                        rows = new_rows
                        params = ""
                        for row in rows:
                            record = ""
                            for data in row:
                                if data is None:
                                    record += "NULL, "
                                else:
                                    record += "'" + str(data) + "', "
                            record = record[:-2]
                            params += "(" + record + "), "
                        params = params[:-2]
                        q += params
                    else:
                        record = ""
                        for data in rows:
                            record += "'" + str(rows.get(data)) + "', "
                        record = record[:-2]
                        q += "(" + record + ")"

                    if update:
                        if pk is None:
                            error_msg = f"Cannot perform upsert on table {target_table} - no primary key defined"
                            logging.error(error_msg)
                            return (False, error_msg) if return_error_msg else False
                        elif len(pk) == 1 and len(columns) == 1:
                            q += f" ON CONFLICT ({pk[0]}) DO UPDATE SET {columns[0]} = EXCLUDED.{columns[0]}"
                        elif len(pk) > 1:
                            # Handle multiple primary keys
                            pk_str = ", ".join(f'"{p}"' for p in pk)
                            set_q = ", ".join(
                                [
                                    f'"{col.lower()}" = EXCLUDED."{col.lower()}"'
                                    for col in columns
                                    if col not in pk
                                ]
                            )
                            q += f" ON CONFLICT ({pk_str}) DO UPDATE SET {set_q}"
                        else:
                            set_q = "ROW ("
                            q += f" ON CONFLICT ({', '.join(pk)}) DO UPDATE SET ("
                            for col in columns:
                                if col in pk:
                                    continue
                                q += f'"{col.lower()}", '
                                set_q += f"EXCLUDED.{col.lower()}, "
                            set_q = set_q[:-2]
                            set_q += ")"
                            q = q[:-2]
                            q += f") = {set_q}"

                    if self.return_logging:
                        logging.info(q)
                    cursor.execute(q)
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
            # Handle connection issues
            error_msg = f"Database connection error: {e}"
            logging.error(error_msg)
            # You might want to attempt reconnection here
            self.connect_with_retries()
            return (False, error_msg) if return_error_msg else False
        except psycopg2.errors.NumericValueOutOfRange as e:
            # Handle numeric overflow/underflow errors (e.g., value too large for smallint)
            error_msg = self._format_sql_error("SQL Data Type Error (Numeric Out of Range)", e, q if 'q' in locals() else None)
            logging.error(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.errors.StringDataRightTruncation as e:
            # Handle string too long for column
            error_msg = self._format_sql_error("SQL Data Type Error (String Too Long)", e, q if 'q' in locals() else None)
            logging.error(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.DataError as e:
            # Handle other data type errors
            error_msg = self._format_sql_error("SQL Data Type Error", e, q if 'q' in locals() else None)
            logging.error(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.IntegrityError as e:
            # Handle other integrity constraint violations not caught above
            error_msg = self._format_sql_error("SQL Integrity Constraint Violation", e, q if 'q' in locals() else None)
            logging.error(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.ProgrammingError as e:
            # Handle SQL syntax or programming errors
            error_msg = self._format_sql_error("SQL Programming Error", e, q if 'q' in locals() else None)
            logging.error(error_msg)
            return (False, error_msg) if return_error_msg else False
        except psycopg2.DatabaseError as e:
            # Handle other database-related errors
            error_msg = self._format_sql_error("SQL Database Error", e, q if 'q' in locals() else None)
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
                self.connection.autocommit = value
            except Exception as e:
                logging.warning(f"Error setting autocommit to {value}: {e}")

    def execute(self, q, params=None, raise_exc=False):
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
        trigger_function_check = f"""
        CREATE OR REPLACE FUNCTION {self.schema}.update_updated_at() RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        try:
            self.execute(trigger_function_check)
            self.connection.commit()
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

        # Extract all column names and their values from the list of dictionaries
        columns = self.get_all_columns(dict_list)
        columns = {key: [] for key in columns}
        for dictionary in dict_list:
            for key, value in dictionary.items():
                columns[key].append(value)

        # Determine the PostgreSQL data type for each column
        columns = {
            key: self.determine_column_type(values) for key, values in columns.items()
        }

        # Create the SQL statement for table creation
        fields = []

        # Add identity column if no primary keys provided
        if not primary_keys:
            fields.append('"sql_id" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY')
            # Add other fields
            fields.extend(f'"{col}" {col_type}' for col, col_type in columns.items())
            fields_str = ", ".join(fields)
            create_table_query = (
                f'CREATE TABLE IF NOT EXISTS "{table_name}" ({fields_str})'
            )
        else:
            # make primary keys the first few columns
            priority_found = [item for item in columns if item in primary_keys]
            remaining = [item for item in columns if item not in primary_keys]
            ordered_columns = priority_found + remaining

            # Add fields in order: primary keys first, then remaining columns
            fields.extend(f'"{col}" {columns[col]}' for col in ordered_columns)

            fields_str = ", ".join(fields)
            primary_keys_str = ", ".join(f'"{pk}"' for pk in primary_keys)
            create_table_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({fields_str}, PRIMARY KEY ({primary_keys_str}))'

        try:
            # Check if table already exists
            table_exists = self.execute_query(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = '{self.schema}')"
            )
            if table_exists[0].get("exists"):
                logging.info(f"Table '{table_name}' already exists.")
                if delete:
                    # Drop the table if it already exists
                    self.execute(f"DROP TABLE {table_name}")
                    logging.info(f"Table '{table_name}' dropped successfully.")
                    # Execute the create table query
                    self.execute(create_table_query)
                    logging.info(f"Table '{table_name}' created successfully.")
                    self.ensure_update_trigger_exists()
                    timestamp_q = f"""
                        ALTER TABLE {table_name} ADD created_at timestamp DEFAULT timezone('US/Central'::text, CURRENT_TIMESTAMP) NULL;
                        ALTER TABLE {table_name}  ADD updated_at timestamp NULL;
                    """
                    trigger_q = f"""
                        CREATE TRIGGER update_updated_at BEFORE
                        UPDATE ON {table_name} FOR EACH ROW EXECUTE FUNCTION core.update_updated_at();
                    """
                    self.execute(timestamp_q)
                    self.execute(trigger_q)
                    logging.info(f"Timestamps added to {table_name}; trigger created.")
                    return True
                else:
                    return False
            else:
                # Execute the create table query
                self.execute(create_table_query)
                logging.info(f"Table '{table_name}' created successfully.")
                self.ensure_update_trigger_exists()
                timestamp_q = f"""
                    ALTER TABLE {table_name} ADD created_at timestamp DEFAULT timezone('US/Central'::text, CURRENT_TIMESTAMP) NULL;
                    ALTER TABLE {table_name}  ADD updated_at timestamp NULL;
                """
                trigger_q = f"""
                    CREATE TRIGGER update_updated_at BEFORE
                    UPDATE ON {table_name} FOR EACH ROW EXECUTE FUNCTION core.update_updated_at();
                """
                self.execute(timestamp_q)
                self.execute(trigger_q)
                logging.info(f"Timestamps added to {table_name}; trigger created.")
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
        try:
            # Check if table already exists
            table_exists = self.execute_query(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = '{self.schema}')"
            )
            return table_exists[0].get("exists")
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
        query = f"""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{self.schema}'
        ORDER BY table_name, ordinal_position;
        """
        try:
            cursor = self.get_cursor()
            cursor.execute(query)
            rows = cursor.fetchall()

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

        # Extract all column names and their values from the list of dictionaries
        columns = self.get_all_columns(dict_list)
        columns = sorted(columns)
        columns_data = {key: [] for key in columns}
        for dictionary in dict_list:
            for key, value in dictionary.items():
                columns_data[key].append(value)

        # Determine the PostgreSQL data type for each column
        columns_data = {
            key: self.determine_column_type(values)
            for key, values in columns_data.items()
        }

        # Create the SQL statement for table creation without primary keys
        fields = ", ".join(
            ['"' + col + '" ' + col_type for col, col_type in columns_data.items()]
        )
        create_table_query = (
            'CREATE TABLE IF NOT EXISTS "' + table_name + '" (' + fields + ")"
        )

        try:
            # Ensure a new cursor is used to prevent any issues with closed cursors
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_query)
                logging.info("Dummy table '" + table_name + "' created successfully.")

                # Prepare the column names for the insert query
                column_names = ", ".join(['"' + col + '"' for col in columns])

                # Prepare the values placeholder for the insert query
                values_placeholder = ", ".join(["%s" for _ in columns])

                # Insert query construction
                insert_query = (
                    'INSERT INTO "'
                    + table_name
                    + '" ('
                    + column_names
                    + ") VALUES ("
                    + values_placeholder
                    + ")"
                )

                # Prepare the data for insertion
                rows = []
                for row in dict_list:
                    rows.append(tuple(row[col] for col in columns))

                # Execute the insert statements
                cursor.executemany(insert_query, rows)
                self.connection.commit()
                logging.info(
                    "Data successfully dumped into dummy table '" + table_name + "'."
                )
                return True

        except Exception as e:
            self.connection.rollback()
            logging.error("Error dumping data to dummy table: " + str(e))
            return False

    def move_table_to_new_database(self, table_name, new_database, new_schema):
        """
        Moves a table from one database to another.

        :param table_name: The name of the table to move.
        :param new_database: The name of the new database to move the table to.
        :param new_schema: The name of the new schema to move the table to.
        :return: bool: True if the table is moved successfully, False otherwise.
        """
        try:
            # Check if the table exists in the current schema
            table_exists = self.execute_query(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')"
            )
            if not table_exists[0].get("exists"):
                logging.error(
                    f"Table '{table_name}' does not exist in the current schema."
                )
                return False

            # Check if the new schema exists
            schema_exists = self.execute_query(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = '{new_schema}')"
            )
            if not schema_exists[0].get("exists"):
                logging.error(f"Schema '{new_schema}' does not exist.")
                return False

            # Check if the new database exists
            database_exists = self.execute_query(
                f"SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = '{new_database}')"
            )
            if not database_exists[0].get("exists"):
                logging.error(f"Database '{new_database}' does not exist.")
                return False

            # Move the table to the new schema
            self.execute(f"ALTER TABLE {table_name} SET SCHEMA {new_schema}")
            logging.info(
                f"Table '{table_name}' moved to schema '{new_schema}' successfully."
            )

            # Move the table to the new database
            self.execute(f"ALTER TABLE {table_name} SET TABLESPACE {new_database}")
            logging.info(
                f"Table '{table_name}' moved to database '{new_database}' successfully."
            )
            return True

        except Exception as e:
            logging.error(f"Error moving table to new database: {e}")
            return False

    def move_table_to_schema(self, table_name, new_schema, remove=False):
        """
        Moves a table from one schema to another.

        :param table_name: The name of the table to move.
        :param new_schema: The name of the new schema to move the table to.
        :return: bool: True if the table is moved successfully, False otherwise.
        """
        try:
            # Check if the table exists in the current schema
            table_exists = self.execute_query(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')"
            )
            if not table_exists[0].get("exists"):
                logging.error(
                    f"Table '{table_name}' does not exist in the current schema."
                )
                return False

            # Check if the new schema exists
            schema_exists = self.execute_query(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = '{new_schema}')"
            )
            if not schema_exists[0].get("exists"):
                logging.error(f"Schema '{new_schema}' does not exist.")
                return False

            # Move the table to the new schema
            self.execute(f"ALTER TABLE {table_name} SET SCHEMA {new_schema}")
            logging.info(
                f"Table '{table_name}' moved to schema '{new_schema}' successfully."
            )

            # Remove the table from the current schema
            if remove:
                self.execute(f"DROP TABLE {table_name}")
                logging.info(
                    f"Table '{table_name}' removed from schema '{self.schema}' successfully."
                )
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
