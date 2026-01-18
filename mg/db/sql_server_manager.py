import pyodbc
import logging
import json
from datetime import datetime, date, time
import decimal

from mg.db.config import _SS_HOST, _SS_USER, _SS_PASSWORD

logging.basicConfig(level=logging.INFO)


class SQLServerManager:
    def __init__(self, database, schema, return_logging=False):
        self.database = database
        self.schema = schema
        self.connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};SERVER={_SS_HOST};Encrypt=No;"
            f"DATABASE={self.database};uid={_SS_USER};pwd={_SS_PASSWORD}"
        )
        self.connection = pyodbc.connect(self.connection_string)
        self.python_to_sql_dtypes = {
            "object": "VARCHAR(MAX)",
            "int64": "INT",
            "float64": "FLOAT",
            "datetime64[ns]": "DATETIME",
            "bool": "BIT",
        }
        self.sql_to_python_dtypes = {
            "varchar(max)": "object",
            "int": "int64",
            "float": "float64",
            "datetime": "datetime64[ns]",
            "bit": "bool",
        }
        self.cursor = self.connection.cursor()
        self.return_logging = return_logging

    def execute_query(self, query):
        """Execute a SQL query.

        Args:
            query (str): Query to execute.

        Returns:
            None
        """
        logging.info(query)
        cursor = self.cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        return results

    def get_table_primary_key(self, table):
        q = f"""
            SELECT 
                column_name
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
                ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME 
                AND KU.table_name='{table}'
            ORDER BY 
                KU.TABLE_NAME
                ,KU.ORDINAL_POSITION; 
            """
        self.cursor.execute(q)
        result = self.cursor.fetchall()
        results = []
        for row in result:
            results.append(row[0])

        if len(results) > 0:
            return results
        else:
            msg = f"Table {self.schema}.{table} does not have a primary key."
            logging.warning(msg)
            return None

    def determine_column_type(self, values):
        """
        Determines the appropriate SQL Server type for a column based on all the values.
        """
        type_map = {
            int: "INT",
            float: "FLOAT",
            bool: "BIT",
            dict: "NVARCHAR(MAX)",  # SQL Server does not have JSON type, usually stored as NVARCHAR(MAX)
            list: "NVARCHAR(MAX)",  # Similarly, lists are stored as NVARCHAR(MAX)
            datetime: "DATETIME",
            date: "DATE",
            time: "TIME",
            bytes: "VARBINARY(MAX)",
            decimal.Decimal: "DECIMAL",
        }

        encountered_types = set()
        max_str_length = 0

        for value in values:
            if value is not None:
                # Try to parse datetime strings
                if isinstance(value, str):
                    try:
                        datetime_value = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                        encountered_types.add(datetime)
                        continue
                    except ValueError:
                        pass
                    # Adjust length for apostrophes (adding 2 for each apostrophe)
                    adjusted_length = len(value) + (value.count("'") * 2)
                    max_str_length = max(max_str_length, adjusted_length)
                encountered_types.add(type(value))

        if len(encountered_types) == 0:
            return "NVARCHAR(MAX)"
        elif len(encountered_types) == 1:
            single_type = next(iter(encountered_types))
            if single_type == str:
                # Determine the appropriate length for VARCHAR
                if (
                    max_str_length > 8000
                ):  # SQL Server VARCHAR can only be up to 8000 characters
                    return "VARCHAR(MAX)"
                else:
                    return (
                        f"VARCHAR({max_str_length})"
                        if max_str_length > 0
                        else "NVARCHAR(MAX)"
                    )
            return type_map[single_type]
        else:
            if encountered_types <= {int, float}:
                return "FLOAT"
            elif encountered_types <= {str, int, float}:
                if max_str_length > 8000:
                    return "VARCHAR(MAX)"
                else:
                    return (
                        f"VARCHAR({max_str_length})"
                        if max_str_length > 0
                        else "NVARCHAR(MAX)"
                    )
            elif encountered_types <= {datetime, str}:
                return "DATETIME"
            elif encountered_types <= {date, str}:
                return "DATE"
            elif encountered_types <= {time, str}:
                return "TIME"
            else:
                return "NVARCHAR(MAX)"

    def insert_rows(
        self,
        target_table,
        columns,
        rows,
        contains_dicts=False,
        update=False,
        return_id=False,
    ):
        """Insert rows into a table.

        Args:
            target_table (str): Table to insert rows into.

        Returns:
            None
        """
        columns = list(columns)
        columns_str = ", ".join([f"[{col}]" for col in columns])
        values_placeholders = ", ".join(["?" for _ in columns])

        if contains_dicts:
            new_rows = []
            for row in rows:
                new_row = {}
                for col in columns:
                    s = row.get(col)
                    if isinstance(s, dict):
                        s = json.dumps(s)
                    if "'" in str(s):
                        s = str(s).replace("'", "''")
                    if "" == str(s):
                        s = None
                    new_row[col] = s
                new_rows.append(new_row)
            rows = new_rows

        if update:
            # Building the MERGE statement for upsert
            pk = self.get_table_primary_key(target_table)
            pk_str = " AND ".join(
                [f"target.[{pk_col}] = source.[{pk_col}]" for pk_col in pk]
            )
            update_str = ", ".join(
                [f"target.[{col}] = source.[{col}]" for col in columns if col not in pk]
            )

            merge_sql = f"""
            MERGE INTO [{target_table}] AS target
            USING (VALUES ({values_placeholders})) AS source ({columns_str})
            ON {pk_str}
            WHEN MATCHED THEN
                UPDATE SET {update_str}
            WHEN NOT MATCHED THEN
                INSERT ({columns_str})
                VALUES ({values_placeholders});
            """
            params = [tuple(row[col] for col in columns) for row in rows]
        else:
            # Building the simple INSERT statement
            insert_sql = f"INSERT INTO [{target_table}] ({columns_str}) VALUES ({values_placeholders})"
            params = [tuple(row[col] for col in columns) for row in rows]

        if self.return_logging:
            logging.info(merge_sql if update else insert_sql)

        try:
            cursor = self.connection.cursor()
            if update:
                for param in params:
                    cursor.execute(
                        merge_sql, param + param
                    )  # Need to pass the params twice for MERGE statement
            else:
                for param in params:
                    cursor.execute(insert_sql, param)
            self.connection.commit()
            logging.info(f"Rows inserted successfully into {target_table}")
            return True
        except Exception as e:
            logging.warning(e)
            return False

    def execute(self, q):
        logging.info(q)
        results = []
        try:
            q = "SET NOCOUNT ON; " + q
            logging.info(q)
            self.cursor.execute(q)
            if self.cursor.description:
                field_names = [i[0] for i in self.cursor.description]
                results = [
                    dict(zip(field_names, row)) for row in self.cursor.fetchall()
                ]
        except Exception as e:
            logging.warning("Error executing query: %s", e)
            logging.warning("Exception details:", exc_info=True)
        return results

    def create_table(self, dict_list, primary_keys, table_name, delete=False):
        """
        Creates a SQL Server table from a list of dictionaries.

        :param dict_list: List of dictionaries containing the data.
        :param primary_keys: List of keys to be used as the primary keys.
        :param table_name: The name of the table to be created.
        :param delete: Flag to indicate whether to delete the table if it exists.
        """
        if not dict_list:
            raise ValueError("The dictionary list is empty")

        if not isinstance(primary_keys, list):
            raise ValueError("Primary keys should be provided as a list")

        # Extract all column names and their values from the list of dictionaries
        columns = {key: [] for key in dict_list[0].keys()}
        for dictionary in dict_list:
            for key, value in dictionary.items():
                columns[key].append(value)

        # Determine the SQL Server data type for each column
        columns = {
            key: self.determine_column_type(values) for key, values in columns.items()
        }

        # Create the SQL statement for table creation
        fields = ", ".join(f'"{col}" {col_type}' for col, col_type in columns.items())
        primary_keys_str = ", ".join(f'"{pk}"' for pk in primary_keys)
        create_table_query = (
            f'CREATE TABLE "{table_name}" ({fields}, PRIMARY KEY ({primary_keys_str}))'
        )

        try:
            # Check if table already exists
            table_exists_query = f"SELECT CASE WHEN OBJECT_ID('{table_name}', 'U') IS NOT NULL THEN 1 ELSE 0 END AS 'exists'"
            table_exists = self.execute_query(table_exists_query)
            if table_exists[0].get("exists") == 1:
                logging.info(f"Table '{table_name}' already exists.")
                if delete:
                    # Drop the table if it already exists
                    self.execute(f"DROP TABLE {table_name}")
                    logging.info(f"Table '{table_name}' dropped successfully.")
                    # Execute the create table query
                    self.execute(create_table_query)
                    logging.info(f"Table '{table_name}' created successfully.")
                    return True
                else:
                    return False
            else:
                # Execute the create table query
                self.execute(create_table_query)
                logging.info(f"Table '{table_name}' created successfully.")
                return True
        except Exception as e:
            self.connection.rollback()
            logging.error(f"Error creating table: {e}")
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
                f"SELECT CASE WHEN OBJECT_ID('{table_name}', 'U') IS NOT NULL THEN 1 ELSE 0 END AS 'exists'"
            )
            return table_exists[0].get("exists")
        except Exception as e:
            logging.error(f"Error checking if table exists: {e}")
            return False

    def close(self):
        try:
            self.connection.close()
            logging.info("Connection closed")
        except:
            logging.warning("Tried to close a connection that was already closed")


if __name__ == "__main__":
    ssms = SQLServerManager(database="cfb", schema="dbo", return_logging=True)
    # results = ssms.execute_query("SELECT top(2) * FROM CFB_TEAM_MAPPING")
    # ssms.insert_rows(target_table="CFB_TEAM_MAPPING_clone", columns=results[0].keys(), rows=results, contains_dicts=True)
    # ssms.close_connection()

    dict_list = [
        {
            "id": 1,
            "name": "Alice",
            "age": 30.1,
            "created_at": "2024-05-23 12:00:00",
            "desc": "test",
        },
        {
            "id": 3,
            "name": "AF",
            "age": 25.2,
            "created_at": "2024-05-23 12:00:00",
            "desc": "test",
        },
    ]
    ssms.create_table(
        dict_list, primary_keys=["id"], table_name="test_table", delete=True
    )
    ssms.insert_rows(
        target_table="test_table",
        columns=dict_list[0].keys(),
        rows=dict_list,
        contains_dicts=True,
        update=True,
    )
    ssms.close()
    # data = ssms.execute("SELECT * FROM DRAFTKINGS_TEAMS")
    # logging.info(data)
