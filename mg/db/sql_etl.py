import logging
import time

from mg.db.postgres_manager import PostgresManager
from mg.db.sql_server_manager import SQLServerManager
from mg.db.queries import queries
from mg.utils.utils import format_seconds_to_hhmmss

logging.basicConfig(level=logging.INFO)

CHUNK_SIZE = 10000


class SqlETL:
    def __init__(
        self,
        source_sql,
        target_sql,
        table_name,
        chunk_inserts=False,
        chunk_size=CHUNK_SIZE,
    ):
        if source_sql["sql"] not in ["postgresql", "sql_server"]:
            raise ValueError("Invalid source sql type")
        if target_sql["sql"] not in ["postgresql", "sql_server"]:
            raise ValueError("Invalid target sql type")
        if source_sql["sql"] == target_sql["sql"] == "postgresql":
            self.source_sql = PostgresManager(
                host=source_sql["host"],
                database=source_sql["database"],
                schema=source_sql["schema"],
            )
            # Create target schema if it doesn't exist before initializing target connection
            temp_target = PostgresManager(
                host=target_sql["host"],
                database=target_sql["database"],
                schema="core",  # Use core schema temporarily
            )
            self._ensure_target_schema_exists(temp_target, target_sql["schema"])
            temp_target.close()
            
            self.target_sql = PostgresManager(
                host=target_sql["host"],
                database=target_sql["database"],
                schema=target_sql["schema"],
            )
        elif source_sql["sql"] == "postgresql":
            self.source_sql = PostgresManager(
                host=source_sql["host"],
                database=source_sql["database"],
                schema=source_sql["schema"],
            )
            self.target_sql = SQLServerManager(
                database=target_sql["database"], schema=target_sql["schema"]
            )
        else:
            self.source_sql = SQLServerManager(
                database=source_sql["database"], schema=source_sql["schema"]
            )
            # Create target schema if it doesn't exist before initializing target connection
            temp_target = PostgresManager(
                host=target_sql["host"],
                database=target_sql["database"],
                schema="core",  # Use core schema temporarily
            )
            self._ensure_target_schema_exists(temp_target, target_sql["schema"])
            temp_target.close()
            
            self.target_sql = PostgresManager(
                host=target_sql["host"],
                database=target_sql["database"],
                schema=target_sql["schema"],
            )
        self.table_name = table_name
        self.source_query = queries[source_sql["sql"]]
        self.target_query = queries[target_sql["sql"]]
        self.source_sql_type = source_sql["sql"]
        self.target_sql_type = target_sql["sql"]
        self.chunk_inserts = chunk_inserts
        self.chunk_size = chunk_size

    def map_col_types(self, column_type):
        if self.source_sql_type == "postgresql":
            # Define the mapping dictionary
            type_mapping = {
                "bigint": "BIGINT",
                "bigserial": "BIGINT IDENTITY",
                "bit": "BIT",
                "boolean": "BIT",
                "bytea": "VARBINARY(MAX)",
                "character varying": "VARCHAR(MAX)",
                "varchar": "VARCHAR(MAX)",
                "character": "CHAR",
                "char": "CHAR",
                "date": "DATE",
                "double precision": "FLOAT",
                "real": "REAL",
                "integer": "INT",
                "int": "INT",
                "serial": "INT IDENTITY",
                "money": "MONEY",
                "numeric": "DECIMAL",
                "decimal": "DECIMAL",
                "smallint": "SMALLINT",
                "smallserial": "SMALLINT IDENTITY",
                "text": "TEXT",
                "time": "TIME",
                "timestamp": "DATETIME",
                "timestamptz": "DATETIMEOFFSET",
                "uuid": "UNIQUEIDENTIFIER",
                "xml": "XML",
                # Add more mappings as needed
            }
        else:
            # Define the mapping dictionary
            type_mapping = {
                "bigint": "BIGINT",
                "bigserial": "BIGINT IDENTITY",
                "bit": "BIT",
                "boolean": "BIT",
                "bytea": "VARBINARY(MAX)",
                "character varying": "VARCHAR(MAX)",
                "varchar": "VARCHAR(MAX)",
                "character": "CHAR",
                "char": "CHAR",
                "date": "DATE",
                "double precision": "FLOAT",
                "real": "REAL",
                "integer": "INT",
                "int": "INT",
                "serial": "INT IDENTITY",
                "money": "MONEY",
                "numeric": "DECIMAL",
                "decimal": "DECIMAL",
                "smallint": "SMALLINT",
                "smallserial": "SMALLINT IDENTITY",
                "text": "TEXT",
                "time": "TIME",
                "timestamp": "DATETIME",
                "timestamptz": "DATETIMEOFFSET",
                "uuid": "UNIQUEIDENTIFIER",
                "xml": "XML",
                # Add more mappings as needed
            }

        return type_mapping.get(column_type.lower(), "VARCHAR(MAX)")

    def _ensure_target_schema_exists(self, temp_connection, schema_name):
        """Ensure target schema exists using a temporary connection"""
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}')"
        result = temp_connection.execute_query(query)
        
        if not result[0].get("exists", False):
            create_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
            temp_connection.execute_query(create_query)
            logging.info(f"Created schema '{schema_name}' in target database")
        else:
            logging.info(f"Schema '{schema_name}' already exists in target database")

    def _get_source_table_schema(self):
        # Get source table schema
        q = self.source_query["get_source_table_schema"].format(
            table_name=self.table_name
        )
        self.source_columns = self.source_sql.execute(q=q)

        # Get primary key column
        self.source_primary_key = self.source_sql.get_table_primary_key(self.table_name)

    def _get_source_data(self):
        q = "SELECT * FROM {table_name}".format(table_name=self.table_name)
        self.data = self.source_sql.execute(q=q)
        logging.info("Source data retrieved")
        logging.info("Number of rows: {n_rows}".format(n_rows=len(self.data)))

    def _etl_target_table(self):

        start_time = time.time()
        # Create target table
        table_name = (
            self.table_name.lower()
            if self.source_sql_type == "sql_server"
            else self.table_name
        )
        self.target_sql.create_table(
            self.data, self.source_primary_key, table_name, delete=False
        )
        logging.info(
            "Added table {table_name} to target database".format(table_name=table_name)
        )
        if len(self.data) > self.chunk_size or self.chunk_inserts:
            logging.info(
                "Inserting data in chunks of {chunk_size}".format(chunk_size=CHUNK_SIZE)
            )
            iterations = len(self.data) // CHUNK_SIZE
            logging.info("Total iterations: {iterations}".format(iterations=iterations))
            for i in range(0, len(self.data), CHUNK_SIZE):
                self.target_sql.insert_rows(
                    table_name,
                    self.data[0].keys(),
                    self.data[i : i + CHUNK_SIZE],
                    contains_dicts=True,
                    update=True,
                )
                logging.info(
                    "Inserted {n_rows} rows into {table_name}".format(
                        n_rows=CHUNK_SIZE, table_name=table_name
                    )
                )
        else:
            logging.info(
                "Inserting data into {table_name}".format(table_name=table_name)
            )
            self.target_sql.insert_rows(
                table_name,
                self.data[0].keys(),
                self.data,
                contains_dicts=True,
                update=True,
            )
        logging.info(
            "Inserted {n_rows} rows into {table_name}".format(
                n_rows=len(self.data), table_name=table_name
            )
        )
        end_time = time.time()
        logging.info(
            "Time elapsed: {time_elapsed}".format(
                time_elapsed=format_seconds_to_hhmmss(end_time - start_time)
            )
        )

    def _close_connections(self):
        self.source_sql.close()
        self.target_sql.close()

    def run(self):
        try:
            self._get_source_table_schema()
            self._get_source_data()
            self._etl_target_table()
            logging.info("ETL process complete")
        except Exception as e:
            logging.error(e)
            logging.info("ETL process failed")
        finally:
            self._close_connections()


if __name__ == "__main__":

    # First ensure the schema exists
    temp_pgm = PostgresManager(
        host="digital_ocean", database="cfb", schema="core", return_logging=True
    )
    schema_name = "underdog"
    query = f"SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}')"
    result = temp_pgm.execute_query(query)
    
    if not result[0].get("exists", False):
        create_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        temp_pgm.execute_query(create_query)
        logging.info(f"Created schema '{schema_name}' in database")
    else:
        logging.info(f"Schema '{schema_name}' already exists in database")
    
    temp_pgm.close()

    # Now connect to the schema and get tables
    pgm = PostgresManager(
        host="digital_ocean", database="defaultdb", schema="underdog", return_logging=True
    )
    tables = pgm.get_tables()
    move_tables = []
    for table in tables:
        logging.info(table)
        source_sql = {
            "sql": "postgresql",
            "host": "digital_ocean",
            "database": "defaultdb",
            "schema": "underdog",
        }
        target_sql = {
            "sql": "postgresql",
            "host": "digital_ocean",
            "database": "cfb",
            "schema": "underdog",
        }
        etl = SqlETL(source_sql, target_sql, table)
        test = etl.run()
