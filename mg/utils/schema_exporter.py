import os
import json
import csv
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from mg.db.postgres_manager import PostgresManager
from mg.db.config import POSTGRES_HOSTS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgreSQLSchemaExporter:
    """
    Export PostgreSQL database schema (DDL) and sample data to organized file structure
    """

    def __init__(
        self,
        host_key: str,
        database_key: str,
        schema_key: str,
        output_base_path: str = "sql_schema",
        sample_rows: int = 100,
    ):
        """
        Initialize the schema exporter

        Args:
            host_key: Host configuration key from POSTGRES_HOSTS
            database_key: Database configuration key
            schema_key: Schema configuration key
            output_base_path: Base path for output files
            sample_rows: Number of sample rows to export per table
        """
        self.host_key = host_key
        self.database_key = database_key
        self.schema_key = schema_key
        self.output_base_path = Path(output_base_path)
        self.sample_rows = sample_rows

        # Initialize database manager
        self.pg_manager = PostgresManager(host_key, database_key, schema_key)

        # Create output directories
        self._create_output_directories()

    def _create_output_directories(self):
        """Create necessary output directories"""
        self.schema_path = (
            self.output_base_path / f"{self.host_key}_{self.database_key}" / "schemas"
        )
        self.data_path = (
            self.output_base_path
            / f"{self.host_key}_{self.database_key}"
            / "sample_data"
        )

        self.schema_path.mkdir(parents=True, exist_ok=True)
        self.data_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Created directories: {self.schema_path}, {self.data_path}")

    def get_all_schemas(self) -> List[str]:
        """Get all schema names in the database"""
        query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name;
        """

        result = self.pg_manager.execute(query)
        return [row["schema_name"] for row in result]

    def get_tables_in_schema(self, schema_name: str) -> List[Dict]:
        """Get all tables in a specific schema"""
        query = """
        SELECT 
            table_name,
            table_type,
            table_schema
        FROM information_schema.tables 
        WHERE table_schema = %s
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """

        return self.pg_manager.execute(query, (schema_name,))

    def get_table_ddl(self, schema_name: str, table_name: str) -> str:
        """Generate CREATE TABLE DDL for a specific table"""
        # Get table columns
        columns_query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            column_default,
            is_nullable,
            ordinal_position
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
        """

        columns = self.pg_manager.execute(columns_query, (schema_name, table_name))

        # Get constraints
        constraints_query = """
        SELECT 
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
        LEFT JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
        WHERE tc.table_schema = %s AND tc.table_name = %s;
        """

        constraints = self.pg_manager.execute(
            constraints_query, (schema_name, table_name)
        )

        # Build DDL
        ddl_lines = [f"CREATE TABLE {schema_name}.{table_name} ("]

        # Add columns
        column_lines = []
        for col in columns:
            line = f"    {col['column_name']} {col['data_type']}"

            if col["character_maximum_length"]:
                line += f"({col['character_maximum_length']})"

            if col["is_nullable"] == "NO":
                line += " NOT NULL"

            if col["column_default"]:
                line += f" DEFAULT {col['column_default']}"

            column_lines.append(line)

        ddl_lines.extend([line + "," for line in column_lines[:-1]])
        ddl_lines.append(column_lines[-1])

        # Add constraints
        for constraint in constraints:
            if constraint["constraint_type"] == "PRIMARY KEY":
                ddl_lines.append(
                    f",    CONSTRAINT {constraint['constraint_name']} PRIMARY KEY ({constraint['column_name']})"
                )
            elif constraint["constraint_type"] == "FOREIGN KEY":
                ddl_lines.append(
                    f",    CONSTRAINT {constraint['constraint_name']} FOREIGN KEY ({constraint['column_name']}) REFERENCES {constraint['foreign_table_name']}({constraint['foreign_column_name']})"
                )
            elif constraint["constraint_type"] == "UNIQUE":
                ddl_lines.append(
                    f",    CONSTRAINT {constraint['constraint_name']} UNIQUE ({constraint['column_name']})"
                )

        ddl_lines.append(");")

        # Add triggers for this table
        triggers = self.get_table_triggers(schema_name, table_name)
        if triggers:
            ddl_lines.append("")
            ddl_lines.append(f"-- Triggers for table {table_name}")
            for trigger in triggers:
                ddl_lines.append(
                    f"-- Trigger: {trigger['trigger_name']} ({trigger['timing']} {trigger['events']})"
                )
                if trigger["trigger_definition"]:
                    ddl_lines.append(trigger["trigger_definition"])
                ddl_lines.append("")

        return "\n".join(ddl_lines)

    def get_view_ddl(
        self, schema_name: str, view_name: str, view_definition: str
    ) -> str:
        """Generate CREATE VIEW DDL"""
        return f"CREATE VIEW {schema_name}.{view_name} AS\n{view_definition};"

    def get_stored_procedures(self, schema_name: str) -> List[Dict]:
        """Get stored procedures/functions in schema with actual source code"""
        query = """
        SELECT 
            p.proname as routine_name,
            pg_get_functiondef(p.oid) as routine_definition,
            CASE 
                WHEN p.prokind = 'f' THEN 'FUNCTION'
                WHEN p.prokind = 'p' THEN 'PROCEDURE'
                ELSE 'FUNCTION'
            END as routine_type,
            pg_catalog.format_type(p.prorettype, NULL) as return_type,
            p.prosrc as source_code
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = %s
        AND p.prokind IN ('f', 'p')
        ORDER BY p.proname;
        """

        return self.pg_manager.execute(query, (schema_name,))

    def get_triggers(self, schema_name: str) -> List[Dict]:
        """Get triggers in schema with actual function definitions"""
        query = """
        SELECT 
            t.tgname as trigger_name,
            pg_get_triggerdef(t.oid) as trigger_definition,
            c.relname as table_name,
            p.proname as function_name,
            pg_get_functiondef(p.oid) as function_definition,
            CASE t.tgtype & 66
                WHEN 2 THEN 'BEFORE'
                WHEN 64 THEN 'AFTER'
                WHEN 66 THEN 'INSTEAD OF'
            END as timing,
            CASE t.tgtype & 28
                WHEN 4 THEN 'INSERT'
                WHEN 8 THEN 'DELETE' 
                WHEN 16 THEN 'UPDATE'
                WHEN 12 THEN 'INSERT OR DELETE'
                WHEN 20 THEN 'INSERT OR UPDATE'
                WHEN 24 THEN 'DELETE OR UPDATE'
                WHEN 28 THEN 'INSERT OR DELETE OR UPDATE'
            END as events
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_proc p ON t.tgfoid = p.oid
        WHERE n.nspname = %s
        AND NOT t.tgisinternal
        ORDER BY c.relname, t.tgname;
        """

        return self.pg_manager.execute(query, (schema_name,))

    def get_table_triggers(self, schema_name: str, table_name: str) -> List[Dict]:
        """Get triggers for a specific table"""
        query = """
        SELECT 
            t.tgname as trigger_name,
            pg_get_triggerdef(t.oid) as trigger_definition,
            CASE t.tgtype & 66
                WHEN 2 THEN 'BEFORE'
                WHEN 64 THEN 'AFTER'
                WHEN 66 THEN 'INSTEAD OF'
            END as timing,
            CASE t.tgtype & 28
                WHEN 4 THEN 'INSERT'
                WHEN 8 THEN 'DELETE' 
                WHEN 16 THEN 'UPDATE'
                WHEN 12 THEN 'INSERT OR DELETE'
                WHEN 20 THEN 'INSERT OR UPDATE'
                WHEN 24 THEN 'DELETE OR UPDATE'
                WHEN 28 THEN 'INSERT OR DELETE OR UPDATE'
            END as events
        FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = %s
        AND c.relname = %s
        AND NOT t.tgisinternal
        ORDER BY t.tgname;
        """

        return self.pg_manager.execute(query, (schema_name, table_name))

    def get_views_in_schema(self, schema_name: str) -> List[Dict]:
        """Get all views in a specific schema with actual view definitions"""
        query = """
        SELECT 
            c.relname as view_name,
            n.nspname as view_schema,
            pg_get_viewdef(c.oid) as view_definition
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'v'
        AND n.nspname = %s
        ORDER BY c.relname;
        """

        return self.pg_manager.execute(query, (schema_name,))

    def export_schema_ddl(self, schema_name: str):
        """Export all DDL for a specific schema"""
        logger.info(f"Exporting DDL for schema: {schema_name}")

        schema_dir = self.schema_path / schema_name
        schema_dir.mkdir(exist_ok=True)

        # Export tables
        tables = self.get_tables_in_schema(schema_name)
        if tables:
            tables_file = schema_dir / "tables.sql"
            with open(tables_file, "w", encoding="utf-8") as f:
                f.write(f"-- Tables in schema: {schema_name}\n")
                f.write(f"-- Generated on: {datetime.now()}\n\n")

                for table in tables:
                    f.write(f"-- Table: {table['table_name']}\n")
                    ddl = self.get_table_ddl(schema_name, table["table_name"])
                    f.write(ddl)
                    f.write("\n\n")

            logger.info(f"Exported {len(tables)} tables to {tables_file}")

        # Export views
        views = self.get_views_in_schema(schema_name)
        if views:
            views_file = schema_dir / "views.sql"
            with open(views_file, "w", encoding="utf-8") as f:
                f.write(f"-- Views in schema: {schema_name}\n")
                f.write(f"-- Generated on: {datetime.now()}\n\n")

                for view in views:
                    f.write(f"-- View: {view['view_name']}\n")
                    ddl = self.get_view_ddl(
                        schema_name, view["view_name"], view["view_definition"]
                    )
                    f.write(ddl)
                    f.write("\n\n")

            logger.info(f"Exported {len(views)} views to {views_file}")

        # Export stored procedures
        procedures = self.get_stored_procedures(schema_name)
        if procedures:
            procedures_file = schema_dir / "procedures.sql"
            with open(procedures_file, "w", encoding="utf-8") as f:
                f.write(f"-- Stored Procedures/Functions in schema: {schema_name}\n")
                f.write(f"-- Generated on: {datetime.now()}\n\n")

                for proc in procedures:
                    f.write(f"-- {proc['routine_type']}: {proc['routine_name']}\n")
                    f.write(f"-- Return Type: {proc['return_type']}\n")
                    if proc["routine_definition"]:
                        f.write(proc["routine_definition"])
                    f.write("\n\n")

            logger.info(
                f"Exported {len(procedures)} procedures/functions to {procedures_file}"
            )

        # Export triggers
        triggers = self.get_triggers(schema_name)
        if triggers:
            triggers_file = schema_dir / "triggers.sql"
            with open(triggers_file, "w", encoding="utf-8") as f:
                f.write(f"-- Triggers in schema: {schema_name}\n")
                f.write(f"-- Generated on: {datetime.now()}\n\n")

                for trigger in triggers:
                    f.write(f"-- Trigger: {trigger['trigger_name']}\n")
                    f.write(f"-- Table: {trigger['table_name']}\n")
                    f.write(f"-- Event: {trigger['events']} {trigger['timing']}\n")
                    f.write(f"-- Function: {trigger['function_name']}\n\n")

                    # Write the trigger definition
                    if trigger["trigger_definition"]:
                        f.write(f"{trigger['trigger_definition']}\n\n")

                    # Write the function definition
                    if trigger["function_definition"]:
                        f.write(f"-- Function Definition:\n")
                        f.write(f"{trigger['function_definition']}\n")
                    f.write("\n" + "=" * 50 + "\n\n")

            logger.info(f"Exported {len(triggers)} triggers to {triggers_file}")

    def export_sample_data(self, schema_name: str):
        """Export sample data for all tables and views in schema"""
        logger.info(f"Exporting sample data for schema: {schema_name}")

        data_dir = self.data_path / schema_name
        data_dir.mkdir(exist_ok=True)

        # Export table data
        tables = self.get_tables_in_schema(schema_name)
        for table in tables:
            try:
                table_name = table["table_name"]
                query = (
                    f"SELECT * FROM {schema_name}.{table_name} LIMIT {self.sample_rows}"
                )

                # Use PostgresManager.execute instead of pandas
                rows = self.pg_manager.execute(query)

                if rows:
                    # Export as CSV
                    csv_file = data_dir / f"{table_name}_sample.csv"
                    self._write_csv(csv_file, rows)

                    # Export as JSON
                    json_file = data_dir / f"{table_name}_sample.json"
                    self._write_json(json_file, rows)

                    logger.info(
                        f"Exported {len(rows)} rows from {schema_name}.{table_name}"
                    )
                else:
                    logger.info(f"No data found in {schema_name}.{table_name}")

            except Exception as e:
                logger.error(
                    f"Error exporting data from {schema_name}.{table_name}: {str(e)}"
                )

        # Export view data
        views = self.get_views_in_schema(schema_name)
        for view in views:
            try:
                view_name = view["view_name"]
                query = (
                    f"SELECT * FROM {schema_name}.{view_name} LIMIT {self.sample_rows}"
                )

                # Use PostgresManager.execute instead of pandas
                rows = self.pg_manager.execute(query)

                if rows:
                    # Export as CSV
                    csv_file = data_dir / f"{view_name}_view_sample.csv"
                    self._write_csv(csv_file, rows)

                    # Export as JSON
                    json_file = data_dir / f"{view_name}_view_sample.json"
                    self._write_json(json_file, rows)

                    logger.info(
                        f"Exported {len(rows)} rows from view {schema_name}.{view_name}"
                    )
                else:
                    logger.info(f"No data found in view {schema_name}.{view_name}")

            except Exception as e:
                logger.error(
                    f"Error exporting data from view {schema_name}.{view_name}: {str(e)}"
                )

    def _write_csv(self, file_path: Path, rows: List[Dict]):
        """Write rows to CSV file"""
        if not rows:
            return

        with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = rows[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                # Convert any non-string values to strings for CSV
                cleaned_row = {}
                for key, value in row.items():
                    if value is None:
                        cleaned_row[key] = ""
                    elif isinstance(value, (dict, list)):
                        cleaned_row[key] = json.dumps(value)
                    else:
                        cleaned_row[key] = str(value)
                writer.writerow(cleaned_row)

    def _write_json(self, file_path: Path, rows: List[Dict]):
        """Write rows to JSON file"""
        # Convert datetime objects and other non-serializable types
        serializable_rows = []
        for row in rows:
            serializable_row = {}
            for key, value in row.items():
                if hasattr(value, "isoformat"):  # datetime objects
                    serializable_row[key] = value.isoformat()
                elif isinstance(value, (bytes, bytearray)):
                    serializable_row[key] = str(value)
                else:
                    serializable_row[key] = value
            serializable_rows.append(serializable_row)

        with open(file_path, "w", encoding="utf-8") as jsonfile:
            json.dump(serializable_rows, jsonfile, indent=2, default=str)

    def export_all(self, specific_schemas: Optional[List[str]] = None):
        """
        Export all schemas or specific schemas

        Args:
            specific_schemas: List of specific schema names to export, or None for all
        """
        try:
            # Get schemas to export
            if specific_schemas:
                schemas = specific_schemas
            else:
                schemas = self.get_all_schemas()

            logger.info(f"Found {len(schemas)} schemas to export: {schemas}")

            # Create summary file
            summary = {
                "export_timestamp": datetime.now().isoformat(),
                "database_config": {
                    "host": self.host_key,
                    "database": self.database_key,
                    "schema": self.schema_key,
                },
                "schemas_exported": [],
                "sample_rows_per_table": self.sample_rows,
            }

            # Export each schema
            for schema_name in schemas:
                try:
                    logger.info(f"Processing schema: {schema_name}")

                    # Export DDL
                    self.export_schema_ddl(schema_name)

                    # Export sample data
                    self.export_sample_data(schema_name)

                    summary["schemas_exported"].append(schema_name)

                except Exception as e:
                    logger.error(f"Error processing schema {schema_name}: {str(e)}")
                    continue

            # Write summary
            summary_file = (
                self.output_base_path
                / f"{self.host_key}_{self.database_key}"
                / "export_summary.json"
            )
            with open(summary_file, "w") as f:
                json.dump(summary, f, indent=2)

            logger.info(f"Export complete! Summary written to {summary_file}")

        except Exception as e:
            logger.error(f"Error during export: {str(e)}")
            raise
        finally:
            self.pg_manager.close()


def main():
    """
    Example usage of the schema exporter
    """
    # Available configurations from your constants
    available_configs = []
    for host in POSTGRES_HOSTS:
        for db in POSTGRES_HOSTS[host]:
            for schema in POSTGRES_HOSTS[host][db]:
                available_configs.append((host, db, schema))

    print("Available database configurations:")
    for i, (host, db, schema) in enumerate(available_configs, 1):
        print(f"{i}. {host}/{db}/{schema}")

    # Example: Export from supabase control schema
    if available_configs:
        host, db, schema = available_configs[0]  # Use first available config

        exporter = PostgreSQLSchemaExporter(
            host_key=host, database_key=db, schema_key=schema, sample_rows=50
        )

        # Export all schemas in the database
        exporter.export_all()

        print(f"Schema export completed for {host}/{db}")


if __name__ == "__main__":
    main()
