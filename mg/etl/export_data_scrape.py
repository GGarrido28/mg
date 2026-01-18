import os
from typing import Optional, Dict, Any, List

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager

from mg.etl.proteus import DataWrangler


class DataExport:
    def __init__(self):
        self.process_name = "data_export"
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = LoggerManager(
            script_name=self.script_name,
            script_path=self.script_path,
            process_name=self.process_name,
            sport=None,
            database="defaultdb",
            schema="control",
        )
        self.logger.log_exceptions()
        self.postgres_manager = PostgresManager(
            "digital_ocean", "defaultdb", "control", return_logging=False
        )
        self.data_wrangler = DataWrangler()

    def nest_data(self, data: Dict[str, Any], parent_key: str = "") -> Dict[str, Any]:
        """Nest data for PostgreSQL"""
        try:
            items = []
            for key, value in data.items():
                new_key = f"{parent_key}.{key}" if parent_key else key
                if isinstance(value, dict):
                    items.extend(self.nest_data(value, parent_key=new_key).items())
                else:
                    items.append((new_key, value))
            return dict(items)
        except Exception as e:
            self.logger.log(level="ERROR", message="Error nesting data")
            raise

    def export_data(
        self, process_id: str, table_name: str, target_database: str, target_schema: str
    ) -> None:
        """Export data to PostgreSQL database"""
        try:
            # Fetch data from control.data_scrape
            query = f"""
                SELECT
                    *
                FROM
                    control.data_scrape
                WHERE
                    process_id = '{process_id}'
            """
            process = self.postgres_manager.execute(query)

            if not process:
                self.logger.log(
                    level="ERROR", message=f"Process ID {process_id} not found"
                )
                return None

            # Parse the JSON data
            data = process[0].get("data")
            data = self.data_wrangler.flatten_dict(data)
            self.logger.log(
                level="INFO", message=f"Exporting data for process ID {process_id}"
            )

            target_db = PostgresManager(
                "digital_ocean", target_database, target_schema, return_logging=False
            )
            target_db.create_table(
                table_name=table_name.lower(), dict_list=data, delete=True
            )
            target_db.insert_rows(
                table_name.lower(),
                data[0].keys(),
                data,
                contains_dicts=True,
                update=True,
            )
            target_db.close()
        except Exception as e:
            self.logger.log(
                level="ERROR",
                message=f"Error exporting data for process ID {process_id}",
            )
            raise
        finally:
            self.postgres_manager.close()


if __name__ == "__main__":
    de = DataExport()
    # de.export_data(
    #     process_id="PGKYv1t8TZWVg0Fn26wjrg",
    #     table_name="PGKYv1t8TZWVg0Fn26wjrg",
    #     target_database="mma",
    #     target_schema= "core",
    # )
