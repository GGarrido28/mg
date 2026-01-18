import logging
import pytz
from datetime import datetime, timedelta, timezone

from mg.db.postgres_manager import PostgresManager
from alerts import BaseCheck, register_check_type


class StaleCheck(BaseCheck):
    """Check for stale data in database tables"""

    def __init__(self, alert_config):
        super().__init__(alert_config)

        # Additional stale check specific attributes
        if isinstance(alert_config, dict):
            self.monitored_table = alert_config.get("monitored_table")
            self.monitored_column = alert_config.get("monitored_column")
            self.tolerance_in_hours = alert_config.get("tolerance_hours")
            self.sport = alert_config.get("sport")
            self.db = alert_config.get("db", "defaultdb")
            self.schema = alert_config.get("schema")
        else:
            self.monitored_table = getattr(alert_config, "monitored_table", None)
            self.monitored_column = getattr(alert_config, "monitored_column", None)
            self.tolerance_in_hours = getattr(alert_config, "tolerance_in_hours", 1)
            self.sport = getattr(alert_config, "sport", None)
            self.db = getattr(alert_config, "db", "defaultdb")
            self.schema = getattr(alert_config, "schema", None)

    # Update this part in stale_checks.py to ensure it uses tolerance_hours correctly
    def check_condition(self):
        """Check if data is stale"""
        try:
            # Get the CST timezone and current time in CST
            cst = pytz.timezone("US/Central")
            dt_now = datetime.now(cst)  # Timezone-aware current time in CST

            # Build fully qualified table name if schema is provided
            table_name = (
                f"{self.schema}.{self.monitored_table}"
                if self.schema
                else self.monitored_table
            )

            # Ensure required fields are present
            if not self.monitored_table or not self.monitored_column:
                logging.error(
                    f"{self.alert_name}: Missing required table or column configuration"
                )
                return True, "Missing configuration"

            sql = PostgresManager("digital_ocean", self.db, self.schema)
            q = f"SELECT MAX({self.monitored_column}) as max_updated from {table_name}"

            result = sql.execute(q)
            sql.close()

            if not result or len(result) == 0:
                logging.error(f"{self.alert_name}: No results returned from query")
                return True, "No results from query"

            last_updated = result[0].get("max_updated")
            if last_updated is None:
                logging.error(
                    f"{self.alert_name}: NULL value returned for {self.monitored_column}"
                )
                return True, "NULL value in monitored column"

            # Calculate cutoff time in CST
            tolerance_hours = self.tolerance_in_hours
            cutoff_time = dt_now - timedelta(hours=tolerance_hours)

            # Handle timezone for last_updated
            if last_updated.tzinfo is None:
                # The timestamp is naive, assume it's stored in CST
                last_updated = cst.localize(last_updated)
                logging.info(f"Localized naive datetime to CST: {last_updated}")
            elif last_updated.tzinfo != cst.tzinfo:
                # If timestamp has a different timezone, convert to CST
                last_updated = last_updated.astimezone(cst)
                logging.info(f"Converted datetime to CST: {last_updated}")

            if last_updated < cutoff_time:
                logging.info(
                    f"{self.alert_name} failing stale data check: {self.alert_message} - Last update: {last_updated}, Cutoff: {cutoff_time}"
                )
                return True, last_updated
            else:
                logging.info(
                    f"{self.alert_name} data is current. Last update: {last_updated}, Cutoff: {cutoff_time}"
                )
                return False, last_updated

        except Exception as e:
            logging.error(f"Error checking stale data for {self.alert_name}: {e}")
            return True, f"Error: {e}"

    def __repr__(self):
        return f"{self.alert_name}: {self.monitored_table}.{self.monitored_column} : {self.alert_message}"


# Register this check type
register_check_type("stale", StaleCheck)
register_check_type("stale_data", StaleCheck)
