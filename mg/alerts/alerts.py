import logging
import pytz
from datetime import datetime, timedelta, timezone

from mg.db.postgres_manager import PostgresManager

# Dictionary to register different check types
CHECK_TYPES = {}


def register_check_type(name, check_class):
    """Register a new check type"""
    CHECK_TYPES[name] = check_class
    logging.info(f"Registered check type: {name}")


class Notification:
    def __init__(self, alert_config):
        self.alert_config = alert_config

    def send(self, last_update=None):
        priority = self.alert_config.get("priority", "MEDIUM")
        alert_message = self.alert_config.get("alert_message", "Alert triggered")

        if last_update:
            logging.info(
                f"Sending {priority} alert: {alert_message} (Last update: {last_update})"
            )
        else:
            logging.info(f"Sending {priority} alert: {alert_message}")


class BaseCheck:
    """Base class for all alert checks"""

    @classmethod
    def from_database(cls, alert_id, db_connection=None):
        """Create a Check instance by loading config from the database"""
        try:
            # If no connection is provided, create one
            if db_connection is None:
                db_connection = PostgresManager("digital_ocean", "defaultdb", "control")
                need_to_close = True
            else:
                need_to_close = False

            query = """
                SELECT * FROM control.util_stale_data_alert 
                WHERE id = %s
            """
            result = db_connection.execute(query, (alert_id,))

            if need_to_close:
                db_connection.close()

            if not result:
                raise ValueError(f"No alert config found with ID {alert_id}")

            # Create the appropriate check type based on alert_type field
            alert_config = result[0]
            alert_type = alert_config.get("alert_type")
            alert_name = alert_config.get("alert_name", "")

            # First check the alert_type field
            if alert_type == "stale_data":
                from mg.alerts.stale_checks import StaleCheck

                return StaleCheck(alert_config)

            # If no alert_type or unrecognized, fall back to existing logic
            # Look for a registered check type based on the alert name
            for type_name, check_class in CHECK_TYPES.items():
                if type_name in alert_name:
                    logging.info(f"Creating {type_name} check for {alert_name}")
                    return check_class(alert_config)

            # Special case for "Check Mac" - exact match
            if alert_name == "Check Mac":
                from mg.alerts.checks import MacCheck

                return MacCheck(alert_config)

            # If we can't determine the type, use configuration indicators
            if alert_config.get("monitored_table") and alert_config.get(
                "monitored_column"
            ):
                from mg.alerts.stale_checks import StaleCheck

                return StaleCheck(alert_config)

            # Default to base check if we can't determine type
            logging.warning(
                f"Could not determine check type for alert {alert_name}, using BaseCheck"
            )
            return cls(alert_config)

        except Exception as e:
            logging.error(f"Error loading check with ID {alert_id}: {e}")
            raise

    @classmethod
    def get_all_active_checks(cls):
        """Return all active Check instances from the database"""
        try:
            connection = PostgresManager("digital_ocean", "defaultdb", "control")
            query = """
                SELECT * FROM control.util_stale_data_alert 
                WHERE is_active = true
            """
            results = connection.execute(query)
            connection.close()

            # Create the appropriate check type for each configuration
            checks = []
            for row in results:
                try:
                    # Use from_database to determine the check type
                    check = cls.from_database(row.get("id"), None)
                    checks.append(check)
                except Exception as e:
                    logging.error(
                        f"Error creating check for {row.get('alert_name')}: {e}"
                    )
            return checks
        except Exception as e:
            logging.error(f"Error getting active checks: {e}")
            raise

    def __init__(self, alert_config):
        """
        Initialize with either a dictionary from the database or an object
        """
        # Handle both dictionary input (from database) and object input (legacy)
        if isinstance(alert_config, dict):
            self.alert_id = alert_config.get("id")
            self.alert_name = alert_config.get("alert_name")
            self.alert_message = alert_config.get("alert_message")
            self.start_hour_utc = alert_config.get("start_hour", 0)
            self.end_hour_utc = alert_config.get("end_hour", 24)
            self.always_on = self.start_hour_utc == 0 and self.end_hour_utc == 24
            self.is_paused = not alert_config.get("is_active", True)
            self.desc = alert_config.get("alert_description")
            self.priority = alert_config.get("priority", "MEDIUM")
        else:
            # Legacy object-based initialization
            self.alert_id = getattr(alert_config, "alert_id", None)
            self.alert_name = getattr(alert_config, "alert_name", "")
            self.alert_message = getattr(alert_config, "alert_message", "")
            self.start_hour_utc = getattr(alert_config, "start_hour_utc", 0)
            self.end_hour_utc = self.start_hour_utc + getattr(
                alert_config, "stop_after_hours", 24
            )
            self.always_on = getattr(alert_config, "always_on", False)
            self.is_paused = getattr(alert_config, "is_paused", False)
            self.desc = getattr(alert_config, "desc", "")
            self.priority = getattr(alert_config, "priority", "MEDIUM")

    def _in_monitoring_window(self):
        try:
            # We want to calculate "current day" based on Central time
            try:
                cst = pytz.timezone("US/Central")
            except Exception as e:
                logging.error(f"Error initializing timezone: {e}")
                # Fall back to UTC if timezone can't be initialized
                return True

            cst_dt = datetime.now(cst).date()
            dt_now = datetime.now(tz=timezone.utc)

            # Create start and end window timestamps
            start_window = datetime(
                cst_dt.year,
                cst_dt.month,
                cst_dt.day,
                self.start_hour_utc,
                tzinfo=timezone.utc,
            )

            # Use end_hour_utc directly instead of calculating from duration
            if self.end_hour_utc < self.start_hour_utc:
                # If end_hour is less than start_hour, it means it's on the next day
                next_day = cst_dt + timedelta(days=1)
                end_window = datetime(
                    next_day.year,
                    next_day.month,
                    next_day.day,
                    self.end_hour_utc,
                    tzinfo=timezone.utc,
                )
            else:
                end_window = datetime(
                    cst_dt.year,
                    cst_dt.month,
                    cst_dt.day,
                    self.end_hour_utc,
                    tzinfo=timezone.utc,
                )

            return dt_now > start_window and dt_now < end_window
        except Exception as e:
            logging.error(f"Error checking monitoring window: {e}")
            # Return True by default to ensure checks run if there's an error
            return True

    def _check_is_active(self):
        try:
            if self.is_paused:
                return False
            elif self.always_on:
                return True
            # Some checks are only active during specific time window
            elif self._in_monitoring_window():
                return True
            else:
                return False
        except Exception as e:
            logging.error(f"Error checking if {self.alert_name} is active: {e}")
            # Return False by default on error
            return False

    def check_condition(self):
        """
        Check the specific condition. To be implemented by subclasses.
        Should return a tuple (is_triggered, details)
        """
        logging.warning(
            f"check_condition() not implemented for {self.__class__.__name__}"
        )
        return False, "Not implemented"

    def check(self):
        """Run the check and send notification if needed"""
        try:
            if self._check_is_active():
                is_triggered, details = self.check_condition()
                if is_triggered:
                    # Convert self to dictionary for notification
                    alert_config_dict = {
                        "id": self.alert_id,
                        "alert_name": self.alert_name,
                        "alert_message": self.alert_message,
                        "priority": self.priority,
                    }
                    notification = Notification(alert_config_dict)
                    notification.send(details)
                    return True
            else:
                logging.info(f"{self.alert_name} is not active")
            return False
        except Exception as e:
            logging.error(f"Error running check {self.alert_name}: {e}")
            return False

    def __repr__(self):
        return f"{self.alert_name}: {self.alert_message}"


def run_all_checks():
    """Run all active checks"""
    try:
        logging.info("Getting all active checks...")
        checks = BaseCheck.get_all_active_checks()
        logging.info(f"Found {len(checks)} active checks")

        for check in checks:
            try:
                logging.info(f"Running check: {check}")
                check.check()
            except Exception as e:
                logging.error(f"Error running check {check}: {e}")
    except Exception as e:
        logging.error(f"Error running checks: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    run_all_checks()
