import os

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager
from notification import send_email_alert  # Import your existing function

# Import our new alert system
from alerts import BaseCheck
import stale_checks  # Import to register the StaleCheck type
import checks  # Import to register the MacCheck type


class AlertManager:
    def __init__(self):
        self.process_name = f"AlertManager"
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.database = "defaultdb"
        self.schema = "control"
        self.logger = LoggerManager(
            script_name=self.script_name,
            script_path=self.script_path,
            process_name=self.process_name,
            database=self.database,
            schema=self.schema,
        )
        self.logger.log_exceptions()
        self.pgm = PostgresManager("digital_ocean", self.database, self.schema)

    def main(self):
        """
        Run the alert manager.
        """
        self.logger.log(level="info", message="Starting alert manager...")
        alert_results = []

        # Get all active checks from the database
        try:
            checks = BaseCheck.get_all_active_checks()
            self.logger.log(
                level="info", message=f"Found {len(checks)} active alerts to check."
            )

            # Run each check
            for check in checks:
                self.logger.log(
                    level="info", message=f"Running alert {check.alert_name}..."
                )
                try:
                    # Run the check and capture the result
                    is_triggered = check.check()

                    if is_triggered:
                        self.logger.log(
                            level="warning",
                            message=f"Alert {check.alert_name} triggered!",
                        )
                        # Store alert for notification
                        alert_results.append(
                            {
                                "name": check.alert_name,
                                "message": check.alert_message,
                                "data": {
                                    "id": check.alert_id,
                                    "priority": check.priority,
                                    "description": check.desc,
                                },
                            }
                        )
                    else:
                        self.logger.log(
                            level="info",
                            message=f"Alert {check.alert_name} check completed successfully.",
                        )
                except Exception as e:
                    self.logger.log(
                        level="error",
                        message=f"Error running alert {check.alert_name}: {e}",
                    )

        except Exception as e:
            self.logger.log(level="error", message=f"Error getting active alerts: {e}")

        # Send email if we have alerts to report
        if alert_results:
            self._send_email_alerts(alert_results)

        self.logger.log(level="info", message="Alert manager finished running.")

    def _send_email_alerts(self, alerts):
        """Send email alerts for triggered conditions"""
        subject = f"Alert Manager: {len(alerts)} alerts triggered"

        # Build message body
        message = "The following alerts were triggered:\n\n"
        for alert in alerts:
            message += f"- {alert['name']}: {alert['message']}\n"
            if alert.get("data"):
                message += f"  Details: {str(alert['data'])}\n"

        # Send the email
        try:
            send_email_alert(subject, message)
            self.logger.log(level="info", message=f"Email alert sent successfully")
        except Exception as e:
            self.logger.log(level="error", message=f"Failed to send email alert: {e}")


if __name__ == "__main__":
    am = AlertManager()
    am.main()
