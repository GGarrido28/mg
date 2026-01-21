import logging
import subprocess
from mg.alerts.alerts import BaseCheck, register_check_type


class MacCheck(BaseCheck):
    """Check if Mac is reachable via SSH"""

    def __init__(self, alert_config):
        super().__init__(alert_config)
        try:
            from mg.alerts.constants import MAC_HOST

            self.mac_host = MAC_HOST
        except ImportError:
            logging.error("Failed to import MAC_HOST from constants")
            self.mac_host = "localhost"  # Default value

    def check_condition(self):
        try:
            cmd = (
                f"ssh -o ConnectTimeout=5 {self.mac_host} echo 'Connection successful'"
            )
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

            if result.returncode == 0:
                logging.info(f"{self.alert_name}: Mac connection successful")
                return False, "Connected"
            else:
                logging.info(
                    f"{self.alert_name}: Mac connection failed: {result.stderr}"
                )
                return True, f"Connection failed: {result.stderr}"
        except Exception as e:
            logging.error(f"{self.alert_name}: Error checking Mac connection: {e}")
            return True, f"Error: {e}"


# Register this check type - use the exact name for precise matching
register_check_type("Check Mac", MacCheck)
