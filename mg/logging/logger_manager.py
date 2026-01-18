import logging
import inspect
import functools
import time
import psutil
import sys
import traceback
import uuid
import base64
import json
import datetime

from mg.db.postgres_manager import PostgresManager
from mg.alerts.config import _EMAIL_SENDER, _EMAIL_RECEIVER, _EMAIL_APP_PASSWORD

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logging.basicConfig(level=logging.INFO)


class LoggerManager:
    def __init__(
        self,
        script_name,
        script_path,
        process_name=None,
        sport=None,
        database=None,
        schema=None,
    ):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.process_id = str(
            base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b"=").decode("utf-8")
        )
        self.db = PostgresManager(
            host="digital_ocean",
            database="defaultdb",
            schema="control",
            return_logging=False,
        )
        self.step_id = 0
        self.script_name = script_name
        self.script_path = script_path
        self.process_name = (
            process_name or f"{self.script_path.split('/')[-1]} {self.script_name}"
        )
        self.sport = sport
        self.database = database
        self.schema = schema
        self.automation_log = []
        self.info_logs = []
        self.data_logs = []
        self.warning_logs = []
        self.error_logs = []
        self.debug_logs = []
        self.last_data_update = None

    def get_logger(self):
        return self.logger

    def log(self, level, message, send_alert=False):
        if level == "info":
            self.logger.info(message)
            self.info_logs.append(message)
            self.update_automation_log("INFO", message)
        elif level == "data":
            self.logger.info(message)
            self.data_logs.append(message)
            self.update_automation_log("DATA", message)
        elif level == "warning":
            self.logger.warning(message)
            self.warning_logs.append(message)
            self.update_automation_log("WARNING", message)
        elif level == "error":
            self.logger.error(message)
            self.error_logs.append(message)
            self.update_automation_log("ERROR", message)
            if send_alert:
                self.send_email_alert(f"Error in {self.script_name}", message)
        elif level == "debug":
            self.logger.debug(message)
            self.debug_logs.append(message)
            self.update_automation_log("DEBUG", message)
        else:
            self.logger.info(message)
            self.info_logs.append(message)
            self.update_automation_log("INFO", message)

    def start_timer(self):
        self.start_time = time.time()
        self.update_automation_log("In Progress", f"{self.script_name} script started")

    def end_timer(self):
        self.end_time = time.time()
        self.update_automation_log("Completed", f"{self.script_name} script completed")
        self.update_automation_log(
            "In Progress",
            f"{self.script_name} script took {self.end_time - self.start_time} seconds",
        )
        self.insert_automation_log()
        self.generate_performance_summary()

    def update_automation_log(self, status, log_message):
        self.step_id += 1
        msg = {
            "process_id": self.process_id,
            "task": self.process_name,
            "step": self.step_id,
            "status": status,
            "log_message": log_message,
        }
        self.automation_log.append(msg)

    def insert_automation_log(self):
        if self.automation_log:
            self.db.insert_rows(
                "automation_log",
                self.automation_log[0].keys(),
                self.automation_log,
                contains_dicts=True,
                update=True,
            )

    @staticmethod
    def log_arguments(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            signature = inspect.signature(func)
            bound_arguments = signature.bind(*args, **kwargs)
            bound_arguments.apply_defaults()

            logging.info(
                f"Calling {func.__name__} with arguments: {dict(bound_arguments.arguments)}"
            )
            result = func(*args, **kwargs)
            logging.info(f"{func.__name__} returned {result}")
            return result

        return wrapper

    @staticmethod
    def log_time(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            duration = end - start
            logging.info(f"Executed {func.__name__} in {duration} seconds.")
            logging.info(f"RAM Used (GB): {psutil.virtual_memory()[3] / 1000000000}")
            return result

        return wrapper

    def send_email_alert(self, subject, message):
        msg = MIMEMultipart()
        msg["From"] = _EMAIL_SENDER
        msg["To"] = _EMAIL_RECEIVER
        msg["Subject"] = subject

        msg.attach(MIMEText(message, "plain"))

        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(_EMAIL_SENDER, _EMAIL_APP_PASSWORD)
        text = msg.as_string()

        server.sendmail(_EMAIL_SENDER, _EMAIL_RECEIVER, text)
        server.quit()

    def display_logs(self, level="all"):
        logs = {
            "info": self.info_logs,
            "data": self.data_logs,
            "warning": self.warning_logs,
            "error": self.error_logs,
            "debug": self.debug_logs,
        }
        if level != "all":
            if level in logs:
                print(f"{level.capitalize()} Logs:")
                for log in logs[level]:
                    print(log)
            else:
                print("Invalid log level")
        else:
            for log_level, log_list in logs.items():
                print(f"\n{log_level.capitalize()} Logs:")
                for log in log_list:
                    print(log)

    def close_logger(self):
        self.insert_automation_log()
        self.update_process_table()
        logging.shutdown()
        self.db.close()

    @staticmethod
    def retry(func, retries=3):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Attempt {attempt+1}/{retries} failed: {e}")
                    time.sleep(2**attempt)
            raise Exception(f"Failed after {retries} retries")

        return wrapper

    def log_system_usage(self):
        cpu_usage = psutil.cpu_percent(interval=1)
        ram_usage = psutil.virtual_memory().percent
        logging.info(f"CPU Usage: {cpu_usage}% | RAM Usage: {ram_usage}%")

    def check_db_connection(self):
        try:
            self.db.execute_query("SELECT 1")
            logging.info("Database connection is alive")
        except Exception as e:
            logging.error(f"Database connection lost: {e}")
            self.send_email_alert("Database Connection Lost", str(e))

    def generate_performance_summary(self):
        total_time = self.end_time - self.start_time
        cpu_usage = psutil.cpu_percent()
        ram_usage = psutil.virtual_memory().percent
        logging.info(
            f"Performance Summary: Total Time - {total_time}s, CPU Usage - {cpu_usage}%, RAM Usage - {ram_usage}%"
        )

    def log_exceptions(self):
        def handle_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            error_message = "".join(
                traceback.format_exception(exc_type, exc_value, exc_traceback)
            )
            logging.error(f"Unhandled exception: {error_message}")
            self.send_email_alert(
                f"Unhandled Exception in {self.script_name}", error_message
            )

        sys.excepthook = handle_exception

    def check_alert_log(self, alert_name, alert_description, **kwargs):
        q = """
            SELECT * 
            FROM control.alert_log 
            WHERE 
                alert_name = %s and 
                alert_description = %s and
                disabled = true and 
                cast(created_at as date) = CURRENT_DATE
        """
        alert_log = self.db.execute_query(q, (alert_name, alert_description))
        if len(alert_log) == 0:
            record = [
                {
                    "process_id": self.process_id,
                    "alert_name": alert_name,
                    "alert_description": alert_description,
                    "sport": self.sport,
                    "database": self.database,
                    "schema": self.schema,
                    "review_script": None,
                    "script_path": self.script_path,
                    "review_table": None,
                    "disabled": False,
                }
            ]
            for arg in kwargs:
                record[0][arg] = kwargs[arg]
            self.db.insert_rows(
                "alert_log", record[0].keys(), record, contains_dicts=True, update=True
            )
            self.send_email_alert(alert_name, alert_description)
        else:
            logging.info(f"Alert already logged")

    def save_data(self, data):
        json_data = json.dumps(
            data, ensure_ascii=False, indent=4, sort_keys=True, default=str
        )
        record = [{"process_id": self.process_id, "data": json_data}]
        self.db.insert_rows(
            "data_scrape", record[0].keys(), record, contains_dicts=True, update=True
        )

    def save_last_data_update(self, last_data_update):
        self.last_data_update = last_data_update

    def update_process_table(self):
        if self.warning_logs or self.error_logs:
            success = False
        else:
            success = True
        delimiter = "\\" if "\\" in self.script_path else "/"
        record = [
            {
                "process_name": self.process_name,
                "last_process_id": self.process_id,
                "sport": self.sport,
                "database": self.database,
                "success": success,
                "last_run": datetime.datetime.now(
                    tz=datetime.timezone(datetime.timedelta(hours=-5))
                ),
                "last_data_timestamp": self.last_data_update,
            }
        ]
        self.db.insert_rows(
            "process", record[0].keys(), record, contains_dicts=True, update=True
        )
        record = [
            {
                "process_name": self.process_name,
                "process_id": self.process_id,
                "sport": self.sport,
                "database": self.database,
                "success": success,
                "last_run": datetime.datetime.now(
                    tz=datetime.timezone(datetime.timedelta(hours=-5))
                ),
                "last_data_timestamp": self.last_data_update,
            }
        ]
        self.db.insert_rows(
            "process_run", record[0].keys(), record, contains_dicts=True, update=True
        )

    def get_process(self):
        return self.db.execute(
            f"SELECT * FROM process WHERE process_name = '{self.process_name}'"
        )

    def check_enabled(self):
        process = self.get_process()
        if process:
            enabled = process[0]["enabled"]
            if enabled:
                return True
            logging.info("Process is disabled")
            return False
        else:
            # Process doesn't exist, so we create it as enabled by default
            record = [
                {
                    "process_name": self.process_name,
                    "sport": self.sport,
                    "database": self.database,
                    "enabled": True,
                }
            ]
            self.db.insert_rows(
                "process", record[0].keys(), record, contains_dicts=True, update=True
            )
            logging.info("New process created and enabled by default")
            return True


if __name__ == "__main__":
    logger_manager = LoggerManager(
        "test_script",
        "test_script.py",
        "RotogrindersScraper_CFB_RG",
        sport="nfl",
        database="nfl",
        schema="core",
    )
    # logger = logger_manager.get_logger()
    # logger_manager.log("This is a test log message")
    # logger_manager.send_email_alert("Test Subject", "Test Message")
    # logger_manager.close_logger()
    # logger_manager.check_alert_log("Test Alert", "This is a test alert", review_script="test_script.py", review_table="test_table")
    # logger_manager.close_logger()
    # print(logger_manager.process_id)
    # alert_name = "Error processing ETR CFB data"
    # alert_description = "Error processing ETR CFB data: No players found for fd fd_main"
    # logger_manager.check_alert_log(alert_name, alert_description)
    print(logger_manager.check_enabled())
