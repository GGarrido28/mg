import os

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager


class ProcessRunner:
    def __init__(self):
        self.process_name = f"ProcessRunner"
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

    def check_open_requests(self):
        self.logger.log("INFO", f"Checking open requests")
        q = f"SELECT * FROM control.processing_requests WHERE status = 'not_started' LIMIT 1"
        requests = self.postgres_manager.execute(q)
        if requests:
            self.logger.log("INFO", f"Got requests {requests}")
            return requests
        else:
            self.logger.log("INFO", f"No open requests")
            return None

    def run_request(self, request: dict):
        self.logger.log("INFO", f"Running request {request}")
        request = request[0]
        request_id = request.get("id")
        self.update_request(request_id, "running")
        task_type = request.get("task_type")
        args = request.get("args")
        self.logger.log("INFO", f"Running task {task_type} with args {args}")
        task = task_type(args)
        task.run()
        self.update_request(request_id, "completed")
        self.logger.log("INFO", f"Completed request {request}")

    def run_check(self, request: dict):
        self.logger.log("INFO", f"Running check {request}")
        request = self.check_open_requests()
        if request:
            request = request[0]
            self.logger.log("INFO", f"Running request {request}")
            self.run_request(request)
        else:
            self.logger.log("INFO", f"No open requests")
