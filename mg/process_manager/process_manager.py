import os

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager


class ProcessingRequestManager:
    def __init__(self, request: dict):
        self.request = request
        self.process_name = f"ProcessingRequestManager"
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
        self.process_id = self.logger.process_id

    def insert_request(self):
        self.logger.log("INFO", f"Inserting request {self.request}")
        rec = [
            {
                "process_id": self.process_id,
                "status": "not_started",
                "task_type": self.request.get("task_type"),
                "args": self.request.get("args"),
            }
        ]
        self.postgres_manager.insert_rows(
            "processing_requests", rec[0].keys(), rec, contains_dicts=True, update=True
        )

    def get_request(self):
        self.logger.log("INFO", f"Getting request {self.request}")
        q = f"SELECT * FROM control.processing_requests WHERE id = '{self.request.get('id')}'"
        request = self.postgres_manager.execute(q)
        self.logger.log("INFO", f"Got request {request}")
        return request

    def update_request(self, status: str):
        self.logger.log("INFO", f"Updating request {self.request}")
        q = f"UPDATE control.processing_requests SET status = '{status}' WHERE id = '{self.request.get('id')}'"
        self.postgres_manager.execute(q)
        self.logger.log("INFO", f"Updated request {self.request}")

    def fetch_status(self) -> str:
        self.logger.log("INFO", f"Fetching status {self.request}")
        q = f"SELECT status FROM control.processing_requests WHERE id = '{self.request.get('id')}'"
        status = self.postgres_manager.execute(q)
        self.logger.log("INFO", f"Fetched status {status}")
        return status.get("status")

    def close_manager(self):
        self.logger.log("INFO", f"Closing {self.process_name}")
        self.logger.close_logger()
        self.postgres_manager.close()
