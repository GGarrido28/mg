import os
import logging
import time
import concurrent.futures

from google.cloud import run_v2
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPICallError, ResourceExhausted, NotFound

from mg.google_cloud.constants import ENV_CREDS_PATH

logging.basicConfig(level=logging.INFO)


class CloudRunJobRunner:
    def __init__(self, project_id, region, creds_path=None):
        """
        Initializes the Cloud Run Job Runner with explicit credentials.

        :param project_id: Google Cloud project ID
        :param region: Google Cloud Run region
        :param credentials_file: Path to the service account key file
        """
        self.project_id = project_id
        self.region = region

        # Define region rankings based on performance and cost
        self.region_rankings = [
            "us-central1",  # Primary region
            "us-east4",  # Northern Virginia - good infrastructure
            "us-east1",  # South Carolina - geographically distinct
            "us-west1",  # Oregon - similar pricing tier as us-central1
            "us-west2",  # Los Angeles
        ]

        # Ensure preferred region is first in the list if not already
        if self.region in self.region_rankings:
            self.region_rankings.remove(self.region)
        self.region_rankings.insert(0, self.region)

        # Load credentials explicitly
        if creds_path is None:
            creds_path = os.environ.get("DFS_SIM_CREDS")
        else:
            creds_path = os.environ.get(ENV_CREDS_PATH.get(creds_path)) or creds_path
        self.credentials = service_account.Credentials.from_service_account_file(
            creds_path
        )
        self.client = run_v2.JobsClient(credentials=self.credentials)
        # Create admin client for job creation if needed
        self.admin_client = run_v2.JobsClient(credentials=self.credentials)

    def job_exists(self, job_name, region):
        """
        Check if a job exists in the specified region

        :param job_name: Name of the Cloud Run job
        :param region: Region to check
        :return: True if job exists, False otherwise
        """
        job_path = f"projects/{self.project_id}/locations/{region}/jobs/{job_name}"
        try:
            self.client.get_job(name=job_path)
            return True
        except NotFound:
            return False
        except Exception as e:
            logging.error(f"Error checking if job exists in {region}: {e}")
            return False

    def copy_job_to_region(self, job_name, source_region, target_region):
        """
        Copy a job from one region to another

        :param job_name: Name of the Cloud Run job
        :param source_region: Source region where the job exists
        :param target_region: Target region where to create the job
        :return: True if successful, False otherwise
        """
        try:
            # Get the source job
            source_job_path = (
                f"projects/{self.project_id}/locations/{source_region}/jobs/{job_name}"
            )
            source_job = self.client.get_job(name=source_job_path)

            # Prepare the new job request
            parent = f"projects/{self.project_id}/locations/{target_region}"

            # Extract configurations from source job
            new_job = run_v2.Job(
                template=source_job.template,
                labels=dict(source_job.labels),
                annotations=dict(source_job.annotations),
                client=source_job.client,
                binary_authorization=source_job.binary_authorization,
            )

            # Create the job in the target region
            create_request = run_v2.CreateJobRequest(
                parent=parent, job=new_job, job_id=job_name
            )

            operation = self.admin_client.create_job(request=create_request)
            # Wait for the operation to complete
            result = operation.result()

            logging.info(
                f"Successfully copied job {job_name} from {source_region} to {target_region}"
            )
            return True

        except Exception as e:
            logging.error(f"Failed to copy job to region {target_region}: {e}")
            return False

    def run_job(self, job_name, arguments=[], monitor=True, auto_create_job=True):
        """
        Runs a Cloud Run job with optional command-line arguments.
        If the job fails due to quota limits, attempts to run in alternative regions.
        Can optionally create the job in alternative regions if it doesn't exist.

        :param job_name: Name of the Cloud Run job
        :param arguments: List of command-line arguments to pass to the job
        :param monitor: If True, monitor the job until completion; if False, just check it started
        :param auto_create_job: If True, automatically create the job in alternative regions if needed
        :return: Tuple of (job status, used region) or ("STARTED", used_region) if monitor=False
        """
        # Track source region where job exists for potential copying
        source_region = None

        # Try regions in order of ranking until successful
        for region in self.region_rankings:
            # Check if job exists in this region
            if not self.job_exists(job_name, region):
                logging.info(f"Job {job_name} does not exist in region {region}")

                # If this is the first region and job doesn't exist, something is wrong
                if region == self.region_rankings[0]:
                    logging.error(
                        f"Job {job_name} does not exist in primary region {region}"
                    )
                    # Can't copy a job if it doesn't exist in the primary region
                    if not auto_create_job:
                        continue
                    else:
                        # We'll try other regions, but mark that we can't copy from this one
                        logging.warning(
                            "Cannot use primary region as source for job copying"
                        )
                else:
                    # For alternative regions, try to copy the job if required
                    if auto_create_job and source_region:
                        logging.info(
                            f"Attempting to copy job {job_name} from {source_region} to {region}"
                        )
                        if not self.copy_job_to_region(job_name, source_region, region):
                            logging.warning(
                                f"Failed to copy job to {region}, skipping this region"
                            )
                            continue
                    elif not auto_create_job:
                        logging.info(
                            f"Skipping region {region} as job does not exist and auto-create is disabled"
                        )
                        continue
            else:
                # Mark this as a potential source region for copying
                if source_region is None:
                    source_region = region

            job_path = f"projects/{self.project_id}/locations/{region}/jobs/{job_name}"
            execution_parent = (
                f"projects/{self.project_id}/locations/{region}/jobs/{job_name}"
            )

            # Ensure arguments are properly formatted
            formatted_args = [str(arg) for arg in arguments] if arguments else []

            execution_request = run_v2.RunJobRequest(
                name=job_path,
                overrides=run_v2.RunJobRequest.Overrides(
                    task_count=1,
                    container_overrides=[
                        run_v2.RunJobRequest.Overrides.ContainerOverride(
                            args=formatted_args
                        )
                    ],
                ),
            )

            try:
                logging.info(
                    f"Attempting to start job {job_name} in region {region} with arguments: {formatted_args}"
                )

                # Start the job but don't wait for completion with a timeout
                operation = self.client.run_job(request=execution_request)

                # Just ensure the job started successfully without waiting for full completion
                if not operation.running():
                    # Check if there was an immediate error
                    if operation.exception():
                        logging.error(
                            f"Job failed to start in {region}: {operation.exception()}"
                        )
                        continue  # Try next region

                logging.info(f"Job {job_name} successfully started in region {region}.")

                # If monitoring is disabled, just return that job started
                if not monitor:
                    return "STARTED", region

                # Monitor the job execution until completion or failure
                logging.info(f"Monitoring execution status in region {region}...")
                status = self._monitor_job_execution(job_name, execution_parent, region)

                return status, region

            except ResourceExhausted as e:
                logging.warning(f"Resource quota exceeded in region {region}: {e}")
                # Continue to next region
                continue

            except NotFound as e:
                logging.warning(f"Job not found in region {region}: {e}")
                # This shouldn't happen since we checked above, but just in case
                continue

            except GoogleAPICallError as e:
                if "Quota exceeded" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    logging.warning(f"Quota exceeded in region {region}: {e}")
                    # Continue to next region
                    continue
                else:
                    logging.error(f"Error executing job in {region}: {e}")
                    # If it's not a quota issue, still try the next region
                    continue

        # If we get here, all regions failed
        logging.error(f"All regions failed to run job {job_name}")
        return None, None

    def run_multiple_jobs(
        self,
        job_name,
        arguments=[],
        execution_count=1,
        monitor=False,
        auto_create_job=True,
    ):
        """
        Runs multiple instances of a Cloud Run job asynchronously.
        Distributes jobs across regions as needed to handle quota limitations.

        :param job_name: Name of the Cloud Run job
        :param arguments: List of command-line arguments to pass to the job
        :param execution_count: Number of times to execute the job
        :param monitor: If True, monitor all jobs until completion
        :param auto_create_job: If True, automatically create the job in alternative regions if needed
        :return: List of tuples (job_status, region)
        """
        if execution_count < 1:
            logging.warning("Execution count must be at least 1. Setting to 1.")
            execution_count = 1

        logging.info(f"Starting {execution_count} instances of job {job_name}")

        # If only running one job, just use the regular method
        if execution_count == 1:
            return [self.run_job(job_name, arguments, monitor, auto_create_job)]

        # For multiple jobs, use concurrent execution with region tracking
        results = []

        # Define a worker function for each job execution
        def execute_job(_):
            return self.run_job(job_name, arguments, monitor, auto_create_job)

        # Use ThreadPoolExecutor to run jobs in parallel
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(execution_count, 10)
        ) as executor:
            # Submit all job executions
            futures = [executor.submit(execute_job, i) for i in range(execution_count)]

            # Collect results as they complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    result, region = future.result()
                    results.append((result, region))
                    logging.info(
                        f"Job execution completed with status: {result} in region: {region}"
                    )
                except Exception as e:
                    logging.error(f"Error in job execution: {e}")
                    results.append((None, None))

        return results

    def _monitor_job_execution(self, job_name, execution_parent, region):
        """
        Monitors the job execution until it completes or fails.

        :param job_name: Name of the Cloud Run job
        :param execution_parent: Parent path for job executions
        :param region: Region where the job is running
        :return: Final status of the job
        """
        execution_client = run_v2.ExecutionsClient(credentials=self.credentials)

        # Initial check to ensure the job started
        status = self._get_latest_execution_status(job_name, execution_parent)
        if not status:
            logging.error(
                f"Could not find execution for job: {job_name} in region {region}"
            )
            return None

        logging.info(f"Initial job status in {region}: {status}")

        # Terminal states for Cloud Run jobs
        terminal_states = ["SUCCEEDED", "FAILED", "CANCELLED", "TIMEOUT"]

        # Wait until the job reaches a terminal state
        while status not in terminal_states:
            logging.info(
                f"Job {job_name} in {region} is still running with status: {status}"
            )
            time.sleep(3)  # Check every 3 seconds
            status = self._get_latest_execution_status(job_name, execution_parent)

        if status == "SUCCEEDED":
            logging.info(f"Job {job_name} in {region} completed successfully")
        else:
            logging.warning(f"Job {job_name} in {region} ended with status: {status}")

        return status

    def _get_latest_execution_status(self, job_name, execution_parent):
        """
        Retrieves the status of the latest execution.

        :param job_name: Name of the Cloud Run job
        :param execution_parent: Parent path for job executions
        """
        execution_client = run_v2.ExecutionsClient(credentials=self.credentials)
        executions = execution_client.list_executions(parent=execution_parent)

        latest_execution = next(iter(executions), None)
        if latest_execution:
            # Debug log the conditions
            logging.debug("Conditions on latest execution:")
            for condition in latest_execution.conditions:
                logging.debug(
                    f"  {condition.type_}: {condition.state} - {getattr(condition, 'message', 'No message')}"
                )

            # Find the Completed condition to determine overall status
            status = "UNKNOWN"
            completed_found = False

            for condition in latest_execution.conditions:
                if condition.type_ == "Completed":
                    completed_found = True
                    # Get the raw enum object and convert to string representation
                    state_name = (
                        condition.state.name
                        if hasattr(condition.state, "name")
                        else str(condition.state)
                    )
                    if "SUCCEEDED" in state_name:
                        status = "SUCCEEDED"
                    elif "FAILED" in state_name:
                        status = "FAILED"
                    elif "RECONCILING" in state_name:
                        status = "RUNNING"
                    else:
                        status = "UNKNOWN"

            # If no Completed condition or status is still unknown, check if it's running
            if not completed_found or status == "UNKNOWN":
                for condition in latest_execution.conditions:
                    if condition.type_ == "Started":
                        state_name = (
                            condition.state.name
                            if hasattr(condition.state, "name")
                            else str(condition.state)
                        )
                        if "SUCCEEDED" in state_name:
                            status = "RUNNING"
                            break
            return status
        else:
            logging.info("No execution records found.")
            return None

    def terminate_all_running_jobs(self, job_name=None, all_regions=False):
        """
        Terminates all running executions of a job or all jobs across one or more regions.

        :param job_name: Optional specific job name to target. If None, targets all jobs.
        :param all_regions: If True, terminates jobs in all regions. If False, only in the primary region.
        :return: Dictionary mapping region to list of terminated execution IDs
        """
        execution_client = run_v2.ExecutionsClient(credentials=self.credentials)
        terminated_executions = {}

        # Determine which regions to check
        regions_to_check = self.region_rankings if all_regions else [self.region]

        for region in regions_to_check:
            terminated_in_region = []
            try:
                # List all jobs in the region if no specific job name is provided
                if job_name is None:
                    parent = f"projects/{self.project_id}/locations/{region}"
                    job_list = list(self.client.list_jobs(parent=parent))
                    job_names = [job.name.split("/")[-1] for job in job_list]
                else:
                    job_names = [job_name]

                # For each job, find and cancel running executions
                for job in job_names:
                    executions_parent = (
                        f"projects/{self.project_id}/locations/{region}/jobs/{job}"
                    )
                    try:
                        executions = list(
                            execution_client.list_executions(parent=executions_parent)
                        )

                        for execution in executions:
                            # Check if the execution is still running
                            is_running = True
                            for condition in execution.conditions:
                                if (
                                    condition.type_ == "Completed"
                                    and "SUCCEEDED" in str(condition.state)
                                ):
                                    is_running = False
                                    break

                            if is_running:
                                # Get the execution ID from the full name
                                execution_id = execution.name.split("/")[-1]
                                cancel_request = run_v2.CancelExecutionRequest(
                                    name=f"projects/{self.project_id}/locations/{region}/jobs/{job}/executions/{execution_id}"
                                )

                                logging.info(
                                    f"Cancelling execution {execution_id} of job {job} in region {region}"
                                )
                                execution_client.cancel_execution(
                                    request=cancel_request
                                )
                                terminated_in_region.append(execution_id)
                    except Exception as e:
                        logging.error(
                            f"Error processing job {job} in region {region}: {e}"
                        )

                if terminated_in_region:
                    terminated_executions[region] = terminated_in_region

            except Exception as e:
                logging.error(f"Error terminating jobs in region {region}: {e}")

        return terminated_executions


if __name__ == "__main__":
    PROJECT_ID = "dfs-sim"
    REGION = "us-central1"
    JOB_NAME = "test-cloud-run-job"
    arguments = ["main_cloud.py", "--mode=run_pre_simulation"]
    # Define the correct credentials file (e.g., DFS_SIM_CREDS)
    CREDENTIALS_FILE = "dfs_sim_service_account.json"

    runner = CloudRunJobRunner(PROJECT_ID, REGION, creds_path=CREDENTIALS_FILE)

    runner.copy_job_to_region(JOB_NAME, "us-central1", "us-east4")
    # Example: Run a single job and monitor it
    # result, region = runner.run_job(JOB_NAME, arguments=["main_cloud.py", "--mode=run_pre_simulation"], monitor=True, auto_create_job=True)
    # print(f"Job completed with status: {result} in region: {region}")

    # Example: Run multiple jobs asynchronously without monitoring
    # results = runner.run_multiple_jobs(JOB_NAME, arguments=["main_cloud.py", "--mode=run_pre_simulation"], execution_count=5, monitor=False, auto_create_job=True)
    # for status, region in results:
    #     print(f"Job status: {status}, Region: {region}")

    # Start a job without monitoring (returns immediately)
    status, region = runner.run_job(
        JOB_NAME, arguments, monitor=False, auto_create_job=True
    )
    print(f"Job started with status: {status} in region: {region}")

    # Start 5 instances of the same job asynchronously
    results = runner.run_multiple_jobs(
        JOB_NAME, arguments, execution_count=5, monitor=False, auto_create_job=True
    )
    for status, region in results:
        print(f"Job status: {status}, Region: {region}")
