import os
import json
import logging
import time
import random
import concurrent.futures

from google.cloud import pubsub_v1
from google.oauth2 import service_account

from mg.google_cloud.secret_manager import get_secret
from mg.google_cloud.constants import ENV_CREDS_PATH

logging.basicConfig(level=logging.INFO)


class PubSub:
    def __init__(
        self,
        project_id="dfs-sim",
        region="us-central1",
        creds_path=None,
        secret_name=None,
    ):
        """
        Initializes the PubSub Runner with explicit credentials.

        :param project_id: Google Cloud project ID
        :param region: Default Google Cloud region
        :param creds_path: Path to the service account key file or credential dictionary
        :param secret_name: Name of the secret in Secret Manager
        """
        self.project_id = project_id
        self.region = region

        # Define region rankings based on performance and cost
        self.region_rankings = [
            "us-central1",  # Primary region
            "us-east4",  # Northern Virginia
            "us-east1",  # South Carolina
            "us-west1",  # Oregon
            "us-west2",  # Los Angeles
        ]

        # Ensure preferred region is first in the list if not already
        if self.region in self.region_rankings:
            self.region_rankings.remove(self.region)
        self.region_rankings.insert(0, self.region)

        # Get credentials
        self.credentials = self._get_credentials(creds_path, secret_name)

        # Create a publisher client for each region to help with distribution
        self.publishers = {}
        for region in self.region_rankings:
            self.publishers[region] = pubsub_v1.PublisherClient(
                credentials=self.credentials
            )

        # Track topics that exist in each region
        self.topic_cache = {region: set() for region in self.region_rankings}

        # Configure rate limiting settings
        self.max_requests_per_min = 40  # Adjust based on your function's capacity
        self.request_timestamps = []

        # Configure retry settings
        self.max_retries = 5
        self.initial_retry_delay = 1  # seconds
        self.max_retry_delay = 30  # seconds

    def _get_credentials(self, creds_path=None, secret_name=None):
        """
        Get credentials from various sources with priority order:
        1. Secret Manager (if secret_name provided)
        2. Direct credentials dict or path
        3. Environment variable
        4. Default credentials

        Args:
            creds_path: Path to credentials file or credentials dictionary
            secret_name: Name of the secret in Secret Manager

        Returns:
            service_account.Credentials: The credentials object
        """
        # If secret_name is provided, retrieve it from Secret Manager
        if secret_name is not None:
            try:
                creds_json = get_secret(secret_name)
                if creds_json:
                    creds_dict = json.loads(creds_json)
                    logging.info(
                        f"Using credentials from Secret Manager: {secret_name}"
                    )
                    return service_account.Credentials.from_service_account_info(
                        creds_dict
                    )
            except Exception as e:
                logging.error(f"Failed to retrieve secret {secret_name}: {e}")
                # Fall through to other credential methods if secret retrieval fails

        # If no secret_name or secret retrieval failed, try other methods
        if creds_path is None:
            creds_path = os.environ.get("DFS_SIM_CREDS")
        else:
            # Check if this is an environment variable name
            creds_path = (
                os.environ.get(ENV_CREDS_PATH.get(creds_path, "")) or creds_path
            )

        # Handle different credential types
        if creds_path is None:
            # Use default credentials
            logging.info("No explicit credentials provided, using default credentials")
            return None
        elif isinstance(creds_path, dict):
            # If credentials are already a dictionary
            logging.info("Using credentials from dictionary")
            return service_account.Credentials.from_service_account_info(creds_path)
        else:
            # Check if this is a file path that exists
            if os.path.exists(creds_path):
                # It's a file path
                logging.info(f"Using credentials from file: {creds_path}")
                return service_account.Credentials.from_service_account_file(creds_path)
            else:
                # If it's not a file, fall back to default credentials
                logging.warning(
                    f"Credentials path '{creds_path}' not found, using default credentials"
                )
                return None

    def _apply_rate_limiting(self):
        """
        Apply rate limiting to avoid overwhelming Cloud Functions.
        Ensures we don't exceed max_requests_per_min.
        """
        now = time.time()
        window_start = now - 60  # 1 minute window

        # Remove timestamps older than our window
        self.request_timestamps = [
            t for t in self.request_timestamps if t > window_start
        ]

        # If we've reached the limit, wait
        if len(self.request_timestamps) >= self.max_requests_per_min:
            oldest = self.request_timestamps[0]
            wait_time = 60 - (now - oldest)
            if wait_time > 0:
                logging.info(f"Rate limiting: waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)

        # Add the new timestamp after possible waiting
        self.request_timestamps.append(time.time())

    def topic_exists(self, topic_name, region):
        """
        Check if a topic exists in the specified region

        :param topic_name: Name of the PubSub topic
        :param region: Region to check
        :return: True if topic exists, False otherwise
        """
        # Check cache first
        if topic_name in self.topic_cache[region]:
            return True

        publisher = self.publishers[region]
        topic_path = publisher.topic_path(self.project_id, topic_name)

        try:
            publisher.get_topic(topic=topic_path)
            # Add to cache
            self.topic_cache[region].add(topic_name)
            return True
        except Exception as e:
            logging.debug(f"Topic {topic_name} not found in region {region}: {e}")
            return False

    def create_topic(self, topic_name, region):
        """
        Create a topic in the specified region

        :param topic_name: Name of the PubSub topic
        :param region: Region to create in
        :return: True if created or exists, False otherwise
        """
        if self.topic_exists(topic_name, region):
            logging.info(f"Topic {topic_name} already exists in {region}")
            return True

        publisher = self.publishers[region]
        topic_path = publisher.topic_path(self.project_id, topic_name)

        try:
            publisher.create_topic(name=topic_path)
            # Add to cache
            self.topic_cache[region].add(topic_name)
            logging.info(f"Topic {topic_name} created in {region}")
            return True
        except Exception as e:
            logging.error(f"Failed to create topic {topic_name} in {region}: {e}")
            return False

    def copy_topic_to_region(self, topic_name, source_region, target_region):
        """
        Copy a topic from one region to another

        :param topic_name: Name of the PubSub topic
        :param source_region: Source region where the topic exists
        :param target_region: Target region where to create the topic
        :return: True if successful, False otherwise
        """
        # PubSub topics are simpler than Cloud Run jobs, just need to create a new one
        if not self.topic_exists(topic_name, source_region):
            logging.error(
                f"Source topic {topic_name} does not exist in {source_region}"
            )
            return False

        return self.create_topic(topic_name, target_region)

    def publish(self, message, topic_name, region=None):
        """
        Publishes a PubSub message to the specified topic with exponential backoff retries.

        :param message: a message to publish
        :param topic_name: name of the PubSub topic
        :param region: region to publish to (defaults to self.region)
        :return: message ID if successful, None otherwise
        """
        if region is None:
            region = self.region

        if not self.topic_exists(topic_name, region):
            logging.warning(f"Topic {topic_name} does not exist in {region}")
            return None

        publisher = self.publishers[region]
        topic_path = publisher.topic_path(self.project_id, topic_name)

        # Apply rate limiting before publishing
        self._apply_rate_limiting()

        # Use exponential backoff for retries
        retry_count = 0
        delay = self.initial_retry_delay

        while retry_count <= self.max_retries:
            try:
                json_message = json.dumps(message, separators=(",", ":"))
                future = publisher.publish(topic_path, json_message.encode("utf-8"))
                message_id = future.result()
                logging.info(
                    f"Published message to {topic_name} in {region}, message ID: {message_id}"
                )
                return message_id
            except Exception as e:
                retry_count += 1

                if (
                    "no available instance" in str(e).lower()
                    or retry_count <= self.max_retries
                ):
                    # Calculate backoff with jitter
                    jitter = random.uniform(0, 0.5 * delay)
                    sleep_time = min(delay + jitter, self.max_retry_delay)

                    logging.warning(
                        f"Error publishing to {topic_name} in {region}, retry {retry_count}/{self.max_retries} after {sleep_time:.2f}s: {e}"
                    )
                    time.sleep(sleep_time)

                    # Exponential backoff
                    delay = min(delay * 2, self.max_retry_delay)
                else:
                    logging.error(
                        f"Failed to publish to {topic_name} in {region} after {retry_count} retries: {e}"
                    )
                    return None

        logging.error(f"Exhausted all retries publishing to {topic_name} in {region}")
        return None

    def publish_with_fallback(self, message, topic_name, auto_create_topic=True):
        """
        Publish a message with automatic fallback to alternative regions

        :param message: a message to publish
        :param topic_name: name of the PubSub topic
        :param auto_create_topic: if True, create topic in alternative regions if needed
        :return: tuple of (message_id, used_region) if successful, (None, None) otherwise
        """
        # Track source region where topic exists for potential copying
        source_region = None

        # Try regions in order of ranking until successful
        for region in self.region_rankings:
            # Check if topic exists in this region
            if not self.topic_exists(topic_name, region):
                logging.info(f"Topic {topic_name} does not exist in region {region}")

                # If this is the first region and topic doesn't exist, something is wrong
                if region == self.region_rankings[0]:
                    logging.error(
                        f"Topic {topic_name} does not exist in primary region {region}"
                    )
                    if auto_create_topic:
                        logging.info(
                            f"Attempting to create topic {topic_name} in {region}"
                        )
                        if not self.create_topic(topic_name, region):
                            continue
                        source_region = region
                    else:
                        continue
                elif auto_create_topic and source_region:
                    # For alternative regions, try to copy the topic if required
                    logging.info(
                        f"Attempting to copy topic {topic_name} from {source_region} to {region}"
                    )
                    if not self.copy_topic_to_region(topic_name, source_region, region):
                        logging.warning(
                            f"Failed to copy topic to {region}, skipping this region"
                        )
                        continue
                elif not auto_create_topic:
                    logging.info(
                        f"Skipping region {region} as topic does not exist and auto-create is disabled"
                    )
                    continue
            else:
                # Mark this as a potential source region for copying
                if source_region is None:
                    source_region = region

            # Try to publish to this region
            message_id = self.publish(message, topic_name, region)
            if message_id:
                return message_id, region

        # If we get here, all regions failed
        logging.error(f"All regions failed to publish to topic {topic_name}")
        return None, None

    def publish_multiple(
        self, message, topic_name, num_iterations=2, auto_create_topic=True
    ):
        """
        Publishes multiple instances of a message to PubSub asynchronously.
        Distributes across regions as needed to handle quota limitations.

        :param message: a message to publish
        :param topic_name: name of the PubSub topic
        :param num_iterations: Number of times to publish the message
        :param auto_create_topic: If True, create topic in alternative regions if needed
        :return: List of tuples (message_id, region)
        """
        if num_iterations < 1:
            logging.warning("Execution count must be at least 1. Setting to 1.")
            num_iterations = 1

        logging.info(f"Starting {num_iterations} publish operations to {topic_name}")

        # If only publishing one message, just use the regular method
        if num_iterations == 1:
            message_id, region = self.publish_with_fallback(
                message, topic_name, auto_create_topic
            )
            return [(message_id, region)]

        # For multiple messages, use concurrent execution with region tracking
        results = []

        # Define worker function for each publish operation
        def execute_publish(_):
            return self.publish_with_fallback(message, topic_name, auto_create_topic)

        # Use ThreadPoolExecutor to run publish operations in parallel
        # Use a smaller pool size to avoid overwhelming Cloud Functions
        max_workers = min(num_iterations, 5)  # Reduced from 10 to 5

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all publish operations
            futures = [
                executor.submit(execute_publish, i) for i in range(num_iterations)
            ]

            # Collect results as they complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    message_id, region = future.result()
                    results.append((message_id, region))
                    logging.info(
                        f"Publish operation completed with message ID: {message_id} in region: {region}"
                    )
                except Exception as e:
                    logging.error(f"Error in publish operation: {e}")
                    results.append((None, None))

        return results

    def publish_multiple_fire_and_forget(
        self, message, topic_name, num_iterations=2, auto_create_topic=True
    ):
        """
        Publishes multiple instances of a message to PubSub asynchronously without waiting for results.
        Distributes across regions as needed to handle quota limitations.

        :param message: a message to publish
        :param topic_name: name of the PubSub topic
        :param num_iterations: Number of times to publish the message
        :param auto_create_topic: If True, create topic in alternative regions if needed
        :return: None
        """
        if num_iterations < 1:
            logging.warning("Execution count must be at least 1. Setting to 1.")
            num_iterations = 1

        logging.info(f"Starting {num_iterations} publish operations to {topic_name}")

        # Define worker function for each publish operation
        def execute_publish(_):
            return self.publish_with_fallback(message, topic_name, auto_create_topic)

        # Use ThreadPoolExecutor to run publish operations in parallel
        # Use a smaller pool size to avoid overwhelming Cloud Functions
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=5
        )  # Reduced from 10 to 5

        # Submit all publish operations but don't wait for results
        for i in range(num_iterations):
            executor.submit(execute_publish, i)

        # Don't wait for completion, let the executor run in the background
        logging.info(
            f"Submitted {num_iterations} publish operations to run in background"
        )


# Create convenience functions similar to those in cloud_storage.py
def publish(
    message, topic_name, project_id="dfs-sim", creds_path=None, secret_name=None
):
    """
    Convenience function to publish a single message without creating a PubSub instance

    :param message: Message to publish
    :param topic_name: Name of the PubSub topic
    :param project_id: Google Cloud project ID
    :param creds_path: Path to credentials file or credentials dictionary
    :param secret_name: Name of secret in Secret Manager
    :return: Tuple of (message_id, region)
    """
    client = PubSub(
        project_id=project_id, creds_path=creds_path, secret_name=secret_name
    )
    return client.publish_with_fallback(message, topic_name)


def publish_multiple(
    message,
    topic_name,
    num_iterations=2,
    project_id="dfs-sim",
    creds_path=None,
    secret_name=None,
):
    """
    Convenience function to publish multiple messages without creating a PubSub instance

    :param message: Message to publish
    :param topic_name: Name of the PubSub topic
    :param num_iterations: Number of times to publish the message
    :param project_id: Google Cloud project ID
    :param creds_path: Path to credentials file or credentials dictionary
    :param secret_name: Name of secret in Secret Manager
    :return: List of tuples (message_id, region)
    """
    client = PubSub(
        project_id=project_id, creds_path=creds_path, secret_name=secret_name
    )
    return client.publish_multiple(message, topic_name, num_iterations)


def publish_multiple_fire_and_forget(
    message,
    topic_name,
    num_iterations=2,
    project_id="dfs-sim",
    creds_path=None,
    secret_name=None,
):
    """
    Convenience function to publish multiple messages in fire-and-forget mode

    :param message: Message to publish
    :param topic_name: Name of the PubSub topic
    :param num_iterations: Number of times to publish the message
    :param project_id: Google Cloud project ID
    :param creds_path: Path to credentials file or credentials dictionary
    :param secret_name: Name of secret in Secret Manager
    :return: None
    """
    client = PubSub(
        project_id=project_id, creds_path=creds_path, secret_name=secret_name
    )
    client.publish_multiple_fire_and_forget(message, topic_name, num_iterations)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Example with secret manager
    # Initialize the PubSub with secret manager
    runner = PubSub(project_id="dfs-sim", secret_name="dfs-sim-creds")

    # Define the message
    message = {
        "mode": "run_pre_simulation",
        "projection_source": None,
        "backtest": False,
    }

    # Option 1: Publish a single message
    message_id, region = runner.publish_with_fallback(
        message, topic_name="test-topic"
    )
    print(f"Published message with ID: {message_id} in region: {region}")

    # Option 2: Publish multiple messages and wait for results (using convenience function)
    results = publish_multiple(
        message,
        topic_name="test-topic",
        num_iterations=3,
        secret_name="dfs-sim-creds",
    )
    for message_id, region in results:
        print(f"Message ID: {message_id}, Region: {region}")

    # Option 3: Fire-and-forget multiple messages (using convenience function)
    publish_multiple_fire_and_forget(
        message,
        topic_name="test-topic",
        num_iterations=3,
        secret_name="dfs-sim-creds",
    )
    print("Publishing started in background")
