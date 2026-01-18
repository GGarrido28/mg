import json
import logging
import gzip
import os
import time
import random

from google.cloud import storage
from google.cloud import exceptions as gcloud_exceptions

from mg.google_cloud.constants import ENV_CREDS_PATH
from mg.google_cloud.secret_manager import get_secret

logging.basicConfig(level=logging.INFO)


def create_client(creds_path=None, secret_name=None, return_credentials=False):
    """
    Create a GCS storage client with appropriate credentials or return the credentials

    Args:
        creds_path: Path to credentials file or credentials dictionary
        secret_name: Name of the secret in Secret Manager
        return_credentials: If True, return the credentials instead of a client

    Returns:
        storage.Client or dict: Authenticated GCS client or credentials dictionary
    """

    # If secret_name is provided, retrieve it from Secret Manager
    if secret_name is not None:
        try:
            creds_json = get_secret(secret_name)
            creds_dict = json.loads(creds_json)

            # Return credentials or create client
            if return_credentials:
                return creds_dict
            else:
                return storage.Client.from_service_account_info(creds_dict)
        except Exception as e:
            logging.error(f"Failed to retrieve secret {secret_name}: {e}")
            # Fall through to other credential methods if secret retrieval fails

    # If no secret_name or secret retrieval failed, try other methods
    if creds_path is None:
        creds_path = os.environ.get("DFS_SIM_CREDS")
    else:
        creds_path = os.environ.get(ENV_CREDS_PATH.get(creds_path)) or creds_path

    # Handle different credential types
    if creds_path is None:
        if return_credentials:
            logging.warning("No credentials found, returning None")
            return None
        else:
            # Use default credentials
            logging.info("No explicit credentials provided, using default credentials")
            return storage.Client()
    elif isinstance(creds_path, dict):
        # If credentials are already a dictionary
        if return_credentials:
            return creds_path
        else:
            logging.info("Using credentials from dictionary")
            return storage.Client.from_service_account_info(creds_path)
    else:
        # Check if this is a file path that exists
        if os.path.exists(creds_path):
            # It's a file path
            if return_credentials:
                # Read the credentials file and return the dict
                with open(creds_path, "r") as f:
                    return json.load(f)
            else:
                logging.info(f"Using credentials from file: {creds_path}")
                return storage.Client.from_service_account_json(creds_path)
        else:
            # If it's not a file, fall back to default credentials
            if return_credentials:
                logging.warning(
                    f"Credentials path '{creds_path}' not found, returning None"
                )
                return None
            else:
                logging.warning(
                    f"Credentials path '{creds_path}' not found, using default credentials"
                )
                return storage.Client()


def store_object(object_name, bucket_name, data, client=None):
    storage_client = client or create_client()

    bucket = storage_client.bucket(bucket_name)
    blob = storage.Blob(object_name, bucket)

    # Properly serialize to JSON instead of using str()
    json_data = json.dumps(data)

    blob.upload_from_string(json_data, content_type="application/json")
    logging.info("Stored object {}".format(object_name))


def retrieve_object(object_name, bucket_name, storage_client=None):
    if storage_client is None:
        storage_client = create_client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    try:
        data = blob.download_as_bytes()
        try:
            # Try to decompress if it's gzipped
            decompressed = gzip.decompress(data)
            content = decompressed.decode("utf-8")
        except (OSError, IOError):
            # Not gzipped, decode as UTF-8
            content = data.decode("utf-8")

        # First try normal JSON parsing
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            # If that fails, it might be a Python string representation
            # Try to evaluate it as a Python literal
            import ast

            try:
                return ast.literal_eval(content)
            except (SyntaxError, ValueError) as e:
                logging.error(f"Failed to parse content: {e}")
                logging.error(f"Content preview: {content[:200]}")
                return None

    except gcloud_exceptions.NotFound:
        logging.warning(f"{object_name} object not found")
    except gcloud_exceptions.Forbidden:
        logging.warning(f"{object_name} object is forbidden")
    except gcloud_exceptions.BadRequest:
        logging.warning(f"{object_name} object is directory")
    except gcloud_exceptions.Unauthorized:
        logging.warning(f"{object_name} object is unauthorized")

    return None


def retrieve_json_object(object_name, bucket_name):
    obj = retrieve_object(object_name, bucket_name)
    try:
        return json.loads(obj)
    except:
        logging.warning("Storage object is not a JSON file")
        return None

def delete_folder_contents(
    bucket_name, folder_prefixes, client=None, max_retries=5, initial_retry_delay=1
):
    """
    Deletes all objects within specified folders in a GCS bucket with retry logic.

    Args:
        bucket_name (str): Name of the bucket
        folder_prefixes (list): List of folder paths to clear (e.g. ["partial", "sim-messages"])
        client: Optional storage client. If None, creates a new client.
        max_retries (int): Maximum number of retry attempts for failed operations
        initial_retry_delay (float): Initial delay in seconds between retries (will increase exponentially)

    Returns:
        dict: Summary of deleted objects per folder
    """
    # Get or create the client
    storage_client = client or create_client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Initialize results tracking
    results = {}

    # Process each folder prefix
    for prefix in folder_prefixes:
        # Ensure prefix ends with /
        if not prefix.endswith("/"):
            prefix += "/"

        # Track stats for this prefix
        deleted = 0
        failed = 0
        skipped = 0
        failure_reasons = {}

        logging.info(f"Deleting objects with prefix: {prefix}")

        # Get all blobs with this prefix
        try:
            blobs = list(bucket.list_blobs(prefix=prefix))
            logging.info(f"Found {len(blobs)} objects with prefix {prefix}")

            # Process each blob
            for blob in blobs:
                # Skip the directory object itself
                if blob.name == prefix:
                    skipped += 1
                    continue

                # Try to delete with retries
                retry_count = 0
                retry_delay = initial_retry_delay
                success = False

                while retry_count <= max_retries and not success:
                    try:
                        # Get a fresh reference to the blob
                        direct_blob = bucket.blob(blob.name)
                        # Delete it
                        direct_blob.delete()
                        deleted += 1
                        success = True

                        if deleted % 100 == 0:
                            logging.info(
                                f"Deleted {deleted} objects so far in {prefix}"
                            )

                    except gcloud_exceptions.ServiceUnavailable as e:
                        retry_count += 1
                        if retry_count <= max_retries:
                            # Add jitter to avoid thundering herd
                            jitter = random.uniform(0.8, 1.2)
                            sleep_time = retry_delay * jitter

                            logging.warning(
                                f"Service unavailable deleting {blob.name}, "
                                f"retrying in {sleep_time:.2f}s (attempt {retry_count}/{max_retries})"
                            )

                            # Sleep before retry
                            time.sleep(sleep_time)

                            # Exponential backoff
                            retry_delay *= 2
                        else:
                            # Max retries exceeded
                            failed += 1
                            error_type = type(e).__name__
                            error_message = str(e)

                            # Track failure reasons
                            if error_type not in failure_reasons:
                                failure_reasons[error_type] = []
                            if len(failure_reasons[error_type]) < 5:  # Limit examples
                                failure_reasons[error_type].append(
                                    (blob.name, error_message)
                                )

                            logging.error(
                                f"Failed to delete {blob.name} after {max_retries} retries: "
                                f"{error_type} - {error_message}"
                            )

                    except Exception as e:
                        # Non-retryable error
                        failed += 1
                        error_type = type(e).__name__
                        error_message = str(e)

                        # Track failure reasons
                        if error_type not in failure_reasons:
                            failure_reasons[error_type] = []
                        if len(failure_reasons[error_type]) < 5:  # Limit examples
                            failure_reasons[error_type].append(
                                (blob.name, error_message)
                            )

                        logging.error(
                            f"Error deleting {blob.name}: {error_type} - {error_message}"
                        )
                        break  # Don't retry non-service unavailable errors

            # Save results for this prefix
            results[prefix] = {
                "deleted": deleted,
                "failed": failed,
                "skipped": skipped,
                "failure_reasons": failure_reasons,
            }

            logging.info(
                f"Completed prefix {prefix}: {deleted} deleted, {failed} failed, {skipped} skipped"
            )

        except Exception as e:
            logging.error(
                f"Error processing prefix {prefix}: {type(e).__name__} - {str(e)}"
            )
            results[prefix] = {
                "deleted": deleted,
                "failed": failed,
                "skipped": skipped,
                "error": f"{type(e).__name__}: {str(e)}",
            }

    # Log summary
    total_deleted = sum(r["deleted"] for r in results.values())
    total_failed = sum(r["failed"] for r in results.values())
    total_skipped = sum(r["skipped"] for r in results.values())
    logging.info(
        f"FINAL SUMMARY: Deleted {total_deleted} objects, "
        f"failed to delete {total_failed} objects, "
        f"skipped {total_skipped} objects"
    )

    return results


if __name__ == "__main__":
    delete_folder_contents(
        bucket_name="bucket",
        folder_prefixes=["automated-tests"],
        client=create_client(),
    )
