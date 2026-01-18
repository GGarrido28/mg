import logging

from google.cloud import secretmanager
from mg.google_cloud.config import GCP_PROJECT_NUMBER

logging.basicConfig(level=logging.INFO)


def get_secret(secret_id, version="latest"):
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_NUMBER}/secrets/{secret_id}/versions/{version}"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Failed to retrieve secret {secret_id}: {e}")
    return None
