import pickle
import dill  # Alternative to pickle for serializing complex objects
import os
import re
import shutil
import logging
from datetime import datetime
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple, TypedDict
import json

from mg.google_cloud.cloud_storage import create_client, store_object, retrieve_object
from mg.google_cloud.constants import SPORT_BUCKET, DFS_SIM_CREDS
from google.cloud import storage


class ModelLoadResult(TypedDict):
    """Structured return type for model loading operations"""

    model: Any
    metadata: Dict[str, Any]
    results: Optional[Dict[str, Any]]


class ModelManager:
    def __init__(self, base_path: str = "models/", max_versions: int = 5):
        """
        Initialize the model manager

        Args:
            base_path: Directory where models will be stored
            max_versions: Maximum number of versions to keep per model
        """
        self.base_path = base_path
        self.max_versions = max_versions
        os.makedirs(base_path, exist_ok=True)

        # Set up logging
        self._setup_logging()

    def is_serializable(self, obj: Any) -> Tuple[bool, str]:
        """
        Check if an object is serializable using either pickle or dill.

        Args:
            obj: Object to check for serializability

        Returns:
            Tuple[bool, str]: (is_serializable, serializer_used)
            where serializer_used is either 'pickle', 'dill', or 'none'
        """
        # Try pickle first
        try:
            pickle.dumps(obj)
            return True, "pickle"
        except Exception as e:
            self.logger.debug(f"Pickle serialization failed: {str(e)}")

            # Try dill if pickle fails
            try:
                dill.dumps(obj)
                return True, "dill"
            except Exception as e:
                self.logger.debug(f"Dill serialization failed: {str(e)}")
                return False, "none"

    def _serialize_object(self, obj: Any) -> Tuple[bytes, str]:
        """
        Attempt to serialize an object using either pickle or dill.

        Args:
            obj: Object to serialize

        Returns:
            Tuple[bytes, str]: (serialized_data, serializer_used)

        Raises:
            ValueError: If object cannot be serialized
        """
        is_serializable, serializer = self.is_serializable(obj)

        if not is_serializable:
            raise ValueError("Object cannot be serialized with either pickle or dill")

        if serializer == "pickle":
            return pickle.dumps(obj), "pickle"
        else:  # serializer == 'dill'
            return dill.dumps(obj), "dill"

    def _deserialize_object(self, data: bytes, serializer: str) -> Any:
        """
        Deserialize an object using the specified serializer.

        Args:
            data: Serialized object data
            serializer: Serializer used ('pickle' or 'dill')

        Returns:
            Any: Deserialized object
        """
        if serializer == "pickle":
            return pickle.loads(data)
        elif serializer == "dill":
            return dill.loads(data)
        else:
            raise ValueError(f"Unknown serializer: {serializer}")

    def _setup_logging(self):
        """Configure logging for the model manager"""
        log_path = os.path.join(self.base_path, "model_manager.log")
        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger("ModelManager")

    def _validate_name(self, name: str) -> bool:
        """
        Validate model name for filesystem compatibility

        Args:
            name: Model name to validate

        Returns:
            bool: True if valid, raises ValueError if invalid
        """
        if not re.match(r"^[\w\-\.]+$", name):
            raise ValueError(
                "Invalid model name. Use only letters, numbers, underscore, dash, and dot."
            )
        return True

    def _validate_metadata(self, metadata: Dict) -> bool:
        """
        Validate metadata format

        Args:
            metadata: Metadata dictionary to validate

        Returns:
            bool: True if valid, raises ValueError if invalid
        """
        required_keys = {"name", "saved_at", "filename"}
        if not all(key in metadata for key in required_keys):
            raise ValueError(f"Metadata must contain keys: {required_keys}")
        return True

    def _check_disk_space(self, required_mb: int = 100) -> bool:
        """
        Check if sufficient disk space is available

        Args:
            required_mb: Required space in MB

        Returns:
            bool: True if sufficient space available
        """
        total, used, free = shutil.disk_usage(self.base_path)
        free_mb = free // (2**20)
        if free_mb < required_mb:
            raise RuntimeError(
                f"Insufficient disk space. Required: {required_mb}MB, Available: {free_mb}MB"
            )
        return True

    def _cleanup_old_versions(self, name: str):
        """
        Remove old versions of a model beyond max_versions

        Args:
            name: Name of the model to cleanup
        """
        files = [
            f
            for f in os.listdir(self.base_path)
            if f.startswith(name) and f.endswith(".pkl")
        ]
        files.sort(reverse=True)

        # Remove old versions
        for old_file in files[self.max_versions :]:
            old_path = os.path.join(self.base_path, old_file)
            metadata_file = old_file.replace(".pkl", "_metadata.json")
            metadata_path = os.path.join(self.base_path, metadata_file)

            try:
                os.remove(old_path)
                if os.path.exists(metadata_path):
                    os.remove(metadata_path)
                self.logger.info(f"Removed old version: {old_file}")
            except Exception as e:
                self.logger.error(f"Error removing old version {old_file}: {str(e)}")

    def save_model(self, model: Any, name: str, metadata: Optional[Dict] = None) -> str:
        """
        Save a model with metadata

        Args:
            model: The model object to save
            name: Name identifier for the model
            metadata: Optional dictionary of metadata about the model

        Returns:
            str: Path where model was saved
        """
        try:
            # Validate inputs
            self._validate_name(name)
            self._check_disk_space()

            # Check serializability
            is_serializable, serializer = self.is_serializable(model)
            if not is_serializable:
                raise ValueError("Model object is not serializable")

            # Create timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Create filename
            filename = f"{name}_{timestamp}.pkl"
            filepath = os.path.join(self.base_path, filename)

            # Prepare metadata
            metadata = metadata or {}
            metadata.update(
                {
                    "name": name,
                    "saved_at": timestamp,
                    "filename": filename,
                    "file_size": 0,  # Will update after saving
                    "saved_by": os.getenv("USER", "unknown"),
                    "serializer": serializer,  # Track which serializer was used
                }
            )

            # Serialize and save model
            model_data, used_serializer = self._serialize_object(model)
            metadata["serializer"] = used_serializer

            with open(filepath, "wb") as f:
                if used_serializer == "pickle":
                    pickle.dump({"model": model, "metadata": metadata}, f)
                else:  # used_serializer == 'dill'
                    dill.dump({"model": model, "metadata": metadata}, f)

            # Update metadata with file size
            metadata["file_size"] = os.path.getsize(filepath)

            # Save separate metadata file
            metadata_path = os.path.join(
                self.base_path, f"{name}_{timestamp}_metadata.json"
            )
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=4)

            # Cleanup old versions
            self._cleanup_old_versions(name)

            self.logger.info(
                f"Successfully saved model: {filename} using {used_serializer}"
            )
            return filepath

        except Exception as e:
            self.logger.error(f"Error saving model {name}: {str(e)}")
            raise

    def load_model(self, name: str, version: Optional[str] = None) -> tuple[Any, Dict]:
        """
        Load a model and its metadata

        Args:
            name: Name of the model to load
            version: Specific version timestamp (if None, loads most recent)

        Returns:
            tuple: (model object, metadata dictionary)
        """
        try:
            self._validate_name(name)

            # List all matching model files
            files = [
                f
                for f in os.listdir(self.base_path)
                if f.startswith(name) and f.endswith(".pkl")
            ]

            if not files:
                raise FileNotFoundError(f"No models found with name {name}")

            if version:
                filename = f"{name}_{version}.pkl"
                if filename not in files:
                    raise FileNotFoundError(
                        f"Version {version} not found for model {name}"
                    )
            else:
                # Get most recent version
                files.sort(reverse=True)
                filename = files[0]

            filepath = os.path.join(self.base_path, filename)

            # Load metadata first to determine serializer
            metadata_path = filepath.replace(".pkl", "_metadata.json")
            with open(metadata_path, "r") as f:
                metadata = json.load(f)

            serializer = metadata.get(
                "serializer", "pickle"
            )  # Default to pickle for backward compatibility

            # Load model using appropriate serializer
            with open(filepath, "rb") as f:
                if serializer == "pickle":
                    data = pickle.load(f)
                else:  # serializer == 'dill'
                    data = dill.load(f)

            # Log model access
            self.logger.info(f"Loaded model: {filename} using {serializer}")

            return data["model"], data["metadata"]

        except Exception as e:
            self.logger.error(f"Error loading model {name}: {str(e)}")
            raise

    def list_models(self, name_filter: Optional[str] = None) -> pd.DataFrame:
        """
        List all available models and their metadata

        Args:
            name_filter: Optional filter for model names

        Returns:
            pd.DataFrame: DataFrame containing model information
        """
        try:
            metadata_files = [
                f for f in os.listdir(self.base_path) if f.endswith("_metadata.json")
            ]

            if name_filter:
                metadata_files = [
                    f for f in metadata_files if f.startswith(name_filter)
                ]

            metadata_list = []
            for mf in metadata_files:
                with open(os.path.join(self.base_path, mf), "r") as f:
                    metadata_list.append(json.load(f))

            return pd.DataFrame(metadata_list)

        except Exception as e:
            self.logger.error(f"Error listing models: {str(e)}")
            raise

    def delete_model(self, name: str, version: Optional[str] = None) -> List[str]:
        """
        Delete a model and its metadata

        Args:
            name: Name of the model to delete
            version: Specific version to delete (if None, deletes all versions)

        Returns:
            List[str]: List of deleted files
        """
        try:
            self._validate_name(name)
            deleted_files = []

            if version:
                model_file = f"{name}_{version}.pkl"
                metadata_file = f"{name}_{version}_metadata.json"
                files_to_delete = [model_file, metadata_file]
            else:
                files_to_delete = [
                    f
                    for f in os.listdir(self.base_path)
                    if f.startswith(name)
                    and (f.endswith(".pkl") or f.endswith("_metadata.json"))
                ]

            for file in files_to_delete:
                file_path = os.path.join(self.base_path, file)
                if os.path.exists(file_path):
                    os.remove(file_path)
                    deleted_files.append(file)
                    self.logger.info(f"Deleted file: {file}")

            return deleted_files

        except Exception as e:
            self.logger.error(f"Error deleting model {name}: {str(e)}")
            raise

    def get_storage_summary(self) -> Dict:
        """
        Get summary of storage usage

        Returns:
            Dict: Storage usage information
        """
        try:
            total, used, free = shutil.disk_usage(self.base_path)
            model_files = [f for f in os.listdir(self.base_path) if f.endswith(".pkl")]

            return {
                "total_space_mb": total // (2**20),
                "used_space_mb": used // (2**20),
                "free_space_mb": free // (2**20),
                "model_count": len(model_files),
                "average_model_size_mb": (
                    sum(
                        os.path.getsize(os.path.join(self.base_path, f))
                        for f in model_files
                    )
                    // (2**20)
                    / len(model_files)
                    if model_files
                    else 0
                ),
            }

        except Exception as e:
            self.logger.error(f"Error getting storage summary: {str(e)}")
            raise

    def save_model_to_gcs(
        self,
        model: Any,
        name: str,
        results: Optional[Dict] = None,
        bucket_name: str = None,
        sport: str = "cfb",
        metadata: Optional[Dict] = None,
    ) -> Dict[str, str]:
        """
        Save a model and its results to Google Cloud Storage with production/archive structure

        Models are saved to models/production/ without timestamps.
        Existing models in production are moved to models/archive/ with timestamps.

        Args:
            model: The model object to save
            name: Name identifier for the model
            results: Optional dictionary of model results/performance metrics
            bucket_name: GCS bucket name (defaults to sport bucket from constants)
            sport: Sport identifier for bucket selection (default: "cfb")
            metadata: Optional dictionary of additional metadata

        Returns:
            Dict[str, str]: Dictionary with GCS paths for saved objects
        """
        try:
            # Validate inputs
            self._validate_name(name)

            # Get bucket name
            if bucket_name is None:
                bucket_name = SPORT_BUCKET.get(sport)
                if bucket_name is None:
                    raise ValueError(f"No bucket configured for sport: {sport}")

            # Create GCS client
            client = create_client(DFS_SIM_CREDS)
            bucket = client.bucket(bucket_name)

            # Create timestamp for archiving and metadata
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Check if model is serializable
            is_serializable, serializer = self.is_serializable(model)
            if not is_serializable:
                raise ValueError("Model object is not serializable")

            # Define production and archive paths (no timestamp in production)
            production_model_path = f"models/production/{name}_model.pkl"
            production_results_path = f"models/production/{name}_results.json"
            production_metadata_path = f"models/production/{name}_metadata.json"

            # Check if existing model exists in production and move to archive
            archived_paths = self._archive_existing_model(bucket, name, timestamp)

            # Prepare base metadata
            base_metadata = {
                "name": name,
                "saved_at": timestamp,
                "saved_by": os.getenv("USER", "unknown"),
                "serializer": serializer,
                "sport": sport,
                "bucket": bucket_name,
                "status": "production",
            }

            # Merge with additional metadata
            if metadata:
                base_metadata.update(metadata)

            # Serialize model data
            model_data, used_serializer = self._serialize_object(model)
            base_metadata["serializer"] = used_serializer

            # Store the serialized model bytes directly in GCS
            model_blob = bucket.blob(production_model_path)
            model_blob.upload_from_string(
                model_data, content_type="application/octet-stream"
            )
            self.logger.info(f"Stored model data at {production_model_path}")

            stored_objects = {"model": f"gs://{bucket_name}/{production_model_path}"}

            # Store results if provided
            if results is not None:
                results_data = {
                    "results": results,
                    "metadata": base_metadata,
                    "model_name": name,
                    "timestamp": timestamp,
                }

                store_object(
                    object_name=production_results_path,
                    bucket_name=bucket_name,
                    data=results_data,
                    client=client,
                )

                stored_objects["results"] = (
                    f"gs://{bucket_name}/{production_results_path}"
                )

            # Store metadata as JSON
            store_object(
                object_name=production_metadata_path,
                bucket_name=bucket_name,
                data=base_metadata,
                client=client,
            )

            stored_objects["metadata"] = (
                f"gs://{bucket_name}/{production_metadata_path}"
            )

            # Add archived paths to response if any were archived
            if archived_paths:
                stored_objects["archived"] = archived_paths

            self.logger.info(
                f"Successfully saved model {name} to GCS production: {stored_objects}"
            )

            return stored_objects

        except Exception as e:
            self.logger.error(f"Error saving model {name} to GCS: {str(e)}")
            raise

    def _archive_existing_model(
        self, bucket: storage.Bucket, name: str, timestamp: str
    ) -> Dict[str, str]:
        """
        Move existing model from production to archive with timestamp

        Args:
            bucket: GCS bucket object
            name: Model name
            timestamp: Timestamp to add to archived files

        Returns:
            Dict[str, str]: Dictionary with paths of archived objects
        """
        archived_paths = {}

        # Define the files to check and archive
        files_to_archive = [
            (
                f"models/production/{name}_model.pkl",
                f"models/archive/{name}_{timestamp}_model.pkl",
            ),
            (
                f"models/production/{name}_results.json",
                f"models/archive/{name}_{timestamp}_results.json",
            ),
            (
                f"models/production/{name}_metadata.json",
                f"models/archive/{name}_{timestamp}_metadata.json",
            ),
        ]

        for production_path, archive_path in files_to_archive:
            try:
                # Check if the production file exists
                production_blob = bucket.blob(production_path)
                if production_blob.exists():
                    # Get the existing object's creation time for the archive metadata
                    production_blob.reload()
                    created_time = production_blob.time_created.strftime(
                        "%Y%m%d_%H%M%S"
                    )

                    # Update archive path to use creation time instead of current timestamp
                    base_filename = os.path.basename(archive_path)
                    file_type_with_ext = base_filename.split("_")[
                        -1
                    ]  # e.g., 'model.pkl', 'results.json'
                    archive_path_with_created_time = (
                        f"models/archive/{name}_{created_time}_{file_type_with_ext}"
                    )

                    # Copy to archive location with creation timestamp
                    archive_blob = bucket.blob(archive_path_with_created_time)
                    archive_blob.upload_from_string(
                        production_blob.download_as_bytes(),
                        content_type=production_blob.content_type,
                    )

                    # Update metadata to indicate it's archived
                    if archive_path_with_created_time.endswith("_metadata.json"):
                        try:
                            # Get existing metadata and update status
                            existing_metadata = retrieve_object(
                                production_path, bucket.name
                            )
                            if existing_metadata:
                                existing_metadata["status"] = "archived"
                                existing_metadata["archived_at"] = timestamp
                                archive_blob.upload_from_string(
                                    json.dumps(existing_metadata),
                                    content_type="application/json",
                                )
                        except Exception as e:
                            self.logger.warning(
                                f"Could not update metadata for archived file: {e}"
                            )

                    # Delete from production
                    production_blob.delete()

                    # Track archived path
                    filename = os.path.basename(production_path)
                    file_key = (
                        filename.rsplit("_", 1)[-1]
                        .replace(".json", "")
                        .replace(".pkl", "")
                    )  # model, results, metadata
                    archived_paths[file_key] = (
                        f"gs://{bucket.name}/{archive_path_with_created_time}"
                    )

                    self.logger.info(
                        f"Archived {production_path} to {archive_path_with_created_time}"
                    )

            except Exception as e:
                self.logger.warning(f"Could not archive {production_path}: {str(e)}")
                continue

        return archived_paths

    def load_model_from_gcs(
        self,
        name: str,
        bucket_name: str = None,
        sport: str = "cfb",
        load_results: bool = True,
    ) -> ModelLoadResult:
        """
        Load a model and its metadata from Google Cloud Storage production folder

        Args:
            name: Name of the model to load
            bucket_name: GCS bucket name (defaults to sport bucket from constants)
            sport: Sport identifier for bucket selection (default: "cfb")
            load_results: Whether to also load results data (default: True)

        Returns:
            ModelLoadResult: Dictionary with 'model', 'metadata', and 'results' keys
        """
        try:
            # Validate inputs
            self._validate_name(name)

            # Get bucket name
            if bucket_name is None:
                bucket_name = SPORT_BUCKET.get(sport)
                if bucket_name is None:
                    raise ValueError(f"No bucket configured for sport: {sport}")

            # Create GCS client
            client = create_client(DFS_SIM_CREDS)
            bucket = client.bucket(bucket_name)

            # Define production paths
            production_model_path = f"models/production/{name}_model.pkl"
            production_results_path = f"models/production/{name}_results.json"
            production_metadata_path = f"models/production/{name}_metadata.json"

            # Load model data directly as bytes
            model_blob = bucket.blob(production_model_path)
            if not model_blob.exists():
                raise FileNotFoundError(
                    f"Model '{name}' not found in production folder"
                )

            model_bytes = model_blob.download_as_bytes()

            # Load metadata to get serializer type
            metadata = retrieve_object(production_metadata_path, bucket_name, client)
            if metadata is None:
                self.logger.warning(
                    f"Could not load metadata for model '{name}', defaulting to pickle"
                )
                serializer = "pickle"
            else:
                serializer = metadata.get("serializer", "pickle")

            # Deserialize the model
            model = self._deserialize_object(model_bytes, serializer)

            # Load metadata
            metadata = retrieve_object(production_metadata_path, bucket_name, client)
            if metadata is None:
                self.logger.warning(
                    f"Could not load metadata for model '{name}', using basic metadata"
                )
                metadata = {
                    "name": name,
                    "status": "production",
                    "loaded_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
                }

            # Load results if requested and available
            results = None
            if load_results:
                results_blob = bucket.blob(production_results_path)
                if results_blob.exists():
                    results_data = retrieve_object(
                        production_results_path, bucket_name, client
                    )
                    if results_data:
                        results = results_data.get("results")
                else:
                    self.logger.info(f"No results file found for model '{name}'")

            self.logger.info(
                f"Successfully loaded model '{name}' from GCS production using {serializer}"
            )

            return ModelLoadResult(model=model, metadata=metadata, results=results)

        except Exception as e:
            self.logger.error(f"Error loading model '{name}' from GCS: {str(e)}")
            raise
