"""Base class for entity mapping between external sources and internal IDs."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional
from datetime import datetime
import json
import logging
import uuid

from mg.db.postgres_manager import PostgresManager

if TYPE_CHECKING:
    from mg.logging.logger_manager import LoggerManager

logging.basicConfig(level=logging.INFO)


class Cartographer(ABC):
    """Base class for mapping external source IDs to internal entity IDs.

    Provides common functionality for looking up and caching mappings between
    external data source identifiers and internal master entity IDs.

    Attributes:
        data_source: The data source identifier (e.g., "draftkings", "fanduel")
        db_name: The database name to connect to
        schema: The database schema (default: "core")
        pgm: PostgresManager instance for database operations
        cache: In-memory cache of mapped entities
        debug: Enable debug logging
    """

    # Subclasses must define these
    SOURCE_MAP_TABLE: str = ""
    ENTITY_TABLE: str = ""
    ENTITY_ID_COLUMN: str = ""

    def __init__(
        self,
        data_source: str,
        db_name: str,
        schema: str = "core",
        logger: Optional["LoggerManager"] = None,
        debug: bool = False,
        normalize_cache_keys: bool = False,
    ):
        """Initialize the mapper.

        Args:
            data_source: Data source identifier (e.g., "draftkings", "fanduel")
            db_name: Database name (e.g., "nfl", "nba")
            schema: Database schema (default: "core")
            logger: Optional LoggerManager instance for structured logging
            debug: Enable debug logging (also enables verbose SQL logging)
            normalize_cache_keys: If True, normalize cache keys to lowercase for
                case-insensitive matching (useful when data_source_id can be names)
        """
        self.data_source = data_source
        self.db_name = db_name
        self.schema = schema
        self.logger = logger
        self.debug = debug
        self.normalize_cache_keys = normalize_cache_keys

        # Validate class-level SQL identifiers to prevent injection
        PostgresManager.validate_identifier(self.SOURCE_MAP_TABLE, "SOURCE_MAP_TABLE")
        PostgresManager.validate_identifier(self.ENTITY_TABLE, "ENTITY_TABLE")
        PostgresManager.validate_identifier(self.ENTITY_ID_COLUMN, "ENTITY_ID_COLUMN")

        self.pgm = PostgresManager(
            host="digital_ocean",
            database=self.db_name,
            schema=self.schema,
        )

        # In-memory cache: data_source_id -> entity dict
        self.cache: dict[str, dict] = {}

        # Entities available for matching (not yet mapped)
        self.entities: list[dict] = []

        # Pending mappings to save
        self._pending: list[dict] = []

        # Pending entities to insert
        self._pending_entities: list[dict] = []

        # Load existing mappings and entities
        self._load_cache()
        self._load_entities()

    def _load_cache(self) -> None:
        """Load existing mappings from the map table into cache."""
        # Use alias for m.data_source_id to avoid collision with e.data_source_id
        query = f"""
            SELECT m.data_source_id AS map_data_source_id, m.entity_id, m.log_info, e.*
            FROM {self.schema}.{self.SOURCE_MAP_TABLE} m
            JOIN {self.schema}.{self.ENTITY_TABLE} e ON m.entity_id = e.{self.ENTITY_ID_COLUMN}
            WHERE m.data_source = %(data_source)s
        """
        rows = self.pgm.execute(query, params={"data_source": self.data_source})
        if self.debug:
            logging.info(f"[{self.__class__.__name__}] Loaded {len(rows)} cached mappings for data_source='{self.data_source}'")
        for row in rows:
            key = str(row["map_data_source_id"])
            if self.normalize_cache_keys:
                key = key.lower()
            self.cache[key] = row

    def _load_entities(self) -> None:
        """Load entities not already mapped for this data_source."""
        query = f"""
            SELECT * FROM {self.schema}.{self.ENTITY_TABLE}
            WHERE {self.ENTITY_ID_COLUMN} NOT IN (
                SELECT entity_id FROM {self.schema}.{self.SOURCE_MAP_TABLE}
                WHERE data_source = %(data_source)s
            )
        """
        self.entities = self.pgm.execute(query, params={"data_source": self.data_source})
        self._build_indices()

    def _build_indices(self) -> None:
        """Build lookup indices for efficient matching.

        Subclasses can override this to build entity-specific indices.
        """
        pass

    @abstractmethod
    def map(self, data_source_id: Optional[str] = None, **kwargs) -> Optional[dict]:
        """Map a source identifier to an internal entity.

        Args:
            data_source_id: The external source identifier
            **kwargs: Additional matching criteria (name, team, etc.)

        Returns:
            The matched entity dict, or None if no match found
        """
        pass

    @abstractmethod
    def get_or_create(self, data_source_id: str, **kwargs) -> dict:
        """Get existing entity or create a new one.

        Subclasses should:
        1. Call map() to check for existing entity
        2. If found, use existing ID
        3. If not found, generate new UUID
        4. Build entity dict with standard fields
        5. Add to _pending_entities for later insertion

        Args:
            data_source_id: The external source identifier
            **kwargs: Entity-specific fields

        Returns:
            Entity dict with ID (existing or newly created)
        """
        pass

    def get_pending_entities(self) -> list[dict]:
        """Get list of new entities to insert."""
        return self._pending_entities

    def clear_pending_entities(self) -> None:
        """Clear the pending entities list after insertion."""
        self._pending_entities = []

    def _lookup_cached(self, data_source_id: str) -> Optional[dict]:
        """Look up a data_source_id in the cache.

        Args:
            data_source_id: The source identifier to look up

        Returns:
            Cached entity dict or None
        """
        key = str(data_source_id)
        if self.normalize_cache_keys:
            key = key.lower()
        return self.cache.get(key)

    def _add_mapping(
        self,
        data_source_id: str,
        entity: dict,
        confidence_rating: int = 100,
        log_info: Optional[dict] = None,
    ) -> None:
        """Add a new mapping to cache and pending list.

        Args:
            data_source_id: The external source identifier
            entity: The matched entity dict
            confidence_rating: Confidence rating 0-100 (100 = exact match)
            log_info: Information about how the match was made
        """
        data_source_id = str(data_source_id)
        cache_key = data_source_id.lower() if self.normalize_cache_keys else data_source_id
        self.cache[cache_key] = entity

        self._pending.append({
            "data_source": self.data_source,
            "data_source_id": data_source_id,
            "entity_id": entity[self.ENTITY_ID_COLUMN],
            "confidence_rating": confidence_rating,
            "log_info": json.dumps(log_info or {}),
        })

    def save(self) -> bool:
        """Persist all pending mappings to the database.

        Returns:
            True if save was successful, False otherwise
        """
        if not self._pending:
            return True

        # Ensure the source_map table exists with proper primary key
        if not self.pgm.check_table_exists(self.SOURCE_MAP_TABLE):
            if self.debug:
                logging.info(f"[{self.__class__.__name__}] Creating source_map table: {self.SOURCE_MAP_TABLE}")
            self.pgm.create_table(
                dict_list=self._pending,
                primary_keys=["data_source", "data_source_id"],
                table_name=self.SOURCE_MAP_TABLE,
                delete=False,
            )

        result = self.pgm.insert_rows(
            self.SOURCE_MAP_TABLE,
            ["data_source", "data_source_id", "entity_id", "confidence_rating", "log_info"],
            self._pending,
            contains_dicts=True,
            update=True,
        )
        if result:
            if self.debug:
                logging.info(f"[{self.__class__.__name__}] Saved {len(self._pending)} mappings to {self.SOURCE_MAP_TABLE}")
            self._pending = []
        else:
            logging.error(f"[{self.__class__.__name__}] Failed to save mappings to {self.SOURCE_MAP_TABLE}")
        return result

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message using LoggerManager if available, otherwise standard logging.

        Args:
            message: The message to log
            level: Log level (debug, info, warning, error)
        """
        formatted_msg = f"[{self.__class__.__name__}] {message}"

        if self.logger:
            self.logger.log(level, formatted_msg)
        elif self.debug or level in ("warning", "error"):
            if level == "warning":
                logging.warning(formatted_msg)
            elif level == "error":
                logging.error(formatted_msg)
            else:
                logging.info(formatted_msg)

    def close(self) -> None:
        """Close the database connection and logger if present."""
        self.pgm.close()
        # Only call close_logger() if it exists (custom LoggerManager, not standard Python logger)
        if self.logger and hasattr(self.logger, 'close_logger'):
            self.logger.close_logger()
