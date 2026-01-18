"""Base class for entity mapping between external sources and internal IDs."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional
from datetime import datetime
import json
import logging

from mg.db.postgres_manager import PostgresManager

if TYPE_CHECKING:
    from mg.logging.logger_manager import LoggerManager

logging.basicConfig(level=logging.INFO)


class Cartographer(ABC):
    """Base class for mapping external source IDs to internal entity IDs.

    Provides common functionality for looking up and caching mappings between
    external data source identifiers and internal master entity IDs.

    Attributes:
        source: The data source identifier (e.g., "draftkings", "fanduel")
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
        source: str,
        db_name: str,
        schema: str = "core",
        logger: Optional["LoggerManager"] = None,
        debug: bool = False,
    ):
        """Initialize the mapper.

        Args:
            source: Data source identifier (e.g., "draftkings", "fanduel")
            db_name: Database name (e.g., "nfl", "nba")
            schema: Database schema (default: "core")
            logger: Optional LoggerManager instance for structured logging
            debug: Enable debug logging
        """
        self.source = source
        self.db_name = db_name
        self.schema = schema
        self.logger = logger
        self.debug = debug

        self.pgm = PostgresManager(
            host="digital_ocean",
            database=self.db_name,
            schema=self.schema,
        )

        # In-memory cache: source_id -> entity dict
        self.cache: dict[str, dict] = {}

        # Entities available for matching (not yet mapped)
        self.entities: list[dict] = []

        # Pending mappings to save
        self._pending: list[dict] = []

        # Load existing mappings and entities
        self._load_cache()
        self._load_entities()

    def _load_cache(self) -> None:
        """Load existing mappings from the map table into cache."""
        query = f"""
            SELECT m.source_id, m.entity_id, m.log_info, e.*
            FROM {self.SOURCE_MAP_TABLE} m
            JOIN {self.ENTITY_TABLE} e ON m.entity_id = e.{self.ENTITY_ID_COLUMN}
            WHERE m.source = '{self.source}'
        """
        rows = self.pgm.execute(query)
        for row in rows:
            self.cache[str(row["source_id"])] = row

    def _load_entities(self) -> None:
        """Load entities not already mapped for this source."""
        query = f"""
            SELECT * FROM {self.ENTITY_TABLE}
            WHERE {self.ENTITY_ID_COLUMN} NOT IN (
                SELECT entity_id FROM {self.SOURCE_MAP_TABLE}
                WHERE source = '{self.source}'
            )
        """
        self.entities = self.pgm.execute(query)
        self._build_indices()

    def _build_indices(self) -> None:
        """Build lookup indices for efficient matching.

        Subclasses can override this to build entity-specific indices.
        """
        pass

    @abstractmethod
    def map(self, source_id: Optional[str] = None, **kwargs) -> Optional[dict]:
        """Map a source identifier to an internal entity.

        Args:
            source_id: The external source identifier
            **kwargs: Additional matching criteria (name, team, etc.)

        Returns:
            The matched entity dict, or None if no match found
        """
        pass

    def _lookup_cached(self, source_id: str) -> Optional[dict]:
        """Look up a source_id in the cache.

        Args:
            source_id: The source identifier to look up

        Returns:
            Cached entity dict or None
        """
        return self.cache.get(str(source_id))

    def _add_mapping(
        self,
        source_id: str,
        entity: dict,
        confidence_rating: int = 100,
        log_info: Optional[dict] = None,
    ) -> None:
        """Add a new mapping to cache and pending list.

        Args:
            source_id: The external source identifier
            entity: The matched entity dict
            confidence_rating: Confidence confidence_rating 0-100 (100 = exact match)
            log_info: Information about how the match was made
        """
        source_id = str(source_id)
        entity["log_info"] = log_info or {}
        entity["confidence_rating"] = confidence_rating
        self.cache[source_id] = entity

        self._pending.append({
            "source": self.source,
            "source_id": source_id,
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

        result = self.pgm.insert_rows(
            self.SOURCE_MAP_TABLE,
            ["source", "source_id", "entity_id", "confidence_rating", "log_info"],
            self._pending,
            contains_dicts=True,
            update=True,
        )
        if result:
            self._pending = []
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
        if self.logger:
            self.logger.close_logger()
