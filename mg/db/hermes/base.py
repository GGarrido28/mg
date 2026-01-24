"""Base class for source data models."""

from dataclasses import dataclass, field, asdict
from typing import Optional, Any
from datetime import datetime
from uuid import UUID
import uuid


@dataclass
class SourceEntity:
    """Base class for all source data entities.

    Provides common fields and utilities for source data ingestion.
    """

    # Required fields
    data_source: str  # Data source identifier (e.g., "draftkings", "espn")
    data_source_id: str  # ID from the source system

    # Auto-generated
    id: UUID = field(default_factory=uuid.uuid4)
    created_at: datetime = field(default_factory=datetime.now)

    # Optional metadata
    raw_data: Optional[dict] = None  # Original source data for debugging

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary, excluding None values and raw_data."""
        data = asdict(self)
        # Remove raw_data from output (keep it internal)
        data.pop("raw_data", None)
        # Remove None values
        return {k: v for k, v in data.items() if v is not None}

    def __post_init__(self):
        """Ensure data_source_id is a string."""
        if self.data_source_id is not None:
            self.data_source_id = str(self.data_source_id)
