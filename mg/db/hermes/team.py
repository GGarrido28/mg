"""Source team data model."""

from dataclasses import dataclass, field
from typing import Optional

from mg.db.hermes.base import SourceEntity


@dataclass
class SourceTeam(SourceEntity):
    """Standardized team data from external sources.

    Use this class to normalize team data from any source before
    mapping to internal entities via TeamCartographer.
    """
    
    # Team identification
    team_name: Optional[str] = None  # Full team name (e.g., "Dallas Cowboys")
    abbreviation: Optional[str] = None  # Short code (e.g., "DAL")
    location: Optional[str] = None  # City/state (e.g., "Dallas")
    mascot: Optional[str] = None  # Team mascot (e.g., "Cowboys")

    # Additional identifiers
    alternate_names: list[str] = field(default_factory=list)  # Other known names
    league: Optional[str] = None  # League identifier (e.g., "NFL", "NBA")
    division: Optional[str] = None  # Division (e.g., "NFC East")
    conference: Optional[str] = None  # Conference (e.g., "NFC")

    # Visual/branding
    logo_url: Optional[str] = None
    primary_color: Optional[str] = None
    secondary_color: Optional[str] = None

    # Status
    is_active: bool = True

    def __post_init__(self):
        """Normalize team data."""
        super().__post_init__()

        # Clean up whitespace
        if self.team_name:
            self.team_name = self.team_name.strip()
        if self.abbreviation:
            self.abbreviation = self.abbreviation.strip().upper()
        if self.location:
            self.location = self.location.strip()
        if self.mascot:
            self.mascot = self.mascot.strip()
