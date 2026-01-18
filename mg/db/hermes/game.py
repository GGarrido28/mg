"""Source game data model."""

from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, date

from mg.db.hermes.base import SourceEntity


@dataclass
class SourceGame(SourceEntity):
    """Standardized game data from external sources.

    Use this class to normalize game data from any source before
    mapping to internal entities via GameCartographer.
    """

    # Team IDs
    away_team_id: Optional[str] = None
    home_team_id: Optional[str] = None
    source_away_team_id: Optional[str] = None  # Team IDs from source
    source_home_team_id: Optional[str] = None  # Team IDs from source

    # Teams
    away_team: Optional[str] = None
    home_team: Optional[str] = None
    source_away_team: Optional[str] = None  # Team names from source
    source_home_team: Optional[str] = None  # Team names from source

    # Timing
    start_time: Optional[datetime] = None  # Full datetime
    game_date: Optional[date] = None  # Just the date
    timezone: Optional[str] = None  # Source timezone (e.g., "UTC", "EST")

    # Status
    status: Optional[str] = None  # "scheduled", "in_progress", "final", etc.
    period: Optional[str] = None  # Current period/quarter/half
    clock: Optional[str] = None  # Game clock

    # Scores
    away_score: Optional[int] = None
    home_score: Optional[int] = None

    # Venue
    venue: Optional[str] = None  # Stadium/arena name
    venue_city: Optional[str] = None
    venue_state: Optional[str] = None
    is_neutral_site: bool = False

    # Additional context
    season: Optional[int] = None  # Season year
    season_type: Optional[str] = None  # "regular", "preseason", "playoffs"
    week: Optional[int] = None  # Week number (for NFL, etc.)
    game_number: Optional[int] = None  # Game in series (for MLB, etc.)
    ppd: bool = False  # Postponed flag
    
    # Broadcast
    broadcast: Optional[str] = None  # TV network
    broadcast_networks: list[str] = field(default_factory=list)

    # Weather (outdoor sports)
    weather: Optional[str] = None
    temperature: Optional[int] = None  # Fahrenheit
    wind: Optional[str] = None
    dome: bool = False

    def __post_init__(self):
        """Normalize game data."""
        super().__post_init__()

        # Clean up team names
        if self.away_team:
            self.away_team = self.away_team.strip()
        if self.home_team:
            self.home_team = self.home_team.strip()

        # Ensure team IDs are strings
        if self.away_team_id is not None:
            self.away_team_id = str(self.away_team_id)
        if self.home_team_id is not None:
            self.home_team_id = str(self.home_team_id)

        # Extract date from datetime if not provided
        if self.start_time and not self.game_date:
            self.game_date = self.start_time.date()

    @property
    def matchup(self) -> str:
        """Get matchup string (e.g., 'DAL @ NYG')."""
        away = self.away_team or "TBD"
        home = self.home_team or "TBD"
        return f"{away} @ {home}"

    @property
    def is_complete(self) -> bool:
        """Check if game is finished."""
        if self.status:
            return self.status.lower() in ("final", "complete", "finished", "f")
        return False
