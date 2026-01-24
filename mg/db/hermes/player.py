"""Source player data model."""

from dataclasses import dataclass, field
from typing import Optional
from datetime import date

from mg.db.hermes.base import SourceEntity


@dataclass
class SourcePlayer(SourceEntity):
    """Standardized player data from external sources.

    Use this class to normalize player data from any source before
    mapping to internal entities via PlayerCartographer.
    """

    # Name fields
    full_name: Optional[str] = None  # Full name as provided
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    nickname: Optional[str] = None  # Common nickname (e.g., "Megatron", "Prime Time")
    display_name: Optional[str] = None  # Preferred display name

    # Team association
    team: Optional[str] = None  # Universal team name/abbreviation
    team_id: Optional[str] = None  # Universal team ID
    source_team: Optional[str] = None  # Team name/abbreviation from source
    data_source_team_id: Optional[str] = None  # Team ID from source
    
    # League info
    league: Optional[str] = None  # League identifier (e.g., "NFL", "NBA")

    # Position/role
    position: Optional[str] = None  # Primary position
    positions: list[str] = field(default_factory=list)  # All eligible positions
    jersey_number: Optional[int] = None

    # Physical attributes
    height: Optional[str] = None  # Height (format varies by source)
    weight: Optional[int] = None  # Weight in lbs
    handedness: Optional[str] = None  # "L", "R", "S" (switch)

    # Biographical
    birth_date: Optional[date] = None
    birth_place: Optional[str] = None
    college: Optional[str] = None
    country: Optional[str] = None

    # Career info
    draft_year: Optional[int] = None
    draft_round: Optional[int] = None
    draft_pick: Optional[int] = None
    years_experience: Optional[int] = None
    rookie_year: Optional[int] = None

    # Status
    status: Optional[str] = None  # "active", "injured", "inactive", etc.
    injury_status: Optional[str] = None  # Specific injury designation
    is_active: bool = True

    # Media
    headshot_url: Optional[str] = None

    def __post_init__(self):
        """Normalize player data."""
        super().__post_init__()

        # Clean up whitespace
        if self.full_name:
            self.full_name = self.full_name.strip()
        if self.first_name:
            self.first_name = self.first_name.strip()
        if self.middle_name:
            self.middle_name = self.middle_name.strip()
        if self.last_name:
            self.last_name = self.last_name.strip()
        if self.nickname:
            self.nickname = self.nickname.strip()
        if self.team:
            self.team = self.team.strip()
        if self.position:
            self.position = self.position.strip().upper()

    @property
    def full_name(self) -> Optional[str]:
        """Get full name, preferring explicit name field."""
        if self.full_name:
            return self.full_name
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        return self.display_name
