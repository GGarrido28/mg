"""Team cartographer for mapping external team IDs to internal entities."""

from typing import TYPE_CHECKING, Optional
import logging

from mg.etl.hermes.base import Cartographer
from mg.etl.lexis import strip_convert_to_lowercase, name_similarity

if TYPE_CHECKING:
    from mg.logging.logger_manager import LoggerManager

logging.basicConfig(level=logging.INFO)


class TeamCartographer(Cartographer):
    """Cartographer for external team IDs to internal team entities.

    Matches teams by:
    1. Exact source_id lookup (cached)
    2. Exact normalized full name (confidence: 100)
    3. Abbreviation match (confidence: 95)
    4. Location match (confidence: 90)
    5. Mascot match (confidence: 85)
    6. Token overlap match (confidence: 80)
    7. Fuzzy similarity match (confidence: based on similarity score)
    """

    SOURCE_MAP_TABLE = "team_source_map"
    ENTITY_TABLE = "teams"
    ENTITY_ID_COLUMN = "id"

    def __init__(
        self,
        source: str,
        db_name: str,
        schema: str = "core",
        team_mapping: Optional[dict[str, str]] = None,
        name_column: str = "team",
        similarity_threshold: float = 0.80,
        logger: Optional["LoggerManager"] = None,
        debug: bool = False,
    ):
        """Initialize the TeamCartographer.

        Args:
            source: Data source identifier
            db_name: Database name
            schema: Database schema
            team_mapping: Dict mapping source team names to internal names
            name_column: Column name for team identifier (team, team_abbrev, etc.)
            similarity_threshold: Minimum similarity score for fuzzy matching
            logger: Optional LoggerManager instance for structured logging
            debug: Enable debug logging
        """
        self.team_mapping = team_mapping or {}
        self.name_column = name_column
        self.similarity_threshold = similarity_threshold
        super().__init__(source, db_name, schema, logger, debug)

    def _normalize_team(self, name: str) -> str:
        """Normalize a team name using the team_mapping."""
        return self.team_mapping.get(name, name)

    def _build_indices(self) -> None:
        """Build lookup indices for team matching."""
        self._by_normalized_name: dict[str, dict] = {}
        self._by_abbreviation: dict[str, dict] = {}
        self._by_location: dict[str, dict] = {}
        self._by_mascot: dict[str, dict] = {}
        self._team_tokens: list[tuple[set[str], dict]] = []

        for team in self.entities:
            # Index by full team name
            name = team.get(self.name_column) or team.get("teamname") or ""
            if name:
                normalized = strip_convert_to_lowercase(name)
                self._by_normalized_name[normalized] = team

                # Build token set for token matching
                tokens = set(name.lower().split())
                self._team_tokens.append((tokens, team))

            # Index by abbreviation
            abbrev = team.get("abbreviation") or team.get("abbrev") or ""
            if abbrev:
                normalized_abbrev = strip_convert_to_lowercase(abbrev)
                self._by_abbreviation[normalized_abbrev] = team

            # Index by location (city/state)
            location = team.get("location") or team.get("city") or ""
            if location:
                normalized_loc = strip_convert_to_lowercase(location)
                self._by_location[normalized_loc] = team

            # Index by mascot
            mascot = team.get("mascot") or team.get("nickname") or ""
            if mascot:
                normalized_mascot = strip_convert_to_lowercase(mascot)
                self._by_mascot[normalized_mascot] = team

    def map(
        self,
        source_id: str,
        name: Optional[str] = None,
    ) -> Optional[dict]:
        """Map a team by source ID or name.

        Args:
            source_id: External source identifier (required)
            name: Team name (full name, location, or mascot)

        Returns:
            Matched team dict or None
        """
        # Normalize source_id to string
        source_id = str(source_id)

        # Check cache
        if source_id:
            cached = self._lookup_cached(source_id)
            if cached:
                self._log(f"Cache hit: source_id={source_id}")
                return cached

        if not name:
            self._log(f"No name provided for source_id={source_id}")
            return None

        # Apply team mapping
        mapped_name = self._normalize_team(name)
        normalized = strip_convert_to_lowercase(mapped_name)

        # Step 1: Exact full name match (confidence: 100)
        team = self._by_normalized_name.get(normalized)
        if team:
            log_info = {"method": "exact_name", "input_name": name}
            self._add_mapping(source_id, team, confidence_rating=100, log_info=log_info)
            self._log(f"Exact name match: {name}")
            return team

        # Step 2: Abbreviation match (confidence: 95)
        team = self._by_abbreviation.get(normalized)
        if team:
            log_info = {"method": "abbreviation", "input_name": name}
            self._add_mapping(source_id, team, confidence_rating=95, log_info=log_info)
            self._log(f"Abbreviation match: {name}")
            return team

        # Step 3: Location match (confidence: 90)
        team = self._by_location.get(normalized)
        if team:
            log_info = {"method": "location", "input_name": name}
            self._add_mapping(source_id, team, confidence_rating=90, log_info=log_info)
            self._log(f"Location match: {name}")
            return team

        # Step 4: Mascot match (confidence: 85)
        team = self._by_mascot.get(normalized)
        if team:
            log_info = {"method": "mascot", "input_name": name}
            self._add_mapping(source_id, team, confidence_rating=85, log_info=log_info)
            self._log(f"Mascot match: {name}")
            return team

        # Step 5: Token overlap match (confidence: 80)
        team = self._match_by_tokens(mapped_name)
        if team:
            log_info = {"method": "token_overlap", "input_name": name}
            self._add_mapping(source_id, team, confidence_rating=80, log_info=log_info)
            self._log(f"Token overlap match: {name}")
            return team

        # Step 6: Fuzzy similarity match
        team, similarity = self._match_by_similarity(mapped_name)
        if team:
            confidence_rating = int(similarity * 100)
            log_info = {
                "method": "fuzzy",
                "input_name": name,
                "similarity": round(similarity, 3),
            }
            self._add_mapping(source_id, team, confidence_rating=confidence_rating, log_info=log_info)
            self._log(f"Fuzzy match: {name} (confidence={confidence_rating})")
            return team

        # No match found
        self._log(
            f"Cannot map team: source={self.source}, "
            f"source_id={source_id}, name={name}",
            level="warning",
        )
        return None

    def _match_by_tokens(self, input_name: str) -> Optional[dict]:
        """Match by token overlap (e.g., 'North Carolina State Wolfpack' matches 'NC State Wolfpack').

        Requires at least 2 matching tokens to be considered a match.
        """
        input_tokens = set(input_name.lower().split())
        if len(input_tokens) < 2:
            return None

        best_match = None
        best_overlap = 0

        for team_tokens, team in self._team_tokens:
            overlap = len(input_tokens & team_tokens)
            # Require at least 2 matching tokens
            if overlap >= 2 and overlap > best_overlap:
                best_overlap = overlap
                best_match = team

        return best_match

    def _match_by_similarity(self, input_name: str) -> tuple[Optional[dict], float]:
        """Match by fuzzy name similarity."""
        best_match = None
        best_similarity = 0.0

        for team in self.entities:
            team_name = team.get(self.name_column) or team.get("teamname") or ""
            if team_name:
                similarity = name_similarity(input_name, team_name)
                if similarity > best_similarity and similarity >= self.similarity_threshold:
                    best_similarity = similarity
                    best_match = team

        return best_match, best_similarity
