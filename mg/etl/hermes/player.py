"""Player cartographer for mapping external player IDs to internal entities."""

from typing import TYPE_CHECKING, Any, Optional
import logging
import uuid

from mg.etl.hermes.base import Cartographer
from mg.etl.lexis import (
    normalize_name,
    name_similarity,
    split_name_parts,
)

if TYPE_CHECKING:
    from mg.logging.logger_manager import LoggerManager

logging.basicConfig(level=logging.INFO)


class PlayerCartographer(Cartographer):
    """Cartographer for external player IDs to internal player entities.

    Matches players by:
    1. Exact data_source_id lookup (cached)
    2. Exact normalized name
    3. Name + team/position filter
    4. Fuzzy name similarity (configurable threshold)
    """

    SOURCE_MAP_TABLE = "player_source_map"
    ENTITY_TABLE = "players"
    ENTITY_ID_COLUMN = "id"

    def __init__(
        self,
        data_source: str,
        db_name: str,
        schema: str = "core",
        position_mapping: Optional[dict[str, str]] = None,
        similarity_threshold: float = 0.85,
        team_cartographer: Optional[Any] = None,
        logger: Optional["LoggerManager"] = None,
        debug: bool = False,
    ):
        """Initialize the PlayerCartographer.

        Args:
            data_source: Data source identifier
            db_name: Database name
            schema: Database schema
            position_mapping: Dict mapping source positions to internal positions
            similarity_threshold: Minimum similarity score for fuzzy matching
            team_cartographer: Optional TeamCartographer for mapping teams
            logger: Optional LoggerManager instance for structured logging
            debug: Enable debug logging
        """
        self.position_mapping = position_mapping or {}
        self.similarity_threshold = similarity_threshold
        self.team_cartographer = team_cartographer
        super().__init__(data_source, db_name, schema, logger, debug)

    def _build_indices(self) -> None:
        """Build lookup indices for efficient name matching."""
        self._by_normalized_name: dict[str, list[dict]] = {}
        self._by_last_initial: dict[str, list[dict]] = {}

        for player in self.entities:
            # Index by normalized full name
            full_name = self._get_full_name(player)
            if full_name:
                normalized = normalize_name(full_name)
                if normalized not in self._by_normalized_name:
                    self._by_normalized_name[normalized] = []
                self._by_normalized_name[normalized].append(player)

            # Index by last name initial
            last_name = player.get("lastname") or player.get("last_name")
            if last_name:
                initial = last_name[0].lower()
                if initial not in self._by_last_initial:
                    self._by_last_initial[initial] = []
                self._by_last_initial[initial].append(player)

    def _get_full_name(self, player: dict) -> Optional[str]:
        """Extract full name from player dict."""
        if player.get("fullname"):
            return player["fullname"]
        if player.get("full_name"):
            return player["full_name"]

        first = player.get("firstname") or player.get("first_name") or ""
        last = player.get("lastname") or player.get("last_name") or ""
        if first and last:
            return f"{first} {last}"
        return None

    def map(
        self,
        data_source_id: str,
        name: Optional[str] = None,
        team: Optional[str] = None,
        team_id: Optional[str] = None,
        position: Optional[str] = None,
    ) -> Optional[dict]:
        """Map a player by source ID or name/team/position.

        Args:
            data_source_id: External source identifier (required)
            name: Player name
            team: Team name/abbreviation
            team_id: Internal team ID (from TeamCartographer)
            position: Position

        Returns:
            Matched player dict or None
        """
        # Normalize data_source_id to string
        data_source_id = str(data_source_id)

        # Step 1: Check cache
        if data_source_id:
            cached = self._lookup_cached(data_source_id)
            if cached:
                self._log(f"Cache hit: data_source_id={data_source_id}")
                return cached

        if not name:
            self._log(f"No name provided for data_source_id={data_source_id}")
            return None

        # Normalize inputs
        normalized_name = normalize_name(name)
        normalized_pos = position if position else None

        # Step 2: Exact normalized name match
        exact_matches = self._by_normalized_name.get(normalized_name, [])

        if len(exact_matches) == 1:
            player = exact_matches[0]
            log_info = {"method": "exact_name", "input_name": name}
            self._add_mapping(data_source_id, player, confidence_rating=100, log_info=log_info)
            self._log(f"Exact name match: {name}")
            return player

        # Step 3: Multiple matches - filter by team/position
        if len(exact_matches) > 1:
            player = self._filter_by_team_position(
                exact_matches, team, team_id, normalized_pos
            )
            if player:
                log_info = {
                    "method": "exact_name_filtered",
                    "input_name": name,
                    "team": team,
                    "team_id": team_id,
                    "position": position,
                }
                self._add_mapping(data_source_id, player, confidence_rating=95, log_info=log_info)
                self._log(f"Exact name + filter: {name}")
                return player

        # Step 4: Fuzzy matching by last name initial
        if len(exact_matches) == 0:
            first_name, last_name = split_name_parts(name)
            if last_name:
                initial = last_name[0].lower()
                candidates = self._by_last_initial.get(initial, [])

                # Filter by team_id first (most reliable), then team name
                if team_id:
                    candidates = [
                        p for p in candidates
                        if str(p.get("team_id", "")) == str(team_id)
                    ]
                elif team:
                    candidates = [
                        p for p in candidates
                        if p.get("team") == team
                    ]

                # Find best fuzzy match
                best_match = None
                best_similarity = 0.0
                for player in candidates:
                    player_name = self._get_full_name(player)
                    if player_name:
                        similarity = name_similarity(name, player_name)
                        if similarity > best_similarity and similarity >= self.similarity_threshold:
                            best_similarity = similarity
                            best_match = player

                if best_match:
                    # Convert similarity (0-1) to confidence rating (0-100)
                    confidence_rating = int(best_similarity * 100)
                    log_info = {
                        "method": "fuzzy_name",
                        "input_name": name,
                        "similarity": round(best_similarity, 3),
                    }
                    self._add_mapping(data_source_id, best_match, confidence_rating=confidence_rating, log_info=log_info)
                    self._log(f"Fuzzy match: {name} (confidence={confidence_rating})")
                    return best_match

        # No match found
        self._log(
            f"Cannot map player: data_source={self.data_source}, "
            f"data_source_id={data_source_id}, name={name}, team={team}",
            level="warning",
        )
        return None

    def _filter_by_team_position(
        self,
        candidates: list[dict],
        team: Optional[str],
        team_id: Optional[str],
        position: Optional[str],
    ) -> Optional[dict]:
        """Filter candidates by team and/or position."""
        # Filter by team_id first (most reliable)
        if team_id:
            team_matches = [
                p for p in candidates
                if str(p.get("team_id", "")) == str(team_id)
            ]
            if len(team_matches) == 1:
                return team_matches[0]
            candidates = team_matches if team_matches else candidates
        elif team:
            team_matches = [
                p for p in candidates
                if p.get("team") == team or p.get("team_abbrev") == team
            ]
            if len(team_matches) == 1:
                return team_matches[0]
            candidates = team_matches if team_matches else candidates

        if position:
            pos_matches = [
                p for p in candidates
                if p.get("position") == position
            ]
            if len(pos_matches) == 1:
                return pos_matches[0]

        return None

    def get_or_create(
        self,
        data_source_id: str,
        player_name: Optional[str] = None,
        team_id: Optional[str] = None,
        team_name: Optional[str] = None,
        position: Optional[str] = None,
        **kwargs,
    ) -> dict:
        """Get existing player or create a new one.

        Args:
            data_source_id: External source identifier (required)
            player_name: Player name/gamertag
            team_id: Internal team ID (from TeamCartographer)
            team_name: Internal team name/abbreviation
            position: Player position/role
            **kwargs: Additional player fields (data_source_team_id, source_team, rating, etc.)

        Returns:
            Player dict with ID (existing or newly created)
        """
        data_source_id = str(data_source_id)

        # Try to find existing player
        existing = self.map(
            data_source_id=data_source_id,
            name=player_name,
            team=team_name,
            team_id=team_id,
            position=position,
        )

        if existing:
            player_id = existing["id"]
            self._log(f"Found existing player: {data_source_id} -> {player_id}")
        else:
            player_id = uuid.uuid4()
            self._log(f"Creating new player: {data_source_id} -> {player_id}")

        # Fields to exclude from clean entity table (stored in source_map instead)
        source_fields = {"data_source_team_id", "source_team"}

        # Build player entity (clean, without source-specific fields)
        player = {
            "id": player_id,
            "player_name": player_name.strip() if player_name else None,
            "team_id": team_id,
            "team_name": team_name.strip() if team_name else None,
            "position": position,
            **{k: v for k, v in kwargs.items() if k not in source_fields},
            "data_source": self.data_source,
        }

        # Remove None values
        player = {k: v for k, v in player.items() if v is not None}

        # Add to cache and pending entities
        self.cache[data_source_id] = player
        self._pending_entities.append(player)

        # Add mapping to pending (for source_map table)
        if not existing:
            log_info = {
                "method": "get_or_create",
                "player_name": player_name,
                "data_source_team_id": kwargs.get("data_source_team_id"),
                "source_team": kwargs.get("source_team"),
            }
            self._add_mapping(data_source_id, player, confidence_rating=100, log_info=log_info)

        return player
