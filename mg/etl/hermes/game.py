"""Game cartographer for mapping external game IDs to internal entities."""

from typing import TYPE_CHECKING, Optional
from datetime import datetime, date, timedelta
import logging

from mg.etl.hermes.base import Cartographer
from mg.etl.chronos import convert_str_to_datetime, convert_to_est

if TYPE_CHECKING:
    from mg.logging.logger_manager import LoggerManager

logging.basicConfig(level=logging.INFO)


class GameCartographer(Cartographer):
    """Cartographer for external game IDs to internal game entities.

    Matches games by:
    1. Exact source_id lookup (cached)
    2. Team IDs + date (if both team IDs provided)
    3. Single team ID + date (if only one team ID matched)
    4. Team names + date (allows swapped team order)
    5. Closest time for same-day multiple games
    """

    SOURCE_MAP_TABLE = "game_source_map"
    ENTITY_TABLE = "games"
    ENTITY_ID_COLUMN = "id"

    def __init__(
        self,
        source: str,
        db_name: str,
        schema: str = "core",
        timezone: Optional[str] = None,
        allow_swapped_teams: bool = True,
        logger: Optional["LoggerManager"] = None,
        debug: bool = False,
    ):
        """Initialize the GameCartographer.

        Args:
            source: Data source identifier
            db_name: Database name
            schema: Database schema
            timezone: Source timezone for time conversion (e.g., "UTC", "PST")
            allow_swapped_teams: If True, match games even if home/away are flipped
            logger: Optional LoggerManager instance for structured logging
            debug: Enable debug logging
        """
        self.timezone = timezone
        self.allow_swapped_teams = allow_swapped_teams
        super().__init__(source, db_name, schema, logger, debug)

    def map(
        self,
        source_id: str,
        away_team: Optional[str] = None,
        home_team: Optional[str] = None,
        away_team_id: Optional[str] = None,
        home_team_id: Optional[str] = None,
        start_time: Optional[datetime | str] = None,
    ) -> Optional[dict]:
        """Map a game by source ID or teams/date.

        Args:
            source_id: External source identifier (required)
            away_team: Away team name/abbreviation
            home_team: Home team name/abbreviation
            away_team_id: Internal away team ID (from TeamCartographer)
            home_team_id: Internal home team ID (from TeamCartographer)
            start_time: Game start time (datetime or string)

        Returns:
            Matched game dict or None
        """
        # Normalize source_id to string
        source_id = str(source_id)

        # Step 1: Check cache
        if source_id:
            cached = self._lookup_cached(source_id)
            if cached:
                self._log(f"Cache hit: source_id={source_id}")
                return cached

        # Need start_time for any matching
        if not start_time:
            self._log("Missing required field: start_time")
            return None

        # Parse and normalize time
        if isinstance(start_time, str):
            game_dt = convert_str_to_datetime(start_time)
        else:
            game_dt = start_time

        if self.timezone:
            game_dt = convert_to_est(game_dt, self.timezone)

        game_date = game_dt.date() if hasattr(game_dt, "date") else game_dt

        # Step 2: Match by team IDs + date (if both team IDs provided)
        if away_team_id and home_team_id:
            matches = self._match_by_team_ids_date(away_team_id, home_team_id, game_date)

            if len(matches) == 1:
                game = matches[0]
                log_info = {
                    "method": "team_ids_date",
                    "away_team_id": away_team_id,
                    "home_team_id": home_team_id,
                    "date": str(game_date),
                }
                self._add_mapping(source_id, game, confidence_rating=100, log_info=log_info)
                self._log(f"Matched by team IDs: {away_team_id} @ {home_team_id} on {game_date}")
                return game

            if len(matches) > 1:
                game = self._match_by_closest_time(matches, game_dt)
                if game:
                    log_info = {
                        "method": "team_ids_date_time",
                        "away_team_id": away_team_id,
                        "home_team_id": home_team_id,
                        "datetime": str(game_dt),
                    }
                    self._add_mapping(source_id, game, confidence_rating=95, log_info=log_info)
                    self._log(f"Matched by team IDs + time: {away_team_id} @ {home_team_id}")
                    return game

        # Step 3: Match by single team ID + date (useful for MMA when only one fighter matched)
        if away_team_id or home_team_id:
            single_team_id = away_team_id or home_team_id
            matches = self._match_by_single_team_id_date(single_team_id, game_date)

            if len(matches) == 1:
                game = matches[0]
                log_info = {
                    "method": "single_team_id_date",
                    "team_id": single_team_id,
                    "date": str(game_date),
                }
                self._add_mapping(source_id, game, confidence_rating=85, log_info=log_info)
                self._log(f"Matched by single team ID: {single_team_id} on {game_date}")
                return game

            if len(matches) > 1:
                game = self._match_by_closest_time(matches, game_dt)
                if game:
                    log_info = {
                        "method": "single_team_id_date_time",
                        "team_id": single_team_id,
                        "datetime": str(game_dt),
                    }
                    self._add_mapping(source_id, game, confidence_rating=80, log_info=log_info)
                    self._log(f"Matched by single team ID + time: {single_team_id}")
                    return game

        # Step 4: Match by team names + date
        if away_team and home_team:
            away = self._normalize_team(away_team)
            home = self._normalize_team(home_team)

            matches = self._match_by_teams_date(away, home, game_date)

            if len(matches) == 1:
                game = matches[0]
                log_info = {
                    "method": "teams_date",
                    "away_team": away_team,
                    "home_team": home_team,
                    "date": str(game_date),
                }
                self._add_mapping(source_id, game, confidence_rating=100, log_info=log_info)
                self._log(f"Matched: {away} @ {home} on {game_date}")
                return game

            if len(matches) > 1:
                game = self._match_by_closest_time(matches, game_dt)
                if game:
                    log_info = {
                        "method": "teams_date_time",
                        "away_team": away_team,
                        "home_team": home_team,
                        "datetime": str(game_dt),
                    }
                    self._add_mapping(source_id, game, confidence_rating=90, log_info=log_info)
                    self._log(f"Matched by time: {away} @ {home}")
                    return game

        # No match found
        self._log(
            f"Cannot map game: source={self.source}, "
            f"source_id={source_id}, {away_team or away_team_id}@{home_team or home_team_id} {start_time}",
            level="warning",
        )
        return None

    def _match_by_team_ids_date(
        self,
        away_team_id: str,
        home_team_id: str,
        game_date: date,
    ) -> list[dict]:
        """Find games matching team IDs and date."""
        matches = []
        for game in self.entities:
            game_away_id = str(game.get("away_team_id", ""))
            game_home_id = str(game.get("home_team_id", ""))
            game_day = game.get("day")

            if game_day is None:
                continue

            # Check date match
            if hasattr(game_day, "date"):
                game_day = game_day.date()
            if game_day != game_date:
                continue

            # Check team IDs
            exact_match = (game_away_id == str(away_team_id) and game_home_id == str(home_team_id))
            swapped_match = (game_away_id == str(home_team_id) and game_home_id == str(away_team_id))
            teams_match = exact_match or (self.allow_swapped_teams and swapped_match)

            if teams_match:
                matches.append(game)

        return matches

    def _match_by_single_team_id_date(
        self,
        team_id: str,
        game_date: date,
    ) -> list[dict]:
        """Find games where one team ID matches on a given date.

        Useful for MMA or other sports where only one participant was matched.
        """
        matches = []
        team_id_str = str(team_id)

        for game in self.entities:
            game_away_id = str(game.get("away_team_id", ""))
            game_home_id = str(game.get("home_team_id", ""))
            game_day = game.get("day")

            if game_day is None:
                continue

            # Check date match
            if hasattr(game_day, "date"):
                game_day = game_day.date()
            if game_day != game_date:
                continue

            # Check if either team ID matches
            if game_away_id == team_id_str or game_home_id == team_id_str:
                matches.append(game)

        return matches

    def _match_by_teams_date(
        self,
        away: str,
        home: str,
        game_date: date,
    ) -> list[dict]:
        """Find games matching team names and date."""
        matches = []
        for game in self.entities:
            game_away = game.get("away_team", "")
            game_home = game.get("home_team", "")
            game_day = game.get("day")

            if game_day is None:
                continue

            # Check date match
            if hasattr(game_day, "date"):
                game_day = game_day.date()
            if game_day != game_date:
                continue

            # Check teams
            exact_match = (game_away.lower() == away.lower() and game_home.lower() == home.lower())
            swapped_match = (game_away.lower() == home.lower() and game_home.lower() == away.lower())
            teams_match = exact_match or (self.allow_swapped_teams and swapped_match)

            if teams_match:
                matches.append(game)

        return matches

    def _match_by_closest_time(
        self,
        candidates: list[dict],
        target_time: datetime,
    ) -> Optional[dict]:
        """Find the game closest to the target time."""
        best_match = None
        best_diff = timedelta.max

        for game in candidates:
            game_time = game.get("datetime") or game.get("date_time")
            if game_time is None:
                continue

            if isinstance(game_time, str):
                game_time = convert_str_to_datetime(game_time)

            # Remove timezone info for comparison
            if hasattr(target_time, "replace"):
                target_naive = target_time.replace(tzinfo=None)
            else:
                target_naive = target_time

            if hasattr(game_time, "replace"):
                game_naive = game_time.replace(tzinfo=None)
            else:
                game_naive = game_time

            diff = abs(game_naive - target_naive)
            if diff < best_diff:
                best_diff = diff
                best_match = game

        # Only return if within 24 hours
        if best_match and best_diff < timedelta(hours=24):
            return best_match

        return None
