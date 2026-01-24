"""Game cartographer for mapping external game IDs to internal entities."""

from typing import TYPE_CHECKING, Any, Optional
from datetime import datetime, date, timedelta
import logging
import uuid

from mg.etl.hermes.base import Cartographer
from mg.etl.chronos import convert_str_to_date, convert_str_to_datetime, convert_to_est, convert_to_utc

if TYPE_CHECKING:
    from mg.logging.logger_manager import LoggerManager

logging.basicConfig(level=logging.INFO)


class GameCartographer(Cartographer):
    """Cartographer for external game IDs to internal game entities.

    Matches games by:
    1. Exact data_source_id lookup (cached)
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
        data_source: str,
        db_name: str,
        schema: str = "core",
        timezone: Optional[str] = None,
        allow_swapped_teams: bool = True,
        logger: Optional["LoggerManager"] = None,
        debug: bool = False,
    ):
        """Initialize the GameCartographer.

        Args:
            data_source: Data source identifier
            db_name: Database name
            schema: Database schema
            timezone: Source timezone for time conversion (e.g., "UTC", "PST")
            allow_swapped_teams: If True, match games even if home/away are flipped
            logger: Optional LoggerManager instance for structured logging
            debug: Enable debug logging
        """
        self.timezone = timezone
        self.allow_swapped_teams = allow_swapped_teams
        super().__init__(data_source, db_name, schema, logger, debug)

    def map(
        self,
        data_source_id: str,
        away_team: Optional[str] = None,
        home_team: Optional[str] = None,
        away_team_id: Optional[str] = None,
        home_team_id: Optional[str] = None,
        start_time: Optional[datetime | str] = None,
    ) -> Optional[dict]:
        """Map a game by source ID or teams/date.

        Args:
            data_source_id: External source identifier (required)
            away_team: Away team name/abbreviation
            home_team: Home team name/abbreviation
            away_team_id: Internal away team ID (from TeamCartographer)
            home_team_id: Internal home team ID (from TeamCartographer)
            start_time: Game start time (datetime or string)

        Returns:
            Matched game dict or None
        """
        # Normalize data_source_id to string
        data_source_id = str(data_source_id)

        # Step 1: Check cache
        if data_source_id:
            cached = self._lookup_cached(data_source_id)
            if cached:
                self._log(f"Cache hit: data_source_id={data_source_id}")
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
                self._add_mapping(data_source_id, game, confidence_rating=100, log_info=log_info)
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
                    self._add_mapping(data_source_id, game, confidence_rating=95, log_info=log_info)
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
                self._add_mapping(data_source_id, game, confidence_rating=85, log_info=log_info)
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
                    self._add_mapping(data_source_id, game, confidence_rating=80, log_info=log_info)
                    self._log(f"Matched by single team ID + time: {single_team_id}")
                    return game

        # Step 4: Match by team names + date
        if away_team and home_team:
            matches = self._match_by_teams_date(away_team, home_team, game_date)

            if len(matches) == 1:
                game = matches[0]
                log_info = {
                    "method": "teams_date",
                    "away_team": away_team,
                    "home_team": home_team,
                    "date": str(game_date),
                }
                self._add_mapping(data_source_id, game, confidence_rating=100, log_info=log_info)
                self._log(f"Matched: {away_team} @ {home_team} on {game_date}")
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
                    self._add_mapping(data_source_id, game, confidence_rating=90, log_info=log_info)
                    self._log(f"Matched by time: {away_team} @ {home_team}")
                    return game

        # No match found
        self._log(
            f"Cannot map game: data_source={self.data_source}, "
            f"data_source_id={data_source_id}, {away_team or away_team_id}@{home_team or home_team_id} {start_time}",
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
            game_day = game.get("game_date")

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
            game_day = game.get("game_date")

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
            game_day = game.get("game_date")

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
            game_time = game.get("start_time") or game.get("datetime")
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

    def get_or_create(
        self,
        data_source_id: str,
        away_team_id: Optional[str] = None,
        home_team_id: Optional[str] = None,
        data_source_away_team_id: Optional[str] = None,
        data_source_home_team_id: Optional[str] = None,
        away_team: Optional[str] = None,
        home_team: Optional[str] = None,
        data_source_away_team: Optional[str] = None,
        data_source_home_team: Optional[str] = None,
        start_time: Optional[datetime] = None,
        game_date: Optional[date] = None,
        timezone: Optional[str] = None,
        status: Optional[str] = None,
        period: Optional[str] = None,
        clock: Optional[str] = None,
        away_score: Optional[int] = None,
        home_score: Optional[int] = None,
        venue: Optional[str] = None,
        venue_city: Optional[str] = None,
        venue_state: Optional[str] = None,
        is_neutral_site: bool = False,
        season: Optional[int] = None,
        season_type: Optional[str] = None,
        week: Optional[int] = None,
        game_number: Optional[int] = None,
        ppd: bool = False,
        broadcast: Optional[str] = None,
        broadcast_networks: Optional[list[str]] = None,
        weather: Optional[str] = None,
        temperature: Optional[int] = None,
        wind: Optional[str] = None,
        dome: bool = False,
    ) -> dict:
        """Get existing game or create a new one.

        Args:
            data_source_id: External source identifier (required)
            away_team_id: Internal away team ID
            home_team_id: Internal home team ID
            data_source_away_team_id: Away team ID from source
            data_source_home_team_id: Home team ID from source
            away_team: Away team name
            home_team: Home team name
            data_source_away_team: Away team name from source
            data_source_home_team: Home team name from source
            start_time: Game start datetime
            game_date: Game date
            timezone: Source timezone
            status: Game status
            period: Current period
            clock: Game clock
            away_score: Away team score
            home_score: Home team score
            venue: Stadium/arena name
            venue_city: Venue city
            venue_state: Venue state
            is_neutral_site: Whether played at neutral site
            season: Season year
            season_type: Type of season
            week: Week number
            game_number: Game number in series
            ppd: Postponed flag
            broadcast: Primary broadcast network
            broadcast_networks: List of broadcast networks
            weather: Weather conditions
            temperature: Temperature in Fahrenheit
            wind: Wind conditions
            dome: Whether played in dome

        Returns:
            Game dict with ID (existing or newly created)
        """
        data_source_id = str(data_source_id)

        # Try to find existing game
        existing = self.map(
            data_source_id=data_source_id,
            away_team=away_team,
            home_team=home_team,
            away_team_id=away_team_id,
            home_team_id=home_team_id,
            start_time=start_time,
        )

        if existing:
            game_id = existing["id"]
            self._log(f"Found existing game: {data_source_id} -> {game_id}")
        else:
            game_id = uuid.uuid4()
            self._log(f"Creating new game: {data_source_id} -> {game_id}")

        # Parse start_time if it's a string
        start_time_dt = None
        if start_time:
            if isinstance(start_time, str):
                start_time_dt = convert_str_to_datetime(start_time)
            else:
                start_time_dt = start_time

        # Parse game_date if it's a string
        game_date_parsed = None
        if game_date:
            if isinstance(game_date, str):
                game_date_parsed = convert_str_to_date(game_date)
            else:
                game_date_parsed = game_date

        # Extract date from start_time if not provided
        if start_time_dt and not game_date_parsed:
            game_date_parsed = start_time_dt.date()

        # Compute UTC conversions if timezone is provided
        start_time_utc = None
        game_date_utc = None
        effective_tz = timezone or self.timezone
        if start_time_dt and effective_tz:
            try:
                start_time_utc = convert_to_utc(start_time_dt, effective_tz)
                game_date_utc = start_time_utc.date()
            except ValueError as e:
                self._log(f"Failed to convert to UTC: {e}", level="warning")

        # Build game entity with all fields (ordered for logical column grouping)
        game = {
            # Identifiers
            "id": game_id,
            "data_source_id": data_source_id,
            # Team IDs
            "away_team_id": away_team_id,
            "home_team_id": home_team_id,
            "data_source_away_team_id": data_source_away_team_id,
            "data_source_home_team_id": data_source_home_team_id,
            # Team names
            "away_team": away_team.strip() if away_team else None,
            "home_team": home_team.strip() if home_team else None,
            "data_source_away_team": data_source_away_team.strip() if data_source_away_team else None,
            "data_source_home_team": data_source_home_team.strip() if data_source_home_team else None,
            # Timing (date first, then time, then timezone)
            "game_date": game_date_parsed,
            "game_date_utc": game_date_utc,
            "start_time": start_time_dt,
            "start_time_utc": start_time_utc,
            "timezone": effective_tz,
            # Game state
            "status": status,
            "period": period,
            "clock": clock,
            # Scores
            "away_score": away_score,
            "home_score": home_score,
            # Venue
            "venue": venue.strip() if venue else None,
            "venue_city": venue_city.strip() if venue_city else None,
            "venue_state": venue_state.strip() if venue_state else None,
            "is_neutral_site": is_neutral_site,
            # Season info
            "season": season,
            "season_type": season_type,
            "week": week,
            "game_number": game_number,
            # Misc
            "ppd": ppd,
            "broadcast": broadcast,
            "broadcast_networks": broadcast_networks,
            "weather": weather,
            "temperature": temperature,
            "wind": wind,
            "dome": dome,
            # Source (before system timestamps)
            "data_source": self.data_source,
        }

        # Remove None values
        game = {k: v for k, v in game.items() if v is not None}

        # Add to cache and pending entities
        self.cache[data_source_id] = game
        self._pending_entities.append(game)

        # Add mapping to pending (for source_map table)
        if not existing:
            log_info = {"method": "get_or_create", "away_team": away_team, "home_team": home_team}
            self._add_mapping(data_source_id, game, confidence_rating=100, log_info=log_info)

        return game
