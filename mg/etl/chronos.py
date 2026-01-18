import logging
import pytz
import datetime

TZ_MAP = {
    "UTC": pytz.utc,
    "PST": pytz.timezone("US/Pacific"),
    "PDT": pytz.timezone("US/Pacific"),
    "MST": pytz.timezone("US/Mountain"),
    "MDT": pytz.timezone("US/Mountain"),
    "EST": pytz.timezone("America/New_York"),
    "EDT": pytz.timezone("America/New_York"),
}


def today_pst() -> datetime.date:
    """Returns today's date in Pacific timezone."""
    return datetime.datetime.now(pytz.timezone("US/Pacific")).date()


def today_est() -> datetime.date:
    """Returns today's date in Eastern timezone."""
    return datetime.datetime.now(pytz.timezone("US/Eastern")).date()


def today_utc() -> datetime.date:
    """Returns today's date in UTC."""
    return datetime.datetime.now(pytz.UTC).date()


def now_est() -> datetime.datetime:
    """Returns current datetime in Eastern timezone."""
    return datetime.datetime.now(pytz.timezone("US/Eastern"))


def now_utc() -> datetime.datetime:
    """Returns current datetime in UTC."""
    return datetime.datetime.now(pytz.UTC)


def now_pst() -> datetime.datetime:
    """Returns current datetime in Pacific timezone."""
    return datetime.datetime.now(pytz.timezone("US/Pacific"))


def convert_to_est(dt: datetime.datetime, orig_timezone: str) -> datetime.datetime:
    """Converts a naive datetime to Eastern timezone.

    Args:
        dt: Naive datetime object to convert
        orig_timezone: Original timezone (UTC, PST, PDT, MST, MDT, EST, EDT)

    Returns:
        Timezone-aware datetime in Eastern timezone

    Raises:
        ValueError: If orig_timezone is not recognized
    """
    if orig_timezone not in TZ_MAP:
        raise ValueError(f"Unknown timezone: {orig_timezone}")

    eastern = TZ_MAP["EST"]
    new_dt = TZ_MAP[orig_timezone].localize(dt)

    if orig_timezone in ("EST", "EDT"):
        return new_dt

    return new_dt.astimezone(eastern)


def convert_to_utc(dt: datetime.datetime, orig_timezone: str) -> datetime.datetime:
    """Converts a naive datetime to UTC.

    Args:
        dt: Naive datetime object to convert
        orig_timezone: Original timezone (UTC, PST, PDT, MST, MDT, EST, EDT)

    Returns:
        Timezone-aware datetime in UTC

    Raises:
        ValueError: If orig_timezone is not recognized
    """
    if orig_timezone not in TZ_MAP:
        raise ValueError(f"Unknown timezone: {orig_timezone}")

    new_dt = TZ_MAP[orig_timezone].localize(dt)

    if orig_timezone == "UTC":
        return new_dt

    return new_dt.astimezone(pytz.utc)


def convert_date_to_str(date: datetime.date | None) -> str | None:
    """Converts a date object to YYYY-MM-DD string format.

    Args:
        date: Date object to convert, or None

    Returns:
        Date string in YYYY-MM-DD format, empty string if None, or None on error
    """
    if not date:
        return ""
    try:
        return date.strftime("%Y-%m-%d")
    except Exception as e:
        logging.error(e)


def convert_datetime_to_str(date: datetime.datetime | None) -> str | None:
    """Converts a datetime object to YYYY-MM-DD HH:MM:SS string format.

    Args:
        date: Datetime object to convert, or None

    Returns:
        Datetime string in YYYY-MM-DD HH:MM:SS format, empty string if None, or None on error
    """
    if not date:
        return ""
    try:
        return date.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logging.error(e)


def convert_str_to_date(date_str: str) -> datetime.date | None:
    """Converts a date string to a date object.

    Args:
        date_str: Date string in YYYY-MM-DD format, or "yesterday"

    Returns:
        Date object, or None on parse error
    """
    if date_str == "yesterday":
        return today_pst() - datetime.timedelta(days=1)
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception as e:
        msg = "Exception {} in convert_str_to_date for {}".format(e, date_str)
        logging.exception(msg)


def convert_str_to_datetime(date_str: str) -> datetime.datetime | None:
    """Converts a datetime string to a datetime object.

    Handles ISO format with 'T' separator and decimal seconds.

    Args:
        date_str: Datetime string in YYYY-MM-DD HH:MM:SS format, or "yesterday"

    Returns:
        Datetime object, or None on parse error
    """
    if date_str == "yesterday":
        return today_pst() - datetime.timedelta(days=1)
    try:
        # to be able to handle datetime that include decimal seconds and
        # or have a T in the string remove these first
        date_str_chk = date_str[:19].replace("T", " ")
        return datetime.datetime.strptime(date_str_chk, "%Y-%m-%d %H:%M:%S")
    except Exception as e:
        msg = "Exception {} in convert_str_to_datetime for {}".format(e, date_str)
        logging.exception(msg)


def get_sport_season(sport: str) -> int:
    """Returns the current season year for a given sport.

    For fall/winter sports (NFL, NBA, NHL, CFB, CBB), returns the previous
    year if we're before September (e.g., in January 2026, NFL season is 2025).

    Args:
        sport: Sport code (nfl, nba, nhl, cfb, cbb, mlb, etc.)

    Returns:
        Season year as integer
    """
    year = datetime.date.today().year
    today = datetime.date.today()
    if today.month < 9:
        if sport.lower() in ["nfl", "nba", "nhl", "cfb", "cbb"]:
            year -= 1
    return year


def date_range(start: datetime.date, end: datetime.date) -> list[datetime.date]:
    """Returns a list of dates from start to end (inclusive)."""
    days = (end - start).days + 1
    return [start + datetime.timedelta(days=i) for i in range(days)]


def date_range_str(start: str, end: str) -> list[str]:
    """Returns a list of date strings from start to end (inclusive).

    Args:
        start: Start date in YYYY-MM-DD format
        end: End date in YYYY-MM-DD format

    Returns:
        List of date strings in YYYY-MM-DD format
    """
    start_date = convert_str_to_date(start)
    end_date = convert_str_to_date(end)
    if not start_date or not end_date:
        return []
    dates = date_range(start_date, end_date)
    return [convert_date_to_str(d) for d in dates]


def hours_until(
    event_time: datetime.datetime,
    timezone: str = "EST",
    from_time: datetime.datetime | None = None,
) -> float:
    """Calculates hours until an event from a given time (or now).

    Args:
        event_time: Naive datetime of the event
        timezone: Timezone of event_time and from_time (UTC, PST, PDT, MST, MDT, EST, EDT)
        from_time: Naive datetime to calculate from (defaults to current time)

    Returns:
        Hours until the event as a float (negative if event is in the past)

    Example:
        >>> hours_until(datetime.datetime(2026, 1, 18, 16, 0, 0), "EST")
        4.78  # if current time is 11:13 EST

        >>> hours_until(
        ...     datetime.datetime(2026, 1, 18, 16, 0, 0),
        ...     "EST",
        ...     from_time=datetime.datetime(2026, 1, 18, 14, 0, 0)
        ... )
        2.0
    """
    if timezone not in TZ_MAP:
        raise ValueError(f"Unknown timezone: {timezone}")

    tz = TZ_MAP[timezone]
    event_localized = tz.localize(event_time)

    if from_time is None:
        from_localized = datetime.datetime.now(tz)
    else:
        from_localized = tz.localize(from_time)

    delta = event_localized - from_localized
    return delta.total_seconds() / 3600


def datetime_difference(
    dt1: datetime.datetime | None, dt2: datetime.datetime | None
) -> float | None:
    """Calculate the absolute difference between two datetimes in seconds.

    Args:
        dt1: First datetime object.
        dt2: Second datetime object.

    Returns:
        Absolute difference in seconds, or None if either input is None.
    """
    if dt1 and dt2:
        return abs((dt1 - dt2).total_seconds())
    return None


def add_time_to_datetime(
    dt: datetime.datetime | None,
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0,
) -> datetime.datetime | None:
    """Add time to a datetime object.

    Args:
        dt: Datetime object to modify.
        days: Number of days to add.
        hours: Number of hours to add.
        minutes: Number of minutes to add.
        seconds: Number of seconds to add.

    Returns:
        New datetime with time added, or None if dt is None.
    """
    if dt:
        return dt + datetime.timedelta(
            days=days, hours=hours, minutes=minutes, seconds=seconds
        )
    return None