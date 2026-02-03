#!/usr/bin/env python3
"""Manual entity mapping tool for linking external source IDs to internal entities.

This script allows users to manually create mappings between external data source
identifiers and internal entity UUIDs by searching entities by name and selecting
from the results.

Usage:
    # Fully interactive mode (prompts for all inputs)
    python -m mg.etl.entity_mapper

    # Partial CLI args (prompts for missing values)
    python -m mg.etl.entity_mapper --sport counterstrike --entity player

    # Full CLI mode (no prompts)
    python -m mg.etl.entity_mapper --sport counterstrike --data_source prizepicks \
        --data_source_id 12345 --entity player --name "donk"
"""

import argparse
import json
import logging
import sys
from typing import Optional

from mg.db.postgres_manager import PostgresManager

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def validate_schema(schema: str) -> str:
    """Validate schema name to prevent SQL injection.

    Args:
        schema: Schema name to validate

    Returns:
        Validated schema name

    Raises:
        ValueError: If schema is invalid
    """
    # Use PostgresManager's validator for SQL identifier safety
    return PostgresManager.validate_identifier(schema, "schema")


# Entity configuration mapping
ENTITY_CONFIG = {
    "team": {
        "table": "teams",
        "source_map_table": "team_source_map",
        "id_column": "id",
        "name_column": "team_name",
        "display_columns": ["id", "team_name", "abbreviation", "data_source"],
    },
    "player": {
        "table": "players",
        "source_map_table": "player_source_map",
        "id_column": "id",
        "name_column": "player_name",
        "display_columns": ["id", "player_name", "team_name", "position", "data_source"],
    },
    "game": {
        "table": "games",
        "source_map_table": "game_source_map",
        "id_column": "id",
        "name_column": "away_team",  # Search by team names
        "display_columns": ["id", "away_team", "home_team", "game_date", "data_source"],
    },
}

# Common data sources for quick selection
COMMON_DATA_SOURCES = ["draftkings", "prizepicks", "underdog", "hltv", "manual"]

# Common sports/schemas
COMMON_SPORTS = ["counterstrike", "lol", "valorant"]


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Manually map external source IDs to internal entities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Fully interactive mode
    python -m mg.etl.entity_mapper

    # Partial CLI args (will prompt for missing values)
    python -m mg.etl.entity_mapper --sport counterstrike --entity player

    # Full CLI mode
    python -m mg.etl.entity_mapper --sport counterstrike --data_source prizepicks \\
        --data_source_id PP_12345 --entity player --name "donk"
        """,
    )
    parser.add_argument(
        "--sport",
        help="Database schema/sport (e.g., counterstrike, lol, valorant)",
    )
    parser.add_argument(
        "--data_source",
        help="Data source identifier (e.g., draftkings, prizepicks, hltv)",
    )
    parser.add_argument(
        "--data_source_id",
        help="External source identifier to map",
    )
    parser.add_argument(
        "--entity",
        choices=["team", "player", "game"],
        help="Entity type to map",
    )
    parser.add_argument(
        "--name",
        help="Name to search for (uses wildcard matching)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def prompt_with_options(prompt: str, options: list[str], allow_custom: bool = True) -> str:
    """Prompt user to select from options or enter custom value.

    Args:
        prompt: The prompt message
        options: List of predefined options
        allow_custom: Whether to allow custom input

    Returns:
        Selected or entered value
    """
    print(f"\n{prompt}")
    for i, opt in enumerate(options, 1):
        print(f"  [{i}] {opt}")
    if allow_custom:
        print(f"  [c] Enter custom value")

    while True:
        user_input = input("\nSelection: ").strip().lower()

        if user_input == "c" and allow_custom:
            return input("Enter value: ").strip()

        try:
            idx = int(user_input)
            if 1 <= idx <= len(options):
                return options[idx - 1]
            print(f"Please enter a number between 1 and {len(options)}")
        except ValueError:
            # Allow direct text input as custom value
            if user_input and allow_custom:
                return user_input
            print("Invalid input. Enter a number or 'c' for custom.")


def prompt_for_value(prompt: str, current_value: Optional[str] = None) -> str:
    """Prompt user for a value, showing current value if set.

    Args:
        prompt: The prompt message
        current_value: Current value (if any)

    Returns:
        User-entered value
    """
    if current_value:
        print(f"\n{prompt}")
        print(f"  Current: {current_value}")
        user_input = input("  New value (Enter to keep current): ").strip()
        return user_input if user_input else current_value
    else:
        return input(f"\n{prompt}: ").strip()


def get_interactive_inputs(args: argparse.Namespace) -> dict:
    """Get all required inputs, prompting for any missing values.

    Args:
        args: Parsed command line arguments

    Returns:
        Dictionary with all required values
    """
    inputs = {}

    print("\n" + "=" * 60)
    print("  ENTITY MAPPER - Interactive Mode")
    print("=" * 60)
    print("\nPaste or type values when prompted. Press Enter to confirm.")

    # Sport
    if args.sport:
        inputs["sport"] = args.sport
        print(f"\n> Sport: {args.sport}")
    else:
        inputs["sport"] = prompt_with_options(
            "Select sport/schema:",
            COMMON_SPORTS,
            allow_custom=True,
        )

    # Entity type
    if args.entity:
        inputs["entity"] = args.entity
        print(f"\n> Entity type: {args.entity}")
    else:
        inputs["entity"] = prompt_with_options(
            "Select entity type:",
            ["team", "player", "game"],
            allow_custom=False,
        )

    # Data source
    if args.data_source:
        inputs["data_source"] = args.data_source
        print(f"\n> Data source: {args.data_source}")
    else:
        inputs["data_source"] = prompt_with_options(
            "Select data source:",
            COMMON_DATA_SOURCES,
            allow_custom=True,
        )

    # Data source ID (always prompt if not provided - likely to be pasted)
    if args.data_source_id:
        inputs["data_source_id"] = args.data_source_id
        print(f"\n> Data source ID: {args.data_source_id}")
    else:
        inputs["data_source_id"] = prompt_for_value(
            "Enter/paste data_source_id (external ID to map)"
        )

    # Name to search (always prompt if not provided - likely to be pasted)
    if args.name:
        inputs["name"] = args.name
        print(f"\n> Search name: {args.name}")
    else:
        inputs["name"] = prompt_for_value(
            "Enter/paste name to search for (partial match supported)"
        )

    inputs["debug"] = args.debug

    return inputs


def search_entities(
    pgm: PostgresManager,
    schema: str,
    entity_type: str,
    search_name: str,
) -> list[dict]:
    """Search for entities matching the given name using wildcard matching.

    Args:
        pgm: PostgresManager instance
        schema: Database schema
        entity_type: Entity type (team, player, game)
        search_name: Name to search for

    Returns:
        List of matching entity dictionaries
    """
    config = ENTITY_CONFIG[entity_type]
    table = config["table"]
    name_column = config["name_column"]

    # For games, search both away_team and home_team
    if entity_type == "game":
        query = f"""
            SELECT * FROM {schema}.{table}
            WHERE away_team ILIKE %(pattern)s
               OR home_team ILIKE %(pattern)s
            ORDER BY game_date DESC NULLS LAST
            LIMIT 50
        """
    else:
        query = f"""
            SELECT * FROM {schema}.{table}
            WHERE {name_column} ILIKE %(pattern)s
            ORDER BY {name_column}
            LIMIT 50
        """

    pattern = f"%{search_name}%"
    return pgm.execute(query, params={"pattern": pattern})


def check_existing_mapping(
    pgm: PostgresManager,
    schema: str,
    entity_type: str,
    data_source: str,
    data_source_id: str,
) -> Optional[dict]:
    """Check if a mapping already exists for this data_source + data_source_id.

    Args:
        pgm: PostgresManager instance
        schema: Database schema
        entity_type: Entity type
        data_source: Data source identifier
        data_source_id: External source identifier

    Returns:
        Existing mapping dict if found, None otherwise
    """
    config = ENTITY_CONFIG[entity_type]
    source_map_table = config["source_map_table"]

    query = f"""
        SELECT * FROM {schema}.{source_map_table}
        WHERE data_source = %(data_source)s
          AND data_source_id = %(data_source_id)s
    """
    results = pgm.execute(
        query,
        params={"data_source": data_source, "data_source_id": data_source_id},
    )
    return results[0] if results else None


def display_results(entities: list[dict], entity_type: str) -> None:
    """Display search results in a numbered list.

    Args:
        entities: List of entity dictionaries
        entity_type: Entity type for display formatting
    """
    config = ENTITY_CONFIG[entity_type]
    display_columns = config["display_columns"]

    print(f"\n{'='*80}")
    print(f"Found {len(entities)} matching {entity_type}(s):")
    print(f"{'='*80}\n")

    for i, entity in enumerate(entities, 1):
        print(f"  [{i}]")
        for col in display_columns:
            value = entity.get(col, "N/A")
            # Truncate long UUIDs for display
            if col == "id" and value:
                value = str(value)[:36]
            elif col == "team_id" and value:
                value = str(value)[:36]
            print(f"      {col}: {value}")
        print()


def get_user_selection(max_option: int) -> Optional[int]:
    """Get user's selection from the numbered list.

    Args:
        max_option: Maximum valid option number

    Returns:
        Selected option number (1-indexed), or None if cancelled
    """
    print(f"{'='*80}")
    print("Enter the number of the entity to map, or 'q' to quit:")
    print(f"{'='*80}")

    while True:
        try:
            user_input = input("\nSelection: ").strip().lower()

            if user_input in ("q", "quit", "exit"):
                return None

            selection = int(user_input)
            if 1 <= selection <= max_option:
                return selection
            else:
                print(f"Please enter a number between 1 and {max_option}")
        except ValueError:
            print("Invalid input. Please enter a number or 'q' to quit.")


def create_mapping(
    pgm: PostgresManager,
    schema: str,
    entity_type: str,
    data_source: str,
    data_source_id: str,
    entity: dict,
) -> bool:
    """Create a new mapping in the source map table.

    Args:
        pgm: PostgresManager instance
        schema: Database schema
        entity_type: Entity type
        data_source: Data source identifier
        data_source_id: External source identifier
        entity: Selected entity dict

    Returns:
        True if successful, False otherwise
    """
    config = ENTITY_CONFIG[entity_type]
    source_map_table = config["source_map_table"]
    id_column = config["id_column"]

    entity_id = entity[id_column]
    log_info = {
        "method": "manual_entity_mapper",
        "search_term": entity.get(config["name_column"]),
    }

    mapping = {
        "data_source": data_source,
        "data_source_id": str(data_source_id).lower(),
        "entity_id": entity_id,
        "confidence_rating": 100,
        "log_info": json.dumps(log_info),
    }

    # Check if source_map table exists
    if not pgm.check_table_exists(source_map_table):
        logging.info(f"Creating source map table: {source_map_table}")
        pgm.create_table(
            dict_list=[mapping],
            primary_keys=["data_source", "data_source_id"],
            table_name=source_map_table,
            delete=False,
        )

    columns = ["data_source", "data_source_id", "entity_id", "confidence_rating", "log_info"]
    success = pgm.insert_rows(
        source_map_table,
        columns,
        [mapping],
        contains_dicts=True,
        update=True,
    )

    return success


def run_mapping_session(pgm: PostgresManager, inputs: dict) -> bool:
    """Run a single mapping session.

    Args:
        pgm: PostgresManager instance
        inputs: Dictionary with sport, entity, data_source, data_source_id, name

    Returns:
        True if mapping was created, False otherwise
    """
    sport = inputs["sport"]
    entity_type = inputs["entity"]
    data_source = inputs["data_source"]
    data_source_id = str(inputs["data_source_id"]).lower()
    search_name = inputs["name"]

    # Validate schema to prevent SQL injection
    try:
        validate_schema(sport)
    except ValueError as e:
        logging.error(f"Invalid schema name: {e}")
        return False

    # Check for existing mapping
    try:
        existing = check_existing_mapping(pgm, sport, entity_type, data_source, data_source_id)
    except Exception as e:
        logging.error(f"Failed to check existing mappings: {e}")
        return False

    if existing:
        print(f"\n[!] WARNING: A mapping already exists:")
        print(f"   Entity ID: {existing.get('entity_id')}")
        print(f"   Confidence: {existing.get('confidence_rating')}")
        print(f"   Log Info: {existing.get('log_info')}")

        confirm = input("\nUpdate this mapping? (y/n): ").strip().lower()
        if confirm not in ("y", "yes"):
            print("Skipped.")
            return False

    # Search for entities
    try:
        entities = search_entities(pgm, sport, entity_type, search_name)
    except Exception as e:
        logging.error(f"Failed to search entities: {e}")
        return False

    if not entities:
        print(f"\n[X] No {entity_type}s found matching '{search_name}'")
        print("   Try a different search term.")
        return False

    # Display results
    display_results(entities, entity_type)

    # Get user selection
    selection = get_user_selection(len(entities))
    if selection is None:
        print("Cancelled.")
        return False

    selected_entity = entities[selection - 1]
    config = ENTITY_CONFIG[entity_type]

    print(f"\n> Selected: {selected_entity.get(config['name_column'])}")
    print(f"  ID: {selected_entity.get(config['id_column'])}")

    # Confirm before creating mapping
    confirm = input("\nCreate this mapping? (y/n): ").strip().lower()
    if confirm not in ("y", "yes"):
        print("Cancelled.")
        return False

    # Create the mapping
    try:
        success = create_mapping(
            pgm,
            sport,
            entity_type,
            data_source,
            data_source_id,
            selected_entity,
        )
    except Exception as e:
        logging.error(f"Failed to create mapping: {e}")
        return False

    if success:
        print(f"\n{'='*60}")
        print("[OK] SUCCESS: Mapping created!")
        print(f"{'='*60}")
        print(f"  {data_source}:{data_source_id}")
        print(f"  -> {selected_entity.get(config['id_column'])}")
        return True
    else:
        logging.error("Failed to create mapping")
        return False


def main() -> int:
    """Main entry point."""
    args = parse_args()

    # Get all inputs (interactive prompts for missing values)
    inputs = get_interactive_inputs(args)

    # Validate required inputs
    for key in ["sport", "entity", "data_source", "data_source_id", "name"]:
        if not inputs.get(key):
            logging.error(f"Missing required input: {key}")
            return 1

    # Show summary
    print(f"\n{'='*60}")
    print("  MAPPING CONFIGURATION")
    print(f"{'='*60}")
    print(f"  Sport:          {inputs['sport']}")
    print(f"  Entity Type:    {inputs['entity']}")
    print(f"  Data Source:    {inputs['data_source']}")
    print(f"  Data Source ID: {inputs['data_source_id']}")
    print(f"  Search Name:    {inputs['name']}")
    print(f"{'='*60}")

    # Connect to database
    try:
        pgm = PostgresManager(
            host="digital_ocean",
            database="postgres",
            schema=inputs["sport"],
            return_logging=inputs.get("debug", False),
        )
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        return 1

    try:
        # Run the mapping session
        run_mapping_session(pgm, inputs)

        # Ask if user wants to do another mapping
        while True:
            print(f"\n{'='*60}")
            another = input("Map another entity? (y/n): ").strip().lower()
            if another not in ("y", "yes"):
                break

            # Prompt for new values (keep sport, entity, data_source by default)
            print("\n(Press Enter to keep previous values)")

            new_data_source_id = input(f"Data Source ID [{inputs['data_source_id']}]: ").strip()
            if new_data_source_id:
                inputs["data_source_id"] = new_data_source_id

            new_name = input(f"Search Name [{inputs['name']}]: ").strip()
            if new_name:
                inputs["name"] = new_name

            # Option to change other fields
            change_others = input("Change sport/entity/source? (y/n): ").strip().lower()
            if change_others in ("y", "yes"):
                new_sport = input(f"Sport [{inputs['sport']}]: ").strip()
                if new_sport:
                    inputs["sport"] = new_sport
                    # Reconnect to new schema
                    pgm.close()
                    pgm = PostgresManager(
                        host="digital_ocean",
                        database="postgres",
                        schema=inputs["sport"],
                        return_logging=inputs.get("debug", False),
                    )

                new_entity = input(f"Entity [{inputs['entity']}]: ").strip()
                if new_entity in ["team", "player", "game"]:
                    inputs["entity"] = new_entity

                new_source = input(f"Data Source [{inputs['data_source']}]: ").strip()
                if new_source:
                    inputs["data_source"] = new_source

            run_mapping_session(pgm, inputs)

        print("\nDone. Goodbye!")
        return 0

    finally:
        pgm.close()


if __name__ == "__main__":
    sys.exit(main())
