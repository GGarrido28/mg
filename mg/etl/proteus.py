"""Proteus - Shape-shifting data transformation utilities.

Named after the Greek sea god who could change his form at will,
Proteus provides flexible data wrangling and transformation tools
for ETL pipelines.
"""

import re
import json
from collections import defaultdict
from datetime import datetime, date
from typing import Any, Callable


class Proteus:
    """Shape-shifting data transformer for cleaning, normalization, and conversion.

    Named after the Greek sea god known for his ability to change form,
    this class provides flexible methods for transforming data between
    different shapes and formats commonly needed in ETL pipelines.

    Capabilities:
    - Dictionary flattening and unnesting
    - Class-to-dict conversion
    - String normalization (snake_case, SQL-friendly)
    - Type conversions (int, float, bool, datetime)
    - Validation (email, phone)
    - Duplicate detection and removal

    Attributes:
        attr_maps: Dictionary mapping class names to their attribute lists
            for class-to-dict conversion.
    """

    def __init__(self, attr_maps: dict[str, list[str]] | None = None) -> None:
        """Initialize Proteus with optional attribute maps.

        Args:
            attr_maps: Dictionary mapping class names to lists of attribute
                names to include when converting class instances to dicts.
        """
        if attr_maps is None:
            attr_maps = {}
        self.attr_maps = attr_maps

    def is_class_instance(self, value: Any) -> bool:
        """Check if a value is a custom class instance (excluding built-in types).

        Args:
            value: Any Python value to check.

        Returns:
            True if value is a class instance but not a built-in type.
        """
        built_in_types = (
            int,
            float,
            str,
            list,
            dict,
            tuple,
            set,
            bool,
            type(None),
            bytes,
            bytearray,
            memoryview,
            complex,
            frozenset,
            range,
            slice,
        )
        return isinstance(value, object) and not isinstance(value, built_in_types)

    def class_to_dict(self, obj: Any, ignore_keys: list[str] | None = None) -> dict[str, Any]:
        """Convert a class instance to a dictionary.

        Recursively converts nested class instances to dictionaries.

        Args:
            obj: Class instance to convert.
            ignore_keys: List of attribute names to exclude from the result.

        Returns:
            Dictionary representation of the class instance.
        """
        if ignore_keys is None:
            ignore_keys = []
        class_dict = obj.__dict__
        new_dict = {}
        for key, value in class_dict.items():
            if key in ignore_keys:
                continue
            if self.is_class_instance(value):
                new_dict[key] = self.class_to_dict(value, ignore_keys=ignore_keys)
            else:
                new_dict[key] = value
        return class_dict

    def unnest_dict(
        self, d: dict[str, Any], ignore_keys: list[str] | None = None
    ) -> dict[str, Any]:
        """Flatten a nested dictionary into a single-level dictionary.

        Recursively unnests nested dictionaries and converts class instances
        to dictionaries. Keys are joined with underscores.

        Args:
            d: Dictionary to flatten.
            ignore_keys: List of keys to exclude from the result.

        Returns:
            Flattened dictionary with all nested values at the top level.
        """
        if ignore_keys is None:
            ignore_keys = []
        stack = [(d, "")]
        flat_dict = {}
        nested_count = defaultdict(int)

        while stack:
            current_dict, parent_key = stack.pop()
            for k, v in current_dict.items():
                if k in ignore_keys:
                    continue
                if isinstance(v, dict):
                    nested_count[k] += 1
                    count = nested_count[k]
                    new_key = (
                        f"{parent_key}_{k}_{count}" if parent_key else f"{k}_{count}"
                    )
                    stack.append((v, new_key))
                else:
                    new_key = f"{parent_key}_{k}" if parent_key else k

                    if self.is_class_instance(v):
                        class_dict = self.class_to_dict(v, ignore_keys=ignore_keys)
                        unnest_dict = self.unnest_dict(
                            class_dict, ignore_keys=ignore_keys
                        )
                        for unk, unv in unnest_dict.items():
                            flat_dict[f"{k}_{unk}"] = unv
                    elif isinstance(v, list):
                        for i, item in enumerate(v):
                            if self.is_class_instance(item):
                                class_dict = self.class_to_dict(
                                    item, ignore_keys=ignore_keys
                                )
                                unnest_dict = self.unnest_dict(
                                    class_dict, ignore_keys=ignore_keys
                                )
                                for unk, unv in unnest_dict.items():
                                    flat_dict[f"{new_key}_{i}_{unk}"] = unv
                            else:
                                flat_dict[f"{new_key}_{i}"] = item
                    elif isinstance(v, dict):
                        flat_dict[new_key] = v
                    else:
                        flat_dict[new_key] = v
        return flat_dict

    def remove_leading_underscore(self, name: str) -> str:
        """Remove leading underscore from a string.

        Args:
            name: Input string.

        Returns:
            String with leading underscore removed, or original if no underscore.
        """
        return name[1:] if name.startswith("_") else name

    def to_snake_case(self, name: str) -> str:
        """Convert a CamelCase or PascalCase name to snake_case.

        Args:
            name: Input string in CamelCase or PascalCase.

        Returns:
            String converted to snake_case.
        """
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        name = re.sub("__([A-Z])", r"_\1", name)
        name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
        return name.lower()

    def sql_friendly_columns(
        self, name: str | dict | list
    ) -> str | dict | list:
        """Make column names SQL-friendly.

        Converts to snake_case and replaces special characters with
        SQL-safe alternatives. Recursively processes dicts and lists.

        Args:
            name: Column name string, or dict/list of names to process.

        Returns:
            SQL-friendly column name(s) in the same structure as input.
        """
        if type(name) == dict:
            return {self.sql_friendly_columns(k): v for k, v in name.items()}
        if type(name) == list:
            return [self.sql_friendly_columns(i) for i in name]

        name = self.to_snake_case(name)
        name = self.remove_leading_underscore(name)
        name = name.replace(" ", "_")
        name = name.replace(".", "")
        name = name.replace("%", "pct")
        name = name.replace("(", "")
        name = name.replace(")", "")
        name = name.replace("+", "_")
        name = name.replace("-", "_")
        name = name.replace("/", "_")
        name = name.replace("\\", "_")
        name = name.replace(",", "")
        name = name.replace(":", "")
        name = name.replace(";", "")
        name = name.replace("__", "_")
        name = name.replace("#", "num")
        return name

    def normalize_whitespace(self, text: str) -> str:
        """Normalize whitespace by trimming and collapsing multiple spaces.

        Args:
            text: Input string with potentially irregular whitespace.

        Returns:
            String with single spaces and no leading/trailing whitespace.
        """
        return re.sub(r"\s+", " ", text).strip()

    def convert_to_int(self, value: Any) -> int | None:
        """Safely convert a value to an integer.

        Args:
            value: Value to convert (string, float, etc.).

        Returns:
            Integer value, or None if conversion fails.
        """
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def convert_to_float(self, value: Any) -> float | None:
        """Safely convert a value to a float.

        Args:
            value: Value to convert (string, int, etc.).

        Returns:
            Float value, or None if conversion fails.
        """
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def is_valid_email(self, email: str) -> bool:
        """Validate an email address format.

        Args:
            email: Email address string to validate.

        Returns:
            True if email format is valid, False otherwise.
        """
        email_regex = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
        return re.match(email_regex, email) is not None

    def is_valid_phone_number(self, phone_number: str) -> bool:
        """Validate a phone number format.

        Accepts optional + prefix and country code, requires 9-15 digits.

        Args:
            phone_number: Phone number string to validate.

        Returns:
            True if phone number format is valid, False otherwise.
        """
        phone_regex = re.compile(r"^\+?1?\d{9,15}$")
        return re.match(phone_regex, phone_number) is not None

    def sum_values(self, values: list[int | float]) -> int | float:
        """Calculate the sum of a list of numeric values.

        Args:
            values: List of numbers to sum.

        Returns:
            Sum of all values.
        """
        return sum(values)

    def average_values(self, values: list[int | float]) -> float | None:
        """Calculate the average of a list of numeric values.

        Args:
            values: List of numbers to average.

        Returns:
            Average value, or None if list is empty.
        """
        return sum(values) / len(values) if values else None

    def filter_dict(
        self, d: dict[str, Any], condition: Callable[[str, Any], bool]
    ) -> dict[str, Any]:
        """Filter a dictionary based on a condition function.

        Args:
            d: Dictionary to filter.
            condition: Function that takes (key, value) and returns bool.

        Returns:
            New dictionary containing only items where condition returns True.
        """
        return {k: v for k, v in d.items() if condition(k, v)}

    def convert_string_to_true_value(self, value: Any) -> Any:
        """Convert a string to its native Python type.

        Attempts to convert strings to int, float, bool, datetime, or None
        based on their content.

        Args:
            value: Value to convert (typically a string).

        Returns:
            Converted value in its native type, or original value if no
            conversion applies.
        """
        if isinstance(value, str):
            if value.isdigit():
                return int(value)
            try:
                return float(value)
            except ValueError:
                pass
            if value.lower() in ["true", "false"]:
                return value.lower() == "true"
            if value.lower() in ["na", "n/a"]:
                return None
            try:
                # Try parsing as datetime
                return datetime.fromisoformat(value)
            except ValueError:
                pass
            if value.lower() == "none":
                return None
        return value

    @staticmethod
    def convert_float(value: float | Any) -> int | float | Any:
        """Convert a float to int if it represents a whole number.

        Args:
            value: Value to potentially convert.

        Returns:
            Integer if float is whole number, otherwise original value.
        """
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

    def remove_duplicates(self, dict_list: list[dict]) -> list[dict]:
        """Remove duplicate dictionaries from a list.

        Compares dictionaries by their key-value pairs. Two dictionaries
        are considered duplicates if all their key-value pairs are identical.

        Args:
            dict_list: List of dictionaries to deduplicate.

        Returns:
            New list with duplicate dictionaries removed, preserving order.
        """
        seen = set()
        unique_dicts = []

        for d in dict_list:
            # Create a frozenset of the dictionary's items to use as a hashable key for uniqueness
            dict_items = frozenset(d.items())
            if dict_items not in seen:
                seen.add(dict_items)
                unique_dicts.append(d)

        return unique_dicts

    def check_duplicate_rows(
        self, rows: list[dict], columns: list[str] | None = None
    ) -> tuple[bool, dict[str, int]]:
        """Check for duplicate rows based on specified columns.

        Args:
            rows: List of row dictionaries to check.
            columns: List of column names to use for comparison. If None,
                compares all columns.

        Returns:
            Tuple of (has_duplicates, duplicate_info) where duplicate_info
            is a dict mapping row keys to their occurrence counts (only for
            rows appearing more than once).
        """
        if columns is None:
            columns = []
        duplicates = False
        duplicate_rows = {}

        for row in rows:
            # Create a filtered version of the row based on the specified columns
            filtered_row = {key: row[key] for key in columns if key in row}

            for key, value in filtered_row.items():
                # Handle complex data types by converting them to a string representation
                if isinstance(value, (dict, list)):
                    filtered_row[key] = json.dumps(value)
                elif isinstance(value, (datetime, date)):
                    filtered_row[key] = value.isoformat()

            # Generate a unique key for the row based on its contents
            row_key = json.dumps(filtered_row, sort_keys=True)

            # Track duplicates by incrementing the count if the row_key already exists
            if row_key in duplicate_rows:
                duplicates = True
                duplicate_rows[row_key] += 1
            else:
                duplicate_rows[row_key] = 1

        # Return only rows that have duplicates (count > 1)
        flagged_duplicates = {
            key: count for key, count in duplicate_rows.items() if count > 1
        }

        return duplicates, flagged_duplicates

    def flatten_dict(self, d: dict[str, Any]) -> list[dict[str, Any]]:
        """Flatten a nested dictionary into a list of flat dictionaries.

        Handles nested dicts and lists by flattening keys with underscores.
        Array elements with the same index are grouped together.

        Args:
            d: Nested dictionary to flatten.

        Returns:
            List of flattened dictionaries with no nesting.
        """
        result: list[dict[str, Any]] = []
        index_map: dict[int, dict[str, Any]] = {}

        def _flatten(obj: Any, prefix: str = "") -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{prefix}_{key}" if prefix else key
                    _flatten(value, new_key)
            elif isinstance(obj, list):
                for i, item in enumerate(
                    item for item in obj if isinstance(item, dict)
                ):
                    flattened_item = {}
                    for key, value in item.items():
                        full_key = f"{prefix}_{key}" if prefix else key
                        if isinstance(value, (dict, list)):
                            _flatten(value, full_key)
                        else:
                            flattened_item[full_key] = value
                    if i in index_map:
                        index_map[i].update(flattened_item)
                    else:
                        index_map[i] = flattened_item
            else:
                # Handle non-dict, non-list values at the root level
                if not result or prefix in result[-1]:
                    result.append({})
                result[-1][prefix] = obj

        _flatten(d)

        # Add the indexed items to the result
        if index_map:
            result.extend(index_map[i] for i in sorted(index_map.keys()))

        return result


if __name__ == "__main__":
    # Define attribute maps for class Person
    attr_maps = {"Person": ["name", "age", "address", "phone_number", "email"]}

    # Initialize Proteus object
    p = Proteus(attr_maps)

    # Define a class instance
    class Person:
        def __init__(self, name, age, address, phone_number, email):
            self.name = name
            self.age = age
            self.address = address
            self.phone_number = phone_number
            self.email = email

    # Create a class instance
    person = Person("John Doe", 30, "123 Main St", "555-555-5555", "")

    # Convert class instance to dictionary
    person_dict = p.class_to_dict(person)
    print(person_dict)

    test = {
        "person": person,
        "test": {
            "nested": {
                "test_one": 1,
                "test_two": 2,
            },
            "nested": {
                "test_three": 3,
                "test_four": 4,
            },
        },
    }
    result = p.unnest_dict(test)
    for key, value in result.items():
        print(f"{key}: {value}")

    # Example usages of the new helper functions
    print(p.normalize_whitespace("  Hello   World  "))  # "Hello World"
    print(p.convert_to_int("123"))  # 123
    print(p.convert_to_float("123.45"))  # 123.45
    print(p.is_valid_email("test@example.com"))  # True
    print(p.is_valid_phone_number("+1234567890"))  # True
    print(p.sum_values([1, 2, 3, 4, 5]))  # 15
    print(p.average_values([1, 2, 3, 4, 5]))  # 3.0
    filtered_dict = p.filter_dict(
        person_dict, lambda k, v: isinstance(v, int) and v > 20
    )
    print(filtered_dict)  # {'age': 30}

    # Converting string to true value
    print(p.convert_string_to_true_value("1"))  # 1
    print(p.convert_string_to_true_value("123.45"))  # 123.45
    print(p.convert_string_to_true_value("true"))  # True
    print(p.convert_string_to_true_value("false"))  # False
    print(p.convert_string_to_true_value("none"))  # None
    print(p.convert_string_to_true_value("NA"))  # None
    print(p.convert_string_to_true_value("N/A"))  # None
    print(p.convert_string_to_true_value("2023-07-13 14:45:00"))  # 2023-07-13 14:45:00
