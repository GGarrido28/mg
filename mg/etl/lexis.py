"""Name processing utilities for player matching and normalization.

This module provides functions for cleaning, normalizing, and comparing
player/team/city names in sports data processing pipelines. It handles common
name variations including suffixes, Unicode characters, and hyphenated names.
"""

import unidecode
from typing import Any, List

from Levenshtein import distance as levenshtein_distance
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Common name suffixes to strip during normalization
NAME_SUFFIXES = [" Jr.", " Sr.", " Jr", " Sr", " III", " II", " IV", " V", " I"]

# Prefixes that should have the following letter capitalized (e.g., McDonald, McIlroy)
NAME_PREFIXES_CAPITALIZE_NEXT = ["Mc", "Mac", "O'"]

# Prefixes where only the prefix itself is capitalized, not the next letter (e.g., de la, van)
NAME_PREFIXES_LOWERCASE = ["de la ", "van ", "von "]

# Special case prefixes where both prefix and next letter are capitalized (e.g., DeChambeau, NeSmith)
NAME_PREFIXES_BOTH_CAPS = ["De", "Ne", "Le"]


def ensure_string(value: Any) -> str:
    """Ensure the input value is a string, converting if necessary.

    Args:
        value: Input value of any type.
    Returns:
        String representation of the input value.
    """
    if not isinstance(value, str):
        value = str(value)
    return value


def strip_suffix(string: str) -> str:
    """Remove common name suffixes like Jr., Sr., III, etc.

    Args:
        string: String potentially containing suffixes.

    Returns:
        String with suffixes removed and trimmed.
    """
    string = ensure_string(string)
    for s in NAME_SUFFIXES:
        if s in string:
            string = string.replace(s, "").strip()
    return string


def normalize_accents(string: str) -> str:
    """Remove accents/diacritics from a string while preserving capitalization.

    Converts accented characters to their ASCII equivalents:
    - José -> Jose
    - García -> Garcia
    - Müller -> Muller
    - São Paulo -> Sao Paulo

    Args:
        string: String with potential accented characters.

    Returns:
        String with accents removed.
    """
    string = ensure_string(string)
    if not string:
        return string
    return unidecode.unidecode(string)


def remove_periods(string: str) -> str:
    """Remove all periods from a string.

    Args:
        string: Input string.

    Returns:
        String with periods removed.
    """
    string = ensure_string(string)
    return string.replace(".", "")


def fix_capitalization(string: str, remove_accents: bool = False) -> str:
    """Fix capitalization for strings with special prefixes.

    Handles common patterns that are often incorrectly capitalized:
    - "Mc" prefixes: Mccarthy -> McCarthy, Mcilroy -> McIlroy
    - "De/Ne/Le" prefixes: Dechambeau -> DeChambeau, Nesmith -> NeSmith
    - "De la" phrases: De La Fuente -> De la Fuente
    - Two-letter abbreviations: Rj -> RJ, Oj -> OJ

    Works for player names, city names, and other text.

    Args:
        string: String with potentially incorrect capitalization.
        remove_accents: If True, also removes accents/diacritics (e.g., José -> Jose).

    Returns:
        String with corrected capitalization.
    """
    string = ensure_string(string)
    if not string:
        return string

    if remove_accents:
        string = normalize_accents(string)

    words = string.split()
    fixed_words = []

    for i, word in enumerate(words):
        # Check for two-letter words that should be all caps (initials like RJ, OJ)
        if len(word) == 2 and word[0].isupper() and word[1].islower():
            fixed_words.append(word.upper())
            continue

        # Check for "Mc" prefix (e.g., Mccarthy -> McCarthy)
        if word.startswith("Mc") and len(word) > 2 and word[2].islower():
            fixed_words.append("Mc" + word[2].upper() + word[3:])
            continue

        # Check for "Mac" prefix (e.g., Macdonald -> MacDonald)
        if word.startswith("Mac") and len(word) > 3 and word[3].islower():
            # Be careful: not all "Mac" names capitalize the next letter
            # Only apply if it looks like a Celtic name pattern
            fixed_words.append("Mac" + word[3].upper() + word[4:])
            continue

        # Check for "De/Ne/Le" prefixes that should have both caps (e.g., Dechambeau -> DeChambeau)
        for prefix in NAME_PREFIXES_BOTH_CAPS:
            if word.startswith(prefix) and len(word) > len(prefix) and word[len(prefix)].islower():
                fixed_words.append(prefix + word[len(prefix)].upper() + word[len(prefix) + 1:])
                break
        else:
            # Check for "De la" pattern - lowercase the "la"
            if word == "La" and i > 0 and fixed_words and fixed_words[-1] == "De":
                fixed_words.append("la")
                continue

            fixed_words.append(word)

    return " ".join(fixed_words)


def strip_convert_to_lowercase(string: str, strip_suffixes: bool = True) -> str:
    """Convert string to lowercase alphanumeric, optionally stripping name suffixes.

    Handles Unicode by transliterating to ASCII. Useful for name matching.

    Args:
        string: Input string to process.
        strip_suffixes: If True, strips suffixes like Jr., Sr., III, etc.

    Returns:
        Lowercase alphanumeric string with no spaces or special characters.
    """
    if not string:
        return string

    new_string = normalize_accents(string) if isinstance(string, str) else string

    if strip_suffixes:
        for suff in NAME_SUFFIXES:
            new_string = "".join(new_string.split(suff))
    new_string = "".join(e.lower() for e in new_string if e.isalnum())
    return new_string


def split_name_parts(string: str, strip_suffixes: bool = True) -> List[str, str]:
    """Split human full names into parts: first name and last name.

    First word becomes first name, remaining words become last name.
    Optionally strips common suffixes (Jr., Sr., III, etc.) before splitting.

    Args:
        string: Full string.
        strip_suffixes: If True, removes suffixes

    Returns:
        List containing [first_name, last_name].
    """
    if strip_suffixes:
        string = strip_suffix(string)

    name_parts = string.split()
    first_name = ""
    last_name = ""
    i = 0
    while i < len(name_parts):
        if i == 0:
            first_name = name_parts[i]
        else:
            last_name += name_parts[i] + " "
        i += 1
    last_name = last_name[: len(last_name) - 1]
    return [first_name, last_name]


def normalize_name(string: str, include_suffixes: bool = True) -> str:
    """Normalize a full name for 1:1 comparison

    Removes suffixes, periods, and converts to lowercase alphanumeric.

    Args:
        string: String to clean.

    Returns:
        Cleaned, normalized name string.
    """
    if include_suffixes:
        string = strip_suffix(string)
    string = remove_periods(string)
    string = strip_convert_to_lowercase(string)
    return string


def normalize_last_name_only(string: str, include_suffixes: bool = True) -> str:
    """Normalize just the last name portion.

    Extracts the last word from the name after cleaning.

    Args:
        string: Full string.

    Returns:
        Cleaned, normalized last name only.
    """
    if not include_suffixes:
        string = strip_suffix(string)
    string = remove_periods(string)
    string = string.split(" ")[-1]
    string = strip_convert_to_lowercase(string)
    return string

def jaccard_similarity(str1: str, str2: str) -> float:
    """Calculate Jaccard similarity between two strings based on character sets.

    Args:
        str1: First string.
        str2: Second string.

    Returns:
        Jaccard similarity coefficient (0.0 to 1.0).
    """
    # Tokenize the strings into character sets
    set1 = set(str1)
    set2 = set(str2)
    # Compute Jaccard similarity
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0


def name_similarity(name1: str, name2: str) -> float:
    """Calculate weighted similarity between two names.

    Combines Levenshtein (50%), Jaccard (30%), and Cosine (20%) similarities
    for robust name matching.

    Args:
        name1: First name to compare.
        name2: Second name to compare.

    Returns:
        Combined similarity score (0.0 to 1.0).
    """
    # Normalize the names by lowercasing and stripping whitespaces
    name1 = normalize_name(name1)
    name2 = normalize_name(name2)

    # Levenshtein similarity as a fraction of max length
    lev_sim = 1 - (levenshtein_distance(name1, name2) / max(len(name1), len(name2)))

    # Jaccard similarity
    jaccard_sim = jaccard_similarity(name1, name2)

    # Cosine similarity between word vectors (optional, for extra similarity)
    vectorizer = CountVectorizer().fit_transform([name1, name2])
    vectors = vectorizer.toarray()
    cosine_sim = cosine_similarity(vectors)[0][1]

    # Combine the similarities into a final score (weighted sum)
    final_score = (0.5 * lev_sim) + (0.3 * jaccard_sim) + (0.2 * cosine_sim)

    return final_score
