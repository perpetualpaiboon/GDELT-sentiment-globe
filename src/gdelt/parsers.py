"""
GDELT GKG field parsers and article classifier.

Provides functions for:
    - Parsing GKG structured fields
    - Identifying primary countries mentioned in an article
    - Classifying news articles into a category

Public Functions:
    - parse_tone()
    - parse_locations()
    - parse_enhanced_field()
    - classify_article_category_by_theme()
    - primary_countries()
"""

from collections import Counter


# --- Tone Field ---

TONE_FIELDS = [
    "tone",
    "positive_score",
    "negative_score",
    "polarity",
    "activity_reference_density",
    "self_group_reference_density",
    "word_count",
]


def parse_tone(raw_string: str | None) -> dict | None:
    """
    Parses the GKG Tone comma-separated field into a dictionary.

    Args:
        raw_string (str | None): Raw Tone string from a GKG record.

    Returns:
        dict | None: Dictionary with tone sub-fields, or None on parse failure.
    """
    if not raw_string or not isinstance(raw_string, str):
        return None

    parts = raw_string.split(",")
    if len(parts) != 7:
        return None

    try:
        numeric_values = [float(part) for part in parts[:6]] + [int(parts[6])]
    except ValueError:
        return None

    return dict(zip(TONE_FIELDS, numeric_values))


# --- Location Field ---

# LOCATION_FIELDS = [
#     "location_type",
#     "location_fullname",
#     "location_country_code",
#     "location_adm1_code",
#     "location_adm2_code",
#     "location_latitude",
#     "location_longitude",
#     "location_feature_id",
# ]


def parse_locations(raw_string: str | None) -> list[dict]:
    """
    Parses the GKG Locations semicolon-separated field into a list of location dicts.

    Args:
        raw_string (str | None): Raw Locations string from a GKG record.

    Returns:
        list[dict]: List of location dictionaries, empty on failure or missing data.
    """
    if not raw_string or not isinstance(raw_string, str):
        return []

    all_locations = []
    for raw_entry in raw_string.split(";"):
        entry = raw_entry.strip()
        if not entry:
            continue

        parts = entry.split("#")
        if len(parts) < 8:
            continue

        # Strip the trailing feature_id sub-field (after the last comma in parts[-1])
        parts[-1] = parts[-1].split(",")[0]

        try:
            all_locations.append({
                "location_type":         parts[0],
                "location_fullname":     parts[1],
                "location_country_code": parts[2],
                "location_adm1_code":    parts[3],
                "location_adm2_code":    parts[4],
                "location_latitude":     float(parts[5]) if parts[5] else None,
                "location_longitude":    float(parts[6]) if parts[6] else None,
                "location_feature_id":   parts[7] if len(parts) > 7 else "",
            })
        except (ValueError, IndexError):
            continue

    return all_locations


# --- Enhanced Field (Themes, Persons, Organizations) ---

def parse_enhanced_field(raw_string: str | None) -> list[tuple[str, int]]:
    """
    Parses any GKG enhanced semicolon-separated field (themes, persons, organizations)
    into a list of (value, character_offset) tuples.

    Args:
        raw_string (str | None): Raw enhanced field string from a GKG record.

    Returns:
        list[tuple[str, int]]: List of (value, offset) pairs, empty on failure.
    """
    if not raw_string or not isinstance(raw_string, str):
        return []

    parsed_entries = []
    for raw_entry in raw_string.split(";"):
        entry = raw_entry.strip()
        if "," not in entry:
            continue

        value, _, offset_string = entry.rpartition(",")
        try:
            parsed_entries.append((value, int(offset_string)))
        except ValueError:
            continue

    return parsed_entries


# --- Translation Info Field ---

def parse_translationinfo(raw_string: str | None) -> str | None:
    """
    Extracts the source language code (srclc) from the GKG TRANSLATIONINFO field.

    Args:
        raw_string (str | None): Raw TRANSLATIONINFO string from a GKG record.

    Returns:
        str | None: ISO 639-2 language code, or None if not found.
    """
    if not raw_string or not isinstance(raw_string, str):
        return None

    for part in raw_string.split(";"):
        stripped_part = part.strip()
        if stripped_part.startswith("srclc:"):
            return stripped_part[len("srclc:"):]

    return None


# --- Country Assignment ---

def primary_countries(enhancedlocations_raw: str | None) -> list[str]:
    """
    Returns a list of FIPS country codes that best represent an article's coverage.

    Rules:
      1. Count all location mentions regardless of location type.
      2. Return all countries tied at the maximum mention count (sorted).
      3. Return empty list if no location data exists.

    Args:
        enhancedlocations_raw (str | None): Raw Locations field.

    Returns:
        list[str]: Sorted list of FIPS country codes (may be empty).
    """
    all_locations = parse_locations(enhancedlocations_raw)
    if not all_locations:
        return []

    mention_counts = Counter(
        location_entry["location_country_code"]
        for location_entry in all_locations
        if location_entry["location_country_code"]
    )
    if not mention_counts:
        return []

    maximum_count = mention_counts.most_common(1)[0][1]
    return sorted(
        country_code
        for country_code, mention_count in mention_counts.items()
        if mention_count == maximum_count
    )


# --- Article Classifier ---

_THEME_CATEGORIES: dict[str, list[str]] = {
    "politics": [
        "GOVERNMENT", "DIPLOMACY", "POLITIC", "ELECTION",
        "DEMOCRA", "DICTAT", "GOVERNANCE", "CORRUPT", "LEGISLAT",
    ],
    "economy": [
        "ECON", "FINANC", "MARKET", "BUSINESS", "TAXES", "TAXATION",
        "INFLATION", "ENTERPRISE", "BANK", "COMMODI",
    ],
    "health": [
        "HEALTH", "DISEASE", "MEDICAL", "SANITA", "PHARMA",
        "PANDEMIC", "VACCINATION", "IMMUNIZATIONS", "DIABETES", "POISON",
    ],
    "crime": [
        "CRIME", "CRIMINAL", "INVESTIGATION", "TERROR", "KIDNAP",
        "SMUGGLING", "TRAFFICKING", "ASSASSINATION", "GENOCIDE",
        "VIOLEN", "RAPE", "FRAUD", "TORTURE",
    ],
}

_CATEGORY_MINIMUM_PERCENTAGE    = 50.0
_CATEGORY_MINIMUM_WINNING_GAP   = 30.0
_THEME_MINIMUM_HITS = {
    "crime":    3,
    "health":   3,
    "politics": 3,
    "economy":  3,
}


def _get_matching_categories_for_theme(theme: str) -> list[str]:
    """
    Returns the list of categories whose prefix patterns match any token in the theme.

    Args:
        theme (str): A single GKG theme string (e.g. "WB_HEALTH_PANDEMIC").

    Returns:
        list[str]: Category names that match the theme's tokens.
    """
    tokens = theme.upper().split("_")
    matching_categories = []
    for category, patterns in _THEME_CATEGORIES.items():
        for token in tokens:
            for pattern in patterns:
                if token.startswith(pattern):
                    matching_categories.append(category)
                    break
            else:
                continue
            break
    return matching_categories


def classify_article_category_by_theme(enhancedthemes_raw: str | None) -> str | None:
    """
    Classifies an article into a category using prefix matching on ENHANCEDTHEMES.

    Rules:
      - The winning category must have > _CATEGORY_MINIMUM_PERCENTAGE % of all matched hits.
      - The winning category must lead the runner-up by > _CATEGORY_MINIMUM_WINNING_GAP percentage points.
      - The winning category must have >= _THEME_MINIMUM_HITS[category] hits.

    Args:
        enhancedthemes_raw (str | None): Raw ENHANCEDTHEMES field from a GKG record.

    Returns:
        str | None: Category name ('politics', 'economy', 'health', 'crime'), or None.
    """
    theme_entries = parse_enhanced_field(enhancedthemes_raw)
    if not theme_entries:
        return None

    category_scores: dict[str, int] = {}
    for theme, _ in theme_entries:
        for category in _get_matching_categories_for_theme(theme):
            category_scores[category] = category_scores.get(category, 0) + 1

    if not category_scores:
        return None

    category_scores = {
        category: score
        for category, score in category_scores.items()
        if score >= _THEME_MINIMUM_HITS.get(category, 1)
    }
    if not category_scores:
        return None

    total_hits = sum(category_scores.values())
    sorted_percentages = sorted(
        (score / total_hits * 100 for score in category_scores.values()),
        reverse=True,
    )
    winner_percentage = sorted_percentages[0]
    runner_up_percentage = sorted_percentages[1] if len(sorted_percentages) > 1 else 0.0

    if winner_percentage <= _CATEGORY_MINIMUM_PERCENTAGE:
        return None
    if (winner_percentage - runner_up_percentage) <= _CATEGORY_MINIMUM_WINNING_GAP:
        return None

    winning_category = max(category_scores, key=category_scores.get)
    return winning_category
