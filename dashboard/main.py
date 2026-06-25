"""
FastAPI backend for the GDELT Sentiment Globe dashboard.

Serves the static map page and provides two API endpoints:
    GET /api/sentiment:             Returns a GeoJSON FeatureCollection where each country's
                                    properties are enriched with its daily sentiment tone score,
                                    article counts, and per-category breakdowns. Consumed by
                                    the globe on initial load to color each country.

    GET /api/articles/{code}:       Returns a paginated list of news articles for a given country,
                                    filtered by category and sorted by tone. Consumed by the
                                    sidebar when a country is clicked on the globe.
"""

import json
import math
import os
from functools import lru_cache
from pathlib import Path

import pandas as pd
import psycopg2
import pycountry
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

# Paths inside the Docker container.
# The source files are mounted from `src/gdelt/` on the host.
FIPS_TO_ISO_CSV = Path("/app/src/gdelt/fips_to_iso.csv")
GEOJSON_PATH    = Path("/app/src/gdelt/countries.geojson")

CATEGORIES = ["politics", "economy", "health", "crime"]

app = FastAPI(title="GDELT Sentiment Globe")
app.mount("/static", StaticFiles(directory="/app/static", html=True), name="static")


# --- Routes ---

@app.get("/")
def root():
    # Redirect the root URL to the globe page
    return RedirectResponse("/static/index.html")


@app.get("/health")
def health():
    # Used by Docker healthcheck to verify the container is alive and ready to serve traffic.
    return {"status": "ok"}


# --- Database ---

def open_database_connection():
    """
    Opens and returns a psycopg2 connection using environment variables.

    Returns:
        psycopg2.connection: Open database connection.
    """
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        port=int(os.environ.get("DB_PORT", 5432)),
    )


# --- Country Mapping ---

# Some GeoJSON features use ISO_A3 = "-99" for certain territories where no standard ISO 3166-1 alpha-3 code is available.
# These are matched by the country's display name (ADMIN field) instead.
GEOJSON_NAME_OVERRIDES: dict[str, str] = {
    "France": "FRA",
    "Norway": "NOR",
    "Kosovo": "XKX",
}

# Manual FIPS -> ISO alpha-3 overrides for cases where pycountry lookup fails
# due to missing/invalid FIPS codes in the fips_to_iso.csv.
FIPS_MANUAL_OVERRIDES: dict[str, str] = {
    "KV": "XKX",   # Kosovo (iso2="-" in CSV, filtered out before pycountry lookup)
    "WA": "NAM",   # Namibia (iso2 is blank in CSV, pycountry returns None)
    "RB": "SRB",   # Serbia (not in fips_to_iso.csv, GDELT uses this alongside the "RI" code)
}


@lru_cache(maxsize=1)
def load_fips_to_iso_alpha3_mapping() -> dict[str, str]:
    """
    Builds and caches a FIPS-code -> ISO 3166-1 alpha-3 lookup table.

    fips_to_iso.csv provides FIPS -> ISO alpha-2 mappings only.
    These are converted to ISO alpha-3 using pycountry, since GeoJSON data
    expects alpha-3 country codes.

    Returns:
        dict[str, str]: Mapping from FIPS code (e.g. "TH") to ISO alpha-3 (e.g. "THA").
    """
    fips_dataframe = pd.read_csv(FIPS_TO_ISO_CSV, dtype=str)
    fips_dataframe = fips_dataframe[fips_dataframe["iso2"] != "-"]

    mapping = dict(FIPS_MANUAL_OVERRIDES)
    for _, row in fips_dataframe.iterrows():
        try:
            country = pycountry.countries.get(alpha_2=row["iso2"])
            if country:
                mapping[row["fips"]] = country.alpha_3
        except LookupError:
            continue

    return mapping


@lru_cache(maxsize=1)
def load_geojson() -> dict:
    """
    Loads and caches the countries GeoJSON file.

    Returns:
        dict: Parsed GeoJSON FeatureCollection containing country boundary polygons.
    """
    with open(GEOJSON_PATH, encoding="utf-8") as geojson_file:
        return json.load(geojson_file)


# --- API Endpoints ---

@app.get("/api/sentiment")
def get_sentiment():
    """
    Returns a GeoJSON FeatureCollection enriched with sentiment data for each country.

    The response is consumed by the globe frontend (globe.gl) on page load to color each country
    by its daily sentiment tone. 

    Data flow:
        1. Query country_sentiment for aggregated tone scores per country (FIPS codes).
        2. Query country_articles for positive/negative article counts per country per category.
        3. Convert FIPS codes to ISO alpha-3 and group by ISO alpha-3 to merge duplicate FIPS
           codes that map to the same country (e.g. Serbia's "RI" and "RB" both -> "SRB").
        4. Deep-copy cached GeoJSON and attach sentiment data to each matching feature's properties.
        5. Return the enriched GeoJSON alongside summary metadata for the header bar.

    Returns:
        dict:   GeoJSON FeatureCollection with added sentiment properties:
        {
            type:       "FeatureCollection",
            features:   list of GeoJSON features enriched with sentiment data,
            meta:       { window_start, window_end, aggregated_at, countries, total_articles, global_avg_tone }
        }
    """
    database_connection = open_database_connection()
    sentiment_dataframe = pd.read_sql(
        "SELECT * FROM country_sentiment ORDER BY article_count DESC",
        database_connection,
    )
    positive_negative_dataframe = pd.read_sql(
        """
        SELECT country_code, category,
               COUNT(*) FILTER (WHERE tone >= 0) AS positive_count,
               COUNT(*) FILTER (WHERE tone <  0) AS negative_count
        FROM country_articles
        GROUP BY country_code, category
        """,
        database_connection,
    )
    database_connection.close()


    # --- Build positive/negative lookup ---

    # Build lookup: { country_code: { category: { positive, negative }, "all": { positive, negative } } }
    positive_negative_lookup: dict[str, dict] = {}
    for _, row in positive_negative_dataframe.iterrows():
        country_code    = row["country_code"]
        category        = row["category"] or "general"
        if country_code not in positive_negative_lookup:
            positive_negative_lookup[country_code] = {}
        positive_negative_lookup[country_code][category] = {
            "positive": int(row["positive_count"]),
            "negative": int(row["negative_count"]),
        }

    # Add an "all" key per country by computing aggregate article counts across all categories.
    for country_code, category_counts in positive_negative_lookup.items():
        positive_negative_lookup[country_code]["all"] = {
            "positive": sum(counts["positive"] for counts in category_counts.values()),
            "negative": sum(counts["negative"] for counts in category_counts.values()),
        }


    # --- Convert FIPS -> ISO alpha-3 and aggregate ---
    
    fips_mapping = load_fips_to_iso_alpha3_mapping()
    sentiment_dataframe["iso_alpha3"] = sentiment_dataframe["country_code"].map(fips_mapping)
    sentiment_dataframe = sentiment_dataframe.dropna(subset=["iso_alpha3"])

    # Aggregation rules for merging rows that share the same ISO alpha-3 code.
    # Some countries appear under multiple FIPS codes (e.g. Serbia: "RI" and "RB"),
    # so they must be merged into a single country-level record.
    # Tone scores are averaged, article counts are summed, and metadata fields
    # are taken from the first row.
    aggregation_specification = {
        "avg_tone":         ("avg_tone",        "mean"),
        "article_count":    ("article_count",   "sum"),
        "country_code":     ("country_code",    "first"),
        "window_start":     ("window_start",    "first"),
        "window_end":       ("window_end",      "first"),
        "aggregated_at":    ("aggregated_at",   "first"),
    }

    # Add per-category sentiment tone and article count columns to the aggregation if they exist in the dataframe.
    for category in CATEGORIES:
        tone_column  = f"{category}_tone"
        count_column = f"{category}_count"
        if tone_column in sentiment_dataframe.columns:
            aggregation_specification[tone_column]  = (tone_column,     "mean")
            aggregation_specification[count_column] = (count_column,    "sum")

    # Result: { "THA": { avg_tone: -0.5, article_count: 142, politics_tone: ..., ... }, ... }
    sentiment_lookup = (
        sentiment_dataframe.groupby("iso_alpha3")
        .agg(**aggregation_specification)
        .to_dict("index")
    )


    # --- Enrich GeoJSON with sentiment data ---

    geojson = json.loads(json.dumps(load_geojson())) # Deep-copy the cached GeoJSON
    for feature in geojson["features"]:
        iso_alpha3 = feature["properties"].get("ISO_A3")

        # Some GeoJSON features use ISO_A3 = "-99" for certain territories where no standard ISO 3166-1 alpha-3 code is available
        # Fall back to matching by country display name via GEOJSON_NAME_OVERRIDES.
        if iso_alpha3 == "-99":
            iso_alpha3 = GEOJSON_NAME_OVERRIDES.get(feature["properties"].get("ADMIN", ""))

        country_data = sentiment_lookup.get(iso_alpha3, {}) if iso_alpha3 else {}

        if not country_data:
            # No sentiment data available for this country; leave properties unchanged.
            # The frontend treats missing avg_tone as "no data" and applies the default color.
            continue

        feature["properties"]["avg_tone"]       = round(country_data["avg_tone"], 2)
        feature["properties"]["article_count"]  = int(country_data.get("article_count") or 0)
        feature["properties"]["country_code"]   = country_data.get("country_code")

        # Attach positive/negative article counts per category for the sidebar stats.
        country_code = country_data.get("country_code")
        country_positive_negative = positive_negative_lookup.get(country_code, {})
        for category in ["all", "politics", "economy", "health", "crime", "general"]:
            category_counts = country_positive_negative.get(category, {})
            feature["properties"][f"{category}_positive_count"] = category_counts.get("positive", 0)
            feature["properties"][f"{category}_negative_count"] = category_counts.get("negative", 0)

        dominant_category = country_data.get("dominant_category")
        if dominant_category and isinstance(dominant_category, str):
            feature["properties"]["dominant_category"] = dominant_category

        # Attach per-category tone and article count to each feature's properties.
        for category in CATEGORIES:
            tone_value = country_data.get(f"{category}_tone")
            if tone_value is not None and not (
                isinstance(tone_value, float) and math.isnan(tone_value)
            ):
                feature["properties"][f"{category}_tone"]   = round(float(tone_value), 2)
                feature["properties"][f"{category}_count"]  = int(
                    country_data.get(f"{category}_count") or 0
                )

    # --- Build metadata for the header bar ---

    metadata = {}
    if not sentiment_dataframe.empty:
        metadata = {
            "window_start":     str(sentiment_dataframe["window_start"].iloc[0]),
            "window_end":       str(sentiment_dataframe["window_end"].iloc[0]),
            "aggregated_at":    str(sentiment_dataframe["aggregated_at"].iloc[0]),
            "countries":        len(sentiment_dataframe),
            "total_articles":   int(sentiment_dataframe["article_count"].sum()),
            "economy_articles": (
                int(sentiment_dataframe["economy_count"].sum())
                if "economy_count" in sentiment_dataframe.columns
                else None
            ),
            "global_avg_tone": round(float(sentiment_dataframe["avg_tone"].mean()), 2),
        }

    return {
        "type":     "FeatureCollection",
        "features": geojson["features"],
        "meta":     metadata,
    }


@app.get("/api/articles/{country_code}")
def get_articles(
    country_code: str,
    offset: int = 0,
    limit: int = 15,
    category: str = "",
    sort: str = "positive",
):
    """
    Returns paginated articles for a country, sorted by sentiment tone descending.

    Called by the sidebar when a country is clicked and when the user clicks "Load more".

    Args:
        country_code (str): FIPS country code (e.g. "TH" for Thailand).
        offset (int):       Number of articles to skip for pagination. Defaults to 0.
        limit (int):        Number of articles to return per page. Defaults to 15.
        category (str):     Optional category filter. One of: politics, economy, health, crime.
                            Empty string returns articles across all categories.
        sort (str):         "positive" -> highest sentiment tone first, only tone >= 0.
                            "negative" -> lowest sentiment tone first, only tone < 0.

    Returns:
        dict: {
            articles:   list of { url, source, date, tone, category, word_count },
            total:      total matching article count (used by frontend to show/hide "Load more"),
            offset:     the request offset, returned as-is for the frontend to track pagination state.
            limit:      the request limit, returned as-is for the frontend to track pagination state.
        }
    """

    # Build SQL query clauses dynamically based on sort and category filters.
    order_clause    = "tone ASC" if sort == "negative" else "tone DESC"
    tone_filter     = "AND tone < 0" if sort == "negative" else "AND tone >= 0"
    category_filter = "AND category = %s" if category else ""


    # Base query parameters: [country_code] or [country_code, category] if category is specified.
    base_parameters = [country_code] + ([category] if category else [])

    database_connection = open_database_connection()
    try:
        articles_dataframe = pd.read_sql(
            f"""
            SELECT url, source_name, article_date, tone, category, word_count
            FROM country_articles
            WHERE country_code = %s {category_filter} {tone_filter}
            ORDER BY {order_clause}
            LIMIT %s OFFSET %s
            """,
            database_connection,
            params=(*base_parameters, limit, offset),
        )
        count_dataframe = pd.read_sql(
            f"""
            SELECT COUNT(*) AS total_count
            FROM country_articles
            WHERE country_code = %s {category_filter} {tone_filter}
            """,
            database_connection,
            params=tuple(base_parameters),
        )
    finally:
        database_connection.close()

    total_count = int(count_dataframe["total_count"].iloc[0])
    articles = [
        {
            "url":          row["url"],
            "source":       row["source_name"] or "Unknown",
            "date":         str(row["article_date"])[:10] if row["article_date"] else "",
            "tone":         round(float(row["tone"]), 2) if row["tone"] else 0,
            "category":     row.get("category") or "",
            "word_count":   int(row["word_count"]) if pd.notna(row.get("word_count")) else None,
        }
        for _, row in articles_dataframe.iterrows()
    ]

    return {
        "articles": articles,
        "total":    total_count,
        "offset":   offset,
        "limit":    limit,
    }
