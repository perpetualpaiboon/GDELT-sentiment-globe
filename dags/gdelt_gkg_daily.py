"""
Airflow DAG: gdelt_gkg_daily

Downloads GDELT GKG files for the previous UTC day,
parses and stores them as Parquet, 
then aggregates country-level sentiment into PostgreSQL.

Schedule: 20:00 UTC (03:00 ICT) daily
"""

import shutil
import zipfile
import io
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


# --- Constants ---

MASTER_URL      = "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
PROCESSED_DIR   = Path("/opt/airflow/pipeline/processed/gkg")
RETENTION_DAYS  = 1
MIN_WORD_COUNT  = 100   #  Exclude short articles during ingestion
MIN_ARTICLES    = 10    # minimum articles before a country enters PostgreSQL for country-level aggregation
CATEGORIES      = ["politics", "economy", "health", "crime"]

GKG_COLUMNS = [
    "GKGRECORDID",
    "DATE",
    "SOURCECOLLECTIONIDENTIFIER",
    "SOURCECOMMONNAME",
    "DOCUMENTIDENTIFIER",
    "COUNTS",
    "ENHANCEDCOUNTS",
    "THEMES",
    "ENHANCEDTHEMES",
    "LOCATIONS",
    "ENHANCEDLOCATIONS",
    "PERSONS",
    "ENHANCEDPERSONS",
    "ORGANIZATIONS",
    "ENHANCEDORGANIZATIONS",
    "TONE",
    "ENHANCEDDATES",
    "GCAM",
    "SHARINGIMAGE",
    "RELATEDIMAGES",
    "SOCIALIMAGEEMBEDS",
    "SOCIALVIDEOEMBEDS",
    "QUOTATIONS",
    "ALLNAMES",
    "AMOUNTS",
    "TRANSLATIONINFO",
    "EXTRASXML",
]


# --- SQL Statements ---

CREATE_TABLE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS country_sentiment (
        country_code  VARCHAR(10)  PRIMARY KEY,
        avg_tone      FLOAT,
        article_count INTEGER,
        window_start  TIMESTAMP,
        window_end    TIMESTAMP,
        aggregated_at TIMESTAMP
    )
    """,
    "ALTER TABLE country_sentiment ADD COLUMN IF NOT EXISTS politics_tone     FLOAT",
    "ALTER TABLE country_sentiment ADD COLUMN IF NOT EXISTS politics_count    INTEGER",
    "ALTER TABLE country_sentiment ADD COLUMN IF NOT EXISTS economy_tone      FLOAT",
    "ALTER TABLE country_sentiment ADD COLUMN IF NOT EXISTS economy_count     INTEGER",
    "ALTER TABLE country_sentiment ADD COLUMN IF NOT EXISTS health_tone       FLOAT",
    "ALTER TABLE country_sentiment ADD COLUMN IF NOT EXISTS health_count      INTEGER",
    "ALTER TABLE country_sentiment ADD COLUMN IF NOT EXISTS crime_tone        FLOAT",
    "ALTER TABLE country_sentiment ADD COLUMN IF NOT EXISTS crime_count       INTEGER",
    """
    CREATE TABLE IF NOT EXISTS country_articles (
        country_code    VARCHAR(10),
        url             TEXT,
        source_name     VARCHAR(255),
        article_date    TIMESTAMP,
        tone            FLOAT,
        category        VARCHAR(20),
        word_count      INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS index_country_articles_country_tone     ON country_articles (country_code, tone DESC)",
    "CREATE INDEX IF NOT EXISTS index_country_articles_country_tone_asc ON country_articles (country_code, tone ASC)",
    "CREATE INDEX IF NOT EXISTS index_country_articles_country_cat      ON country_articles (country_code, category)",
]

UPSERT_SENTIMENT_SQL = """
INSERT INTO country_sentiment (
    country_code, avg_tone, article_count,
    politics_tone, politics_count,
    economy_tone, economy_count,
    health_tone, health_count,
    crime_tone, crime_count,
    window_start, window_end, aggregated_at
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (country_code) DO UPDATE SET
    avg_tone           = EXCLUDED.avg_tone,
    article_count      = EXCLUDED.article_count,
    politics_tone      = EXCLUDED.politics_tone,
    politics_count     = EXCLUDED.politics_count,
    economy_tone       = EXCLUDED.economy_tone,
    economy_count      = EXCLUDED.economy_count,
    health_tone        = EXCLUDED.health_tone,
    health_count       = EXCLUDED.health_count,
    crime_tone         = EXCLUDED.crime_tone,
    crime_count        = EXCLUDED.crime_count,
    window_start       = EXCLUDED.window_start,
    window_end         = EXCLUDED.window_end,
    aggregated_at      = EXCLUDED.aggregated_at;
"""

INSERT_ARTICLES_SQL = """
INSERT INTO country_articles (country_code, url, source_name, article_date, tone, category, word_count)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""


# --- File Fetch Functions ---

def get_day_parquet_paths(date: datetime) -> list[Path]:
    """
    Returns all Parquet file paths under the directory for the given date.

    Args:
        date (datetime): The target date.

    Returns:
        list[Path]: List of Parquet file paths, empty if the directory does not exist.
    """
    day_directory = PROCESSED_DIR / f"{date.year}/{date.month:02d}/{date.day:02d}"
    if not day_directory.exists():
        return []
    return list(day_directory.rglob("*.parquet"))


# --- Parse Functions ---

def parse_gkg_file(url: str) -> pd.DataFrame:
    """
    Downloads a GKG zip file, parses it, and returns a filtered DataFrame.

    Applies tone parsing, word count filtering, country assignment,
    and article category classification.

    Args:
        url (str): URL of the GKG zip file to download.

    Returns:
        pd.DataFrame: Parsed and filtered DataFrame for one GKG file.
    """
    from src.gdelt.parsers import (
        parse_tone,
        classify_article_category_by_theme,
        primary_countries,
    )

    response = requests.get(url, timeout=300, stream=True)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_archive:
        csv_filename = next(
            name for name in zip_archive.namelist()
            if name.lower().endswith(".csv")
        )
        with zip_archive.open(csv_filename) as csv_file:
            raw_dataframe = pd.read_csv(
                csv_file,
                sep="\t",
                header=None,
                names=GKG_COLUMNS,
                dtype=str,
                encoding="latin-1",
                on_bad_lines="warn",
            )

    tone_parsed = raw_dataframe["TONE"].apply(parse_tone)
    tone_columns = pd.json_normalize(tone_parsed)
    parsed_dataframe = pd.concat(
        [raw_dataframe.drop(columns=["TONE"]), tone_columns],
        axis=1,
    )

    parsed_dataframe = parsed_dataframe[
        parsed_dataframe["word_count"].notna()
        & (parsed_dataframe["word_count"] >= MIN_WORD_COUNT)
    ]

    parsed_dataframe["primary_country_codes"] = parsed_dataframe["ENHANCEDLOCATIONS"].apply(
        primary_countries
    )
    parsed_dataframe["article_category"] = parsed_dataframe["ENHANCEDTHEMES"].apply(
        classify_article_category_by_theme
    )

    return parsed_dataframe


# --- DAG Definition ---

@dag(
    dag_id="gdelt_gkg_daily",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule="0 20 * * *",
    catchup=False,
    tags=["gdelt", "gkg"],
    params={"target_date": ""},
    default_args={"retries": 999, "retry_delay": timedelta(minutes=1)},
)
def gdelt_gkg_daily():
    """Daily pipeline: fetch → ingest → aggregate → write to PostgreSQL."""

    @task()
    def fetch_yesterday_urls(params=None) -> list[str]:
        """
        Fetches GKG translation file URLs for the target date from the GDELT master list.

        Args:
            params (dict | None): Optional DAG run params. Accepts 'target_date' (YYYY-MM-DD).
                [Defaults to yesterday's UTC date if not provided].

        Returns:
            list[str]: List of GKG zip file URLs for the target date.
        """
        target_date_string = params.get("target_date", "") if params else ""
        if target_date_string:
            target_date = datetime.strptime(target_date_string, "%Y-%m-%d").date()
        else:
            target_date = datetime.now(tz=timezone.utc).date() - timedelta(days=1)

        date_prefix = target_date.strftime("%Y%m%d")

        response = requests.get(MASTER_URL, timeout=60)
        response.raise_for_status()

        all_urls = [
            line.strip().split()[-1]
            for line in response.text.strip().splitlines()
            if line.strip().endswith(".gkg.csv.zip") and date_prefix in line
        ]
        print(f"[FETCH] Found {len(all_urls)} GKG files for {target_date}")
        return all_urls

    @task()
    def ingest_all_files(urls: list[str]) -> None:
        """
        Downloads and parses each GKG file, writing the result to a partitioned Parquet file.

        Args:
            urls (list[str]): List of GKG zip file URLs to download and ingest.
        """
        for url in urls:
            filename = url.split("/")[-1]
            parsed_dataframe = parse_gkg_file(url)

            date_string = parsed_dataframe["DATE"].iloc[0]
            file_datetime = datetime.strptime(date_string[:12], "%Y%m%d%H%M")
            partition_directory = (
                PROCESSED_DIR
                / f"{file_datetime.year}/{file_datetime.month:02d}"
                / f"{file_datetime.day:02d}/{file_datetime.hour:02d}"
            )
            partition_directory.mkdir(parents=True, exist_ok=True)

            output_path = partition_directory / filename.replace(".gkg.csv.zip", ".parquet")
            parsed_dataframe.to_parquet(output_path, index=False)
            print(f"[INGEST] Written: {output_path}")

    @task()
    def delete_old_data() -> None:
        """Delete Parquet day directories older than RETENTION_DAYS and remove empty parent directories."""
        cutoff_date = datetime.now(tz=timezone.utc).date() - timedelta(days=RETENTION_DAYS)
        if not PROCESSED_DIR.exists():
            return
        for year_directory in PROCESSED_DIR.iterdir():
            for month_directory in year_directory.iterdir():
                for day_directory in month_directory.iterdir():
                    try:
                        directory_date = datetime.strptime(
                            f"{year_directory.name}/{month_directory.name}/{day_directory.name}",
                            "%Y/%m/%d",
                        ).date()
                    except ValueError:
                        continue
                    if directory_date < cutoff_date:
                        shutil.rmtree(day_directory)
                        print(f"[CLEANUP] Deleted: {day_directory}")
                if not any(month_directory.iterdir()):
                    month_directory.rmdir()
            if not any(year_directory.iterdir()):
                year_directory.rmdir()

    @task()
    def aggregate_and_write_postgres(params=None) -> None:
        """
        Reads Parquet files for the target date, aggregates country-level tone scores,
        applies Bayesian shrinkage, and writes results to PostgreSQL.

        Args:
            params (dict | None): Optional DAG run params. Accepts 'target_date' (YYYY-MM-DD).
                [Defaults to yesterday's UTC date if not provided].
        """
        target_date_string = params.get("target_date", "") if params else ""
        if target_date_string:
            target_date = datetime.strptime(target_date_string, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        else:
            today = datetime.now(tz=timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            target_date = today - timedelta(days=1)

        aggregated_at = datetime.now(tz=timezone.utc)
        parquet_paths = get_day_parquet_paths(target_date)
        if not parquet_paths:
            raise FileNotFoundError("No Parquet files found for yesterday.")


        # --- Accumulators ---

        def make_empty_accumulator() -> dict:
            """
            Returns a fresh accumulator dict for log-weighted tone aggregation.

            Returns:
                dict: Accumulator with sum_tone_weighted, sum_weights, and count set to zero.
            """
            return {"sum_tone_weighted": 0.0, "sum_weights": 0.0, "count": 0}

        overall_accumulator  = defaultdict(make_empty_accumulator)
        category_accumulator = {cat: defaultdict(make_empty_accumulator) for cat in CATEGORIES}
        seen_urls: set[str] = set()
        total_articles_written = 0

        def has_country_codes(value) -> bool:
            """
            Returns True if the value is a non-empty list-like.

            Args:
                value: The primary_country_codes value for one row.

            Returns:
                bool: True if the article has at least one assigned country.
            """
            return value is not None and hasattr(value, "__len__") and len(value) > 0

        def parse_article_date(raw_date) -> datetime | None:
            """
            Parses a raw GKG DATE string into a datetime object.

            Args:
                raw_date: Raw DATE value from a GKG record.

            Returns:
                datetime | None: Parsed datetime, or None if the value is missing or invalid.
            """
            if pd.notna(raw_date) and raw_date:
                return datetime.strptime(str(raw_date)[:12], "%Y%m%d%H%M")
            return None

        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        with postgres_hook.get_conn() as database_connection:
            with database_connection.cursor() as database_cursor:
                for statement in CREATE_TABLE_STATEMENTS:
                    database_cursor.execute(statement)
                database_cursor.execute("TRUNCATE TABLE country_sentiment;")
                database_cursor.execute("TRUNCATE TABLE country_articles;")
            database_connection.commit()

            for parquet_path in parquet_paths:
                dataframe = pd.read_parquet(parquet_path, columns=[
                    "DATE", "DOCUMENTIDENTIFIER", "SOURCECOMMONNAME",
                    "tone", "word_count", "primary_country_codes", "article_category",
                ])
                dataframe = dataframe[dataframe["word_count"].notna() & dataframe["tone"].notna()]
                dataframe = dataframe[dataframe["word_count"] > 0]
                dataframe = dataframe[dataframe["primary_country_codes"].apply(has_country_codes)]

                # Keep articles with exactly one primary country; discard ties
                dataframe = dataframe[dataframe["primary_country_codes"].apply(lambda x: len(x) == 1)]
                dataframe["primary_country_code"] = dataframe["primary_country_codes"].apply(lambda x: x[0])
                dataframe = dataframe[dataframe["primary_country_code"].str.strip() != ""]
                dataframe["log_word_count"] = np.log(dataframe["word_count"])

                classified_dataframe = dataframe[dataframe["article_category"].notna()]

                # Overall accumulation
                for country_code, group in dataframe.groupby("primary_country_code"):
                    acc = overall_accumulator[country_code]
                    acc["sum_tone_weighted"] += (group["tone"] * group["log_word_count"]).sum()
                    acc["sum_weights"]       += group["log_word_count"].sum()
                    acc["count"]             += len(group)

                # Per-category accumulation
                for category in CATEGORIES:
                    subset = classified_dataframe[classified_dataframe["article_category"] == category]
                    for country_code, group in subset.groupby("primary_country_code"):
                        acc = category_accumulator[category][country_code]
                        acc["sum_tone_weighted"] += (group["tone"] * group["log_word_count"]).sum()
                        acc["sum_weights"]       += group["log_word_count"].sum()
                        acc["count"]             += len(group)

                # Write articles to country_articles for sidebar, skipping duplicate URLs
                dataframe["article_category"] = dataframe["article_category"].fillna("general")
                articles_subset = dataframe[
                    dataframe["DOCUMENTIDENTIFIER"].notna() & dataframe["tone"].notna()
                ]
                article_records = []
                for _, row in articles_subset.iterrows():
                    url = row["DOCUMENTIDENTIFIER"]
                    if url not in seen_urls:
                        seen_urls.add(url)
                        article_records.append((
                            row["primary_country_code"],
                            url,
                            row["SOURCECOMMONNAME"],
                            parse_article_date(row["DATE"]),
                            row["tone"],
                            row["article_category"],
                            int(row["word_count"]) if pd.notna(row["word_count"]) else None,
                        ))

                if article_records:
                    with database_connection.cursor() as database_cursor:
                        database_cursor.executemany(INSERT_ARTICLES_SQL, article_records)
                    database_connection.commit()
                    total_articles_written += len(article_records)

                del dataframe, articles_subset, article_records


        # --- Build aggregation DataFrame ---

        aggregated_rows = []
        for country_code, acc in overall_accumulator.items():
            if acc["sum_weights"] == 0:
                continue
            row = {
                "primary_country_code": country_code,
                "avg_tone":      acc["sum_tone_weighted"] / acc["sum_weights"],
                "article_count": acc["count"],
            }
            for category in CATEGORIES:
                cat_acc = category_accumulator[category].get(country_code)
                row[f"{category}_tone"] = (
                    cat_acc["sum_tone_weighted"] / cat_acc["sum_weights"]
                    if cat_acc and cat_acc["sum_weights"] else None
                )
                row[f"{category}_count"] = cat_acc["count"] if cat_acc else 0
            aggregated_rows.append(row)

        if not aggregated_rows:
            raise ValueError("No rows accumulated — all articles filtered out.")

        aggregated_dataframe = pd.DataFrame(aggregated_rows)


        # Shrink country-level tone toward neutral (0) based on article volume, with stronger shrinkage 
        # for low-volume countries to reduce extreme scores        
        #
        # Preserves tone direction; positive values remain positive and negative values remain negative

        QUANTILE = 0.60
        shrinkage_k = float(aggregated_dataframe["article_count"].quantile(QUANTILE))
        aggregated_dataframe["avg_tone"] = (
            aggregated_dataframe["avg_tone"]
            * aggregated_dataframe["article_count"]
            / (aggregated_dataframe["article_count"] + shrinkage_k)
        )
        print(f"[AGG] Shrinkage K (60th percentile): {shrinkage_k:.1f}")

        aggregated_dataframe = aggregated_dataframe[
            aggregated_dataframe["article_count"] >= MIN_ARTICLES
        ]


        # --- Write country-level tone score to PostgreSQL ---

        postgres_hook2 = PostgresHook(postgres_conn_id="postgres_default")
        with postgres_hook2.get_conn() as database_connection:
            with database_connection.cursor() as database_cursor:
                sentiment_records = [
                    (
                        row["primary_country_code"],
                        row["avg_tone"],
                        int(row["article_count"]),
                        row.get("politics_tone"),  int(row.get("politics_count") or 0),
                        row.get("economy_tone"),   int(row.get("economy_count") or 0),
                        row.get("health_tone"),    int(row.get("health_count") or 0),
                        row.get("crime_tone"),     int(row.get("crime_count") or 0),
                        target_date,
                        target_date + timedelta(days=1),
                        aggregated_at,
                    )
                    for _, row in aggregated_dataframe.iterrows()
                ]
                database_cursor.executemany(UPSERT_SENTIMENT_SQL, sentiment_records)
            database_connection.commit()

        print(f"[AGG] Written {len(aggregated_dataframe)} countries to country_sentiment")
        print(f"[AGG] Written {total_articles_written} articles to country_articles")


    # --- DAG Task Graph ---

    urls_task    = fetch_yesterday_urls()
    cleanup_task = delete_old_data()
    ingest_task  = ingest_all_files(urls_task)
    urls_task >> cleanup_task >> ingest_task >> aggregate_and_write_postgres()


gdelt_gkg_daily()
