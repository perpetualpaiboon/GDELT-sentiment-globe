# Parquet Data

Parquet files are the intermediate storage between ingestion and aggregation. Each file corresponds to one 15-minute GKG batch and is written by `ingest_all_files` to `/opt/airflow/pipeline/processed/gkg/YYYY/MM/DD/HH/`. Files older than one day are deleted by `delete_old_data` after each pipeline run.

---

## Columns

The columns below are the subset read by `aggregate_and_write_postgres`. The full Parquet files also contain all raw GKG columns (THEMES, ENHANCEDTHEMES, LOCATIONS, ENHANCEDLOCATIONS, PERSONS, ORGANIZATIONS, etc.) that are not used downstream.

| Column | Type | Description |
|---|---|---|
| DATE | STRING | GKG record timestamp in `YYYYMMDDHHmm` format |
| DOCUMENTIDENTIFIER | STRING | Source article URL |
| SOURCECOMMONNAME | STRING | Human-readable name of the source publication |
| tone | FLOAT | Overall sentiment of the article. Positive = favorable, negative = unfavorable |
| word_count | INTEGER | Number of words in the article. Articles with fewer than 100 words are dropped at parse time |
| primary_country_codes | LIST[STRING] | FIPS country codes of the most-mentioned country. Contains multiple entries if countries tie; ties are discarded in aggregation |
| article_category | STRING | Article category assigned by theme classifier: `politics`, `economy`, `health`, `crime`, or `None` if no category meets the dominance thresholds |
