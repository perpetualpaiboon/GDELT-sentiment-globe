# PostgreSQL Data

Both tables are written by `aggregate_and_write_postgres`. The task truncates both tables before each write, so they always reflect the most recently processed day.

---

## Tables

- [country_sentiment](#country_sentiment)
- [country_articles](#country_articles)

---

### country_sentiment

**Purpose:** One row per country. Stores the daily sentiment score and per-category breakdown used to color each country on the globe.

| Column | Type | Description |
|---|---|---|
| country_code | VARCHAR(10) | FIPS country code. Primary key |
| avg_tone | FLOAT | Log-weighted sentiment score after Bayesian shrinkage toward neutral (0) |
| article_count | INTEGER | Number of articles that contributed to this country's score |
| politics_tone | FLOAT | Log-weighted sentiment score for politics articles. NULL if no politics articles |
| politics_count | INTEGER | Number of politics articles for this country |
| economy_tone | FLOAT | Log-weighted sentiment score for economy articles. NULL if no economy articles |
| economy_count | INTEGER | Number of economy articles for this country |
| health_tone | FLOAT | Log-weighted sentiment score for health articles. NULL if no health articles |
| health_count | INTEGER | Number of health articles for this country |
| crime_tone | FLOAT | Log-weighted sentiment score for crime articles. NULL if no crime articles |
| crime_count | INTEGER | Number of crime articles for this country |
| window_start | TIMESTAMP | Start of the target date (midnight UTC) |
| window_end | TIMESTAMP | End of the target date (midnight UTC the following day) |
| aggregated_at | TIMESTAMP | Timestamp when the aggregation task ran |

---

### country_articles

**Purpose:** One row per article. Stores article-level data used to display on the sidebar when a country is clicked.

| Column | Type | Description |
|---|---|---|
| country_code | VARCHAR(10) | FIPS country code the article is assigned to |
| url | TEXT | Source article URL |
| source_name | VARCHAR(255) | Human-readable name of the source publication |
| article_date | TIMESTAMP | Publication timestamp parsed from the GKG DATE field |
| tone | FLOAT | Raw GDELT tone score (not shrunk) |
| category | VARCHAR(20) | Article category: `politics`, `economy`, `health`, `crime`, or `general` |
| word_count | INTEGER | Number of words in the article |
