{{ config(
    materialized = 'incremental',
    unique_key   = 'root_string',
    on_schema_change = 'append'
) }}

WITH source_lines AS (
    -- 1) Read each line from the Snowflake stage.
    SELECT $1 AS raw_line
    FROM @JOB_SCRAPING.RAW.JOB_STAGE/job_postings/builtin/latest_jobs.json

),

parsed AS (
    -- 2) Parse each line into JSON, skip blank lines
    SELECT
        parse_json(raw_line) AS root,
        to_varchar(parse_json(raw_line)) AS root_string
    FROM source_lines
    WHERE parse_json(raw_line) IS NOT NULL

    {% if is_incremental() %}
      -- Only insert lines that haven't been loaded (based on our root_string)
      AND to_varchar(parse_json(raw_line)) NOT IN (
          SELECT root_string FROM {{ this }}
      )
    {% endif %}
)

-- 3) Final SELECT: store one row per raw JSON line, plus a string key for incremental merges
SELECT
    root,
    root_string
FROM parsed
