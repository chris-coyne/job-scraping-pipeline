{{ config(
    materialized = 'incremental',
    unique_key   = 'job_url',
    on_schema_change = 'append'
) }}

WITH raw AS (
    -- 1) Pull the raw JSON (VARIANT) from raw_jobs
    SELECT root
    FROM {{ ref('raw_jobs') }}
),

flattened AS (
    -- 2) Flatten that JSON. If each line is an array of jobs, we LATERAL FLATTEN on `root`.
    --    Then parse out each field, adjusting names/data types as needed.
    SELECT
        parse_json(j.value):company_id::int             AS company_id,
        parse_json(j.value):date_added::timestamp_ntz   AS date_added,
        parse_json(j.value):job_description::string     AS job_description,
        parse_json(j.value):job_title::string           AS job_title,
        parse_json(j.value):job_url::string             AS job_url,
        parse_json(j.value):level::string               AS level,
        parse_json(j.value):location::string            AS location,
        parse_json(j.value):salary::string              AS salary,
        parse_json(j.value):searched_job_title::string  AS searched_job_title,
        parse_json(j.value):source::string              AS source
    FROM raw,
         LATERAL FLATTEN(input => raw.root) j

    {% if is_incremental() %}
      -- 3) Only load records whose job_url isn't already in stg_jobs
      WHERE parse_json(j.value):job_url::string NOT IN (
          SELECT job_url FROM {{ this }}
      )
    {% endif %}
)

-- 4) Final SELECT
SELECT
    company_id,
    date_added,
    job_description,
    job_title,
    job_url,
    level,
    location,
    salary,
    searched_job_title,
    source
FROM flattened
