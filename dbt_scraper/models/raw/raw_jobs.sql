{{ config(
    materialized='incremental',
    unique_key='job_url',
    on_schema_change='append'
) }}

WITH source_lines AS (
    -- 1) Select each line from the Snowflake stage as a string
    SELECT $1 AS raw_line
    FROM @job_stage/job_postings/builtin/latest_jobs.json
),

parsed AS (
    -- 2) Parse each line into a JSON array or object
    SELECT parse_json(raw_line) AS root
    FROM source_lines
),

flattened AS (
    -- 3) Flatten the JSON array (each job posting) into separate rows
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
    FROM parsed,
         LATERAL FLATTEN(input => parsed.root) j

    {% if is_incremental() %}
    -- 4) Only load rows where the job_url is not already in the target table
    WHERE parse_json(j.value):job_url::string NOT IN (
        SELECT job_url FROM {{ this }}
    )
    {% endif %}
)

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
