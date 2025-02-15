{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append'
) }}

WITH source_lines AS (
    -- 1) Select each line from your companies stage as a string
    SELECT $1 AS raw_line
    FROM @job_stage/reference/company_table.json
),

parsed AS (
    -- 2) Parse each line into a JSON object
    SELECT parse_json(raw_line) AS root
    FROM source_lines
    WHERE parse_json(raw_line) IS NOT NULL
),

flattened AS (
    -- 3) Flatten the JSON object so each key is a row
    SELECT
        t.key::string    AS company_name,
        t.value:id::number AS id
    FROM parsed,
         LATERAL FLATTEN(input => parsed.root) t

    {% if is_incremental() %}
    -- 4) Only load rows where the "id" isn't already in our target table
    WHERE t.value:id::number NOT IN (
        SELECT id FROM {{ this }}
    )
    {% endif %}
)

-- 5) Final SELECT
SELECT
    id,
    company_name
FROM flattened
