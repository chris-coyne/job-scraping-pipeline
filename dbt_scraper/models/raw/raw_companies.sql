{{ config(
    materialized = 'incremental',
    unique_key   = 'root_string',
    on_schema_change = 'append'
) }}

WITH source_lines AS (
    -- 1) Select each line from your Snowflake stage as a string
    SELECT 
        $1 AS raw_line
    FROM @JOB_SCRAPING.RAW.JOB_STAGE/reference/company_table.json
),

parsed AS (
    -- 2) Parse each line into a JSON object, ignore blank/malformed lines
    SELECT 
        parse_json(raw_line) AS root,
        -- We'll use this for an incremental MERGE key:
        to_varchar(parse_json(raw_line)) AS root_string  
        -- ^ In Snowflake, to_varchar(VARIANT) -> string representation
    FROM source_lines
    WHERE parse_json(raw_line) IS NOT NULL
    
    {% if is_incremental() %}
      AND to_varchar(parse_json(raw_line)) NOT IN (
          SELECT root_string FROM {{ this }}
      )
    {% endif %}
)

SELECT
    root,
    root_string,
    current_timestamp() as dbt_loaded_at
FROM parsed
