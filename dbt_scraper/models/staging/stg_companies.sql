{{ config(
    materialized = 'incremental',
    unique_key   = 'id',
    on_schema_change = 'append'
) }}

WITH raw AS (
    SELECT root
    FROM {{ ref('raw_companies') }}
),

flattened AS (
    SELECT
        t.key::string AS company_name,
        t.value:id::number AS id
    FROM raw,
         LATERAL FLATTEN(input => raw.root) t

    {% if is_incremental() %}
      -- Only load new "id" values that aren't already in this table
      WHERE t.value:id::number NOT IN (
          SELECT id FROM {{ this }}
      )
    {% endif %}
)

SELECT
    id,
    company_name,
    current_timestamp() as dbt_loaded_at
FROM flattened
