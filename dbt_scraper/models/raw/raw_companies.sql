{{ config(
    materialized='incremental',
    unique_key='company_id',
    on_schema_change='append',
    pre_hook="
        COPY INTO raw.companies (raw_json)
        FROM @job_stage/reference/company_table.json
        FILE_FORMAT = (TYPE = JSON)
        MATCH_BY_COLUMN_NAME = 'NONE'
        ON_ERROR = CONTINUE;
    "
) }}

SELECT 
    raw_json:id::STRING AS company_id,
    raw_json:name::STRING AS company_name,
    raw_json:industry::STRING AS industry,
    raw_json:company_size::STRING AS company_size,
    raw_json:location::STRING AS headquarters,
    raw_json:website::STRING AS website,
    CURRENT_TIMESTAMP AS ingestion_timestamp
FROM raw.companies
