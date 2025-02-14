{{ config(
    materialized='incremental',
    unique_key='job_id',
    on_schema_change='append',
    pre_hook="
        COPY INTO raw.jobs (raw_json)
        FROM @job_stage/job_postings/builtin/latest_jobs.json
        FILE_FORMAT = (TYPE = JSON)
        MATCH_BY_COLUMN_NAME = 'NONE'
        ON_ERROR = CONTINUE;
    "
) }}

SELECT 
    raw_json:id::STRING AS job_id,
    raw_json:title::STRING AS job_title,
    raw_json:company::STRING AS company_name,
    raw_json:location::STRING AS location,
    raw_json:posted_date::TIMESTAMP AS posted_date,
    raw_json:employment_type::STRING AS employment_type,
    raw_json:remote_status::STRING AS remote_status,
    raw_json:salary_range::STRING AS salary_range,
    raw_json:description::STRING AS job_description,
    raw_json:requirements::STRING AS job_requirements,
    CURRENT_TIMESTAMP AS ingestion_timestamp
FROM raw.jobs
