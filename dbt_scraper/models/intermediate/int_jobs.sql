SELECT
    j.company_id,
    c.company_name,
    date(j.date_added) as date_added,
    j.job_title,
    j.level,
    j.location,
    j.salary,
    j.job_description,
    j.job_url,
    j.searched_job_title,
    j.source,
    current_timestamp() as dbt_run_timestamp
FROM {{ ref('stg_companies') }} c
LEFT JOIN {{ ref('stg_jobs') }} j
    ON c.id = j.company_id
