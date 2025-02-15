# Job Scraping & Analytics Pipeline

## Overview
This project is focused on scraping, storing, and analyzing job postings for remote **Data Analyst, Senior Data Analyst, Analytics Engineer, and Senior Analytics Engineer** roles. The goal is to build a complete data pipeline that extracts job postings from **LinkedIn** and **BuiltIn**, processes the data, and visualizes trends.

## Tech Stack
- **Python** (BeautifulSoup, Selenium/Playwright for web scraping)
- **Snowflake** (Cloud-based data warehouse for scalable storage and querying)
- **dbt** (Data modeling and transformation)
- **GitHub Actions** (CI/CD for SQL linting and automation)
- **Tableau / Power BI / Streamlit** (Visualization and analysis)

## Data Collection Plan
We will extract job postings with the following fields:
| **Field**          | **Description** |
|--------------------|----------------|
| `job_title`       | The title of the position |
| `company_name`    | The company offering the job |
| `location`        | Remote (or any exceptions) |
| `job_description` | The full job description |
| `employment_type` | Full-time, contract, internship, etc. |
| `posted_date`     | When the job was listed |
| `salary`         | If available (some postings list salary) |
| `job_url`        | Link to the job posting |
| `source`         | Whether it came from LinkedIn or BuiltIn |

## Project Structure
```
job-scraping-pipeline/
│── scraper/         # Web scraping scripts
│── data/            # Raw & cleaned job data
│── dbt_models/      # SQL models for transformation
│── tests/           # CI/CD checks (sqlfluff, pytest, etc.)
│── notebooks/       # Exploratory analysis
│── docs/            # Documentation & findings
│── requirements.txt # Python dependencies
│── config/          # Credentials (excluded via .gitignore)
```

## AWS Lambda
After written and tested locally, the scraper .py files were converted to zip files to be used within AWS Lambda. This function writes .json to an S3 bucket daily with three outputs: a job_postings daily output with job search results, an all_jobs output combining the results of the daily job_postings files (while removing any duplicates), and a companies file tracking each company and ID. The script only saves the prior 14 days worth of runs, limiting stale postings and an ever-growing output in the S3 bucket and all_jobs file. 

### IAM
Role created for lambda function to grant read/write permissions to S3 bucket.

### Scheduling Setup
Lambda function is scheduled to run daily at 9am cental with AWS Event Bridge.

## Database Setup
Job postings will be stored in **Snowflake**, a cloud-based data warehouse that provides high scalability and performance for analytical workloads. Decided on Snowflake over a lower-cost, local solution like Postgres due to popularity. I wanted a cloud-based database used by many startups, and that could be integrated with other tooling like AWS and visualization software. 

### Snowflake Table Schema:
To facilitate better data modeling and transformations in **dbt**, I will use a multi-table structure:

#### `jobs` Table
```sql
CREATE TABLE jobs (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    job_title STRING,
    company_id INTEGER REFERENCES companies(id),
    location STRING,
    job_description STRING,
    employment_type STRING,
    posted_date DATE,
    salary STRING,
    job_url STRING UNIQUE,
    source STRING
);
```

#### `companies` Table
```sql
CREATE TABLE companies (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    company_name STRING UNIQUE,
    industry STRING,
    headquarters STRING,
    company_size STRING
);
```

By structuring the database with separate tables for **jobs** and **companies**, and by source (Built In, LinkedIn) I can use **dbt** to join and transform the data efficiently.

### Snowflake AWS Access
Role created for Snowflake to grant read permissions to S3 bucket.

### Secrets Manager
Secret created for Snowflake S3 access credentials.

## Next Steps
- [✅] **Create GitHub repository**
- [✅] **Set up Snowflake database**
- [✅] **Write the first web scraper for BuiltIn**
- [🔄] **Write the second web scraper for LinkedIn** 
- [✅] **Store scraped data in Snowflake**
- [✅] **Automate scraping and scheduling** 

## Future Enhancements
- [🔄] Implement **dbt models** for transforming job data
- [✅] **Create develop branch for PRs rather than pushing directly to main**
- [✅] **Use AWS Lambda to run queries**
- [✅] **Store data in AWS S3 bucket**
- [✅] **Load data back into snowflake (parquet?)**
          - Decided on json load to snowflake. If I were using more AWS services like Athena or Redshift I would convert to parquet with AWS Glue prior to Snowflake.
- [ ] **Feature to upload resume, have ChatGPT write Cover Letters tailored to the position automatically**
- [ ] Build **dashboards** to visualize trends (salary insights, hiring patterns)
- [ ] Automate **CI/CD checks** with GitHub Actions
- [ ] **SQLFluff code rules in CI**
- [ ] **Logging with Cloudwatch**

---
### **Contributions & Feedback**
This project is a learning experience in data engineering and analytics. Any suggestions or feedback are welcome!

### **Decision Making Notes**

**Lambda Packaging**
- When creating a zip to upload to AWS Lambda...Create package folder in root of project...Activate virtual environment...pip install -r requirements.txt --target package/...cp lambda_function.py package/

**Snowflake Connection to S3 Bucket**
- Created an S3 integration in snowflake
- Updated IAM role trust relationship for snowflake
- Created an external stage in snowflake for the S3 integration

**DBT Setup**
- Created python env dbt_env
- pip install dbt-snowflake
- dbt init dbt_models to setup dbt structure
- setup .dbt/profiles.yml
- Created new snowflake role for dbt
- dbt debug successful connection
- From cd job-scraping-python, run dbt_env (pip installed dbt in) by dbt_env/Scripts/Activate (deactivate to exit)
- Setup model structure in dbt_project.yml


Next Up:
- [✅] only keep prehook for raw_companies table..parsed shouldn't also go into raw, probably staging
- schedule dbt run?
- Redo model structure based on dbt best practices
- For raw models, is there a better way to read in S3 bucket than source lines and parsed CTEs?
- Eventually move model configs into dbt_project.yml for multiple sources (LinkedIn)

References
https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview