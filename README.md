# Job Scraping & Analytics Pipeline

## Overview
This project is focused on scraping, storing, and analyzing job postings for remote **Data Analyst, Senior Data Analyst, Analytics Engineer, and Senior Analytics Engineer** roles. The goal is to build a complete data pipeline that extracts job postings from **LinkedIn** and **BuiltIn**, processes the data, and visualizes trends.

## Tech Stack
- **Python** (Scrapy, BeautifulSoup, Selenium/Playwright for web scraping)
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

## Database Setup
Job postings will be stored in **Snowflake**, a cloud-based data warehouse that provides high scalability and performance for analytical workloads.

### Snowflake Table Schema:
To facilitate better data modeling and transformations in **dbt**, we will use a multi-table structure:

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

By structuring the database with separate tables for **jobs** and **companies**, we can use **dbt** to join and transform the data efficiently.

## Next Steps
- [ ] **Create GitHub repository** ✅
- [ ] **Set up Snowflake database** ✅
- [ ] **Write the first web scraper for BuiltIn** ✅
- [ ] **Write the second web scraper for LinkedIn** 🔄
- [ ] **Store scraped data in Snowflake** ✅
- [ ] **Automate scraping and scheduling** 🔄

## Future Enhancements
- Implement **dbt models** for transforming job data
- Create develop branch for PRs rather than pushing directly to main ✅
- Use AWS Lambda to run queries
- Store data in AWS S3 bucket
- Load data back into snowflake (parquet?)
- Feature to upload resume, have ChatGPT write Cover Letters tailored to the position automatically
- Build **dashboards** to visualize trends (salary insights, hiring patterns)
- Automate **CI/CD checks** with GitHub Actions
- SQLFluff code rules in CI

---
### **Contributions & Feedback**
This project is a learning experience in data engineering and analytics. Any suggestions or feedback are welcome!

