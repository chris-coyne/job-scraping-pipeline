import os
import requests
from bs4 import BeautifulSoup
import snowflake.connector
import datetime

# Load credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# Snowflake connection details
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse="COMPUTE_WH",
    database="job_scraping",
    schema="raw_data",
)

# Job roles to scrape
JOB_TITLES = ["data analyst", "analytics engineer"]

# Scraping jobs for multiple roles
def get_jobs():
    job_list = []
    
    for job_title in JOB_TITLES:
        search_query = job_title.replace(" ", "+")
        BASE_URL = f"https://builtin.com/jobs/remote?search={search_query}&daysSinceUpdated=1"

        print(f"🔎 Searching for {job_title} roles...")
        
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(BASE_URL, headers=headers)

        if response.status_code != 200:
            print(f"❌ Failed to fetch {job_title} jobs. Status Code: {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")

        # Find all job cards
        job_cards = soup.find_all("div", class_="rounded-3")

        if not job_cards:
            print(f"⚠️ No {job_title} job listings found.")
            continue

        for job in job_cards:
            title_elem = job.find("a", {"data-id": "job-card-title"})
            company_elem = job.find("a", {"data-id": "company-title"})
            description_elem = job.find("div", class_="fs-sm fw-regular mb-md text-gray-04")  # Extract brief description
            details_elems = job.find_all("span", class_="font-barlow text-gray-04")

            if title_elem and company_elem:
                title = title_elem.text.strip()
                company = company_elem.text.strip()
                job_url = "https://builtin.com" + title_elem["href"]

                # Extract brief job description from the card
                job_description = description_elem.text.strip() if description_elem else "No description provided"

                # Default values
                location = "Remote"
                salary = "Not Provided"
                level = "Unknown"
                date_added = datetime.datetime.now()  # Store current timestamp

                for elem in details_elems:
                    text = elem.text.strip()
                    if any(keyword in text for keyword in ["$", "K", "Annually", "Per Year"]):  
                        salary = text
                    elif "USA" in text or "," in text:  
                        location = text
                    elif any(keyword in text for keyword in ["level", "Junior", "Senior"]):
                        level = text

                job_list.append({
                    "searched_job_title": job_title,
                    "job_title": title,
                    "company_name": company,
                    "location": location,
                    "salary": salary,
                    "level": level,
                    "job_url": job_url,
                    "job_description": job_description,
                    "date_added": date_added,
                    "source": "BuiltIn"
                })
    
    return job_list


# Ensure company exists in the database
def get_or_create_company(cursor, company_name):
    """Retrieve company_id from Snowflake or insert the company if missing."""
    
    cursor.execute("SELECT id FROM companies WHERE company_name = %s", (company_name,))
    company = cursor.fetchone()

    if company:
        return company[0]  # Return existing company_id

    cursor.execute("INSERT INTO companies (company_name) VALUES (%s)", (company_name,))
    
    cursor.execute("SELECT id FROM companies WHERE company_name = %s", (company_name,))
    return cursor.fetchone()[0]


# Check if a job already exists before inserting
def job_exists(cursor, job_url):
    """Returns True if the job already exists in the database, otherwise False."""
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE job_url = %s", (job_url,))
    return cursor.fetchone()[0] > 0


# Writing records into Snowflake with duplicate prevention
def insert_into_snowflake(jobs):
    cursor = conn.cursor()
    
    for job in jobs:
        try:
            # Check if job already exists
            if job_exists(cursor, job["job_url"]):
                print(f"⚠️ Skipping duplicate job: {job['job_title']} at {job['company_name']}")
                continue  # Skip this job

            # Ensure company exists
            company_id = get_or_create_company(cursor, job["company_name"])

            # Insert job with job_description and date_added
            sql = """
            INSERT INTO jobs (searched_job_title, job_title, company_id, location, salary, job_url, job_description, date_added, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                job["searched_job_title"], 
                job["job_title"], 
                company_id, 
                job["location"], 
                job["salary"], 
                job["job_url"], 
                job["job_description"],
                job["date_added"],
                job["source"]
            ))
            print(f"✅ Inserted: {job['job_title']} at {job['company_name']}")

        except Exception as e:
            print(f"❌ Error inserting job {job['job_title']}: {e}")

    conn.commit()
    cursor.close()
    print("✅ All data inserted into Snowflake")


# Run the scraper
jobs = get_jobs()

# Run the import to Snowflake
insert_into_snowflake(jobs)
