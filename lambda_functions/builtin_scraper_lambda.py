import os
import json
import requests
from bs4 import BeautifulSoup
import boto3
import datetime

# AWS S3 Setup
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")  # Set in AWS Lambda environment variables
S3_SUBFOLDER = "job_postings/builtin/"  # Job postings folder
COMPANY_FILE_PATH = "reference/company_table.json"  # Company reference file
s3_client = boto3.client("s3")

# Job roles to scrape
JOB_TITLES = ["data analyst", "analytics engineer"]

### **Step 1: Load or Create the Company Table**
def load_company_table():
    """Loads the company table from S3 or creates an empty one if missing."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=COMPANY_FILE_PATH)
        return json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        print("ℹ️ No existing company table found. Creating a new one.")
        return {}  # Return an empty dictionary if the file doesn't exist

def save_company_table(company_table):
    """Saves the updated company table back to S3."""
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=COMPANY_FILE_PATH,
        Body=json.dumps(company_table, indent=2),
        ContentType="application/json"
    )

def get_or_create_company(company_name, company_table):
    """Checks if a company exists in S3's company table, adds it if missing."""
    if company_name not in company_table:
        company_table[company_name] = {"id": len(company_table) + 1}  # Assign a unique ID
        print(f"🆕 Added new company: {company_name}")
    return company_table[company_name]["id"]  # Return company ID

### **Step 2: Modify Job Scraping to Reference Company IDs**
def get_jobs():
    """Scrapes job postings from BuiltIn."""
    job_list = []
    company_table = load_company_table()  # Load the current company reference table

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
        job_cards = soup.find_all("div", class_="rounded-3")

        if not job_cards:
            print(f"⚠️ No {job_title} job listings found.")
            continue

        for job in job_cards:
            title_elem = job.find("a", {"data-id": "job-card-title"})
            company_elem = job.find("a", {"data-id": "company-title"})
            description_elem = job.find("div", class_="fs-sm fw-regular mb-md text-gray-04")  
            details_elems = job.find_all("span", class_="font-barlow text-gray-04")

            if title_elem and company_elem:
                title = title_elem.text.strip()
                company = company_elem.text.strip()
                job_url = "https://builtin.com" + title_elem["href"]
                job_description = description_elem.text.strip() if description_elem else "No description provided"

                location = "Remote"
                salary = "Not Provided"
                level = "Unknown"
                date_added = datetime.datetime.utcnow().isoformat()

                for elem in details_elems:
                    text = elem.text.strip()
                    if any(keyword in text for keyword in ["$", "K", "Annually", "Per Year"]):  
                        salary = text
                    elif "USA" in text or "," in text:  
                        location = text
                    elif any(keyword in text for keyword in ["level", "Junior", "Senior"]):
                        level = text

                # Get or create company ID
                company_id = get_or_create_company(company, company_table)

                job_list.append({
                    "searched_job_title": job_title,
                    "job_title": title,
                    "company_id": company_id,  # Store company ID instead of name
                    "location": location,
                    "salary": salary,
                    "level": level,
                    "job_url": job_url,
                    "job_description": job_description,
                    "date_added": date_added,
                    "source": "BuiltIn"
                })

    # Save the updated company table back to S3
    save_company_table(company_table)
    
    return job_list

def get_last_14_files():
    """Retrieves the latest 14 job posting files from S3 (sorted by timestamp)."""
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_SUBFOLDER, Delimiter="/")

    if "Contents" not in response:
        return []

    # Get all files that match the expected job postings format
    all_files = [obj["Key"] for obj in response["Contents"] if S3_SUBFOLDER + "job_postings_" in obj["Key"]]
    
    # Sort files by most recent timestamp (descending order)
    all_files.sort(reverse=True)

    return all_files[:14]  # Keep only the latest 14 runs


### **Step 3: Keep Only the Last 14 Job Postings**
def delete_old_files(latest_files):
    """Deletes any job posting files older than the last 14 runs."""
    all_files = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_SUBFOLDER).get("Contents", [])
    all_keys = [obj["Key"] for obj in all_files if S3_SUBFOLDER + "job_postings_" in obj["Key"]]

    files_to_delete = [key for key in all_keys if key not in latest_files]

    if files_to_delete:
        delete_objects = [{"Key": key} for key in files_to_delete]
        s3_client.delete_objects(Bucket=S3_BUCKET_NAME, Delete={"Objects": delete_objects})
        print(f"🗑 Deleted {len(files_to_delete)} old job postings files.")

def aggregate_latest_jobs(latest_files):
    """Combines job postings from the last 14 runs and removes duplicates."""
    all_jobs = {}
    
    for file_key in latest_files:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        job_data = json.loads(response["Body"].read().decode("utf-8"))

        for job in job_data:
            all_jobs[job["job_url"]] = job  # Deduplicate by job URL

    return list(all_jobs.values())  # Return deduplicated job postings


### **Step 4: Upload Jobs to S3**
def upload_to_s3(jobs):
    """Uploads new job postings to S3 and updates latest_jobs.json."""
    if not jobs:
        print("✅ No new job postings found. Skipping upload.")
        return {"status": "no_new_data"}

    # Save with timestamp for versioning
    timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f"{S3_SUBFOLDER}job_postings_{timestamp}.json"

    # Upload new dataset
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(jobs, indent=2),
        ContentType="application/json"
    )
    print(f"✅ Uploaded job postings to {file_name}")

    # Retrieve latest 14 files and aggregate jobs
    latest_files = get_last_14_files()
    aggregated_jobs = aggregate_latest_jobs(latest_files)

    # Upload the deduplicated latest_jobs.json
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=f"{S3_SUBFOLDER}latest_jobs.json",
        Body=json.dumps(aggregated_jobs, indent=2),
        ContentType="application/json"
    )
    print("✅ Updated latest_jobs.json with deduplicated data from last 14 runs")

    # Cleanup old files (keep only latest 14)
    delete_old_files(latest_files)

    return {"status": "success", "s3_path": f"s3://{S3_BUCKET_NAME}/{file_name}"}

### **Step 5: Lambda Handler**
def lambda_handler(event, context):
    """AWS Lambda handler function."""
    jobs = get_jobs()
    if not jobs:
        return {"status": "no_jobs_found"}
    
    return upload_to_s3(jobs)
