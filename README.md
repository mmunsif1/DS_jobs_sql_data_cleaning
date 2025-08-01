# 📊 SQL Data Cleaning Project: Data Science Job Listings

## 🧾 Overview

This project focuses on cleaning and enriching a raw dataset of data science job listings stored in a SQL table named `Uncleaned_DS_jobs`. Using a single SQL Common Table Expression (CTE), the script standardizes job titles, extracts salary ranges, classifies roles, detects tech skills, and parses company metadata—transforming the messy input into a structured dataset ready for analysis or machine learning.

---

## ⚙️ Cleaning Logic & Features

The SQL script uses a `Cleaned_DS_Jobs` CTE to perform the following transformations:

### 1. 🧹 Job Title Normalization
- Converts job titles to lowercase.
- Removes redundant formats (e.g., `Sr.`, `Senior`, `(sr.)`) and standardizes to `sr` or `jr`.

### 2. 🗺️ Location Type Detection
- Classifies job listings into:
  - `remote`
  - `hybrid`
  - `on_site`
  - `other`
- Uses keywords found in both `job_title` and `job_description`.

### 3. 💼 Employment Type Classification
- Identifies employment types based on keywords:
  - `full_time`, `part_time`, `contract`, `internship`, `freelance`, `temporary`, `consultant`

### 4. 💲 Salary Range Extraction
- Handles salary strings like `$75K-$120K (Glassdoor est.)`
- Extracts:
  - `salary_range`: Clean string like `75-120`
  - `min_salary` and `max_salary`: Integers (e.g., 75000 and 120000)

### 5. 🏢 Company Information Cleanup
- Replaces invalid company ratings (e.g., -1) with 0.
- Extracts company names by removing the rating from the name if present.

### 6. 🕰️ Company Age Calculation
- Parses `founded` year.
- Calculates `company_age = current_year - founded`

### 7. 🔍 Skill Detection (Binary Flags)
- Creates binary columns for tech skills based on the presence of keywords in `job_description`:
  - Python, Java, Scala, SQL, Tableau, PowerBI, Excel
  - AWS, Azure, Databricks, Hadoop, Spark, Kafka
  - MongoDB, NoSQL, BigQuery, TensorFlow, BigData

### 8. 🧠 Role Categorization
- Uses job titles to:
  - Detect seniority (`senior` or `n/a`)
  - Categorize role: `data scientist`, `data analyst`, `data engineer`, `BI analyst`, etc.

---

## 🧪 Output

The output of the CTE (`Clean_DS_Jobs`) contains:

- Cleaned job titles and descriptions
- Standardized salary information
- Employment type and location classifications
- Company ratings and derived ages
- Binary indicators for technical skills
- Role categorization and seniority flags

---

## 🛠️ DBMS

- Microsoft SQL Server
  
---

## 📌 How to Use

1. Load the raw data into your SQL environment under the `Uncleaned_DS_jobs` table.
2. Run the `cleaned_ds_jobs.sql` script.
3. Query from the resulting `Cleaned_DS_Jobs` CTE or materialize it as a new table/view if needed:
   ```sql
   SELECT * FROM Cleaned_DS_Jobs;
