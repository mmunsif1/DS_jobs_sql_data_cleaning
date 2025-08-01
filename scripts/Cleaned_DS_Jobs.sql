/* ====================================================================================================
   Script: Data Cleaning & Enrichment for Data Science Job Listings
   ----------------------------------------------------------------------------------
   Purpose:
     This query transforms raw job posting data from the `Uncleaned_DS_jobs` table into a cleaned and
     enriched dataset suitable for analysis, modeling, and reporting. It uses a Common Table Expression 
     (CTE) named `Cleaned_DS_Jobs` to apply several key preprocessing steps that normalize, classify, and 
     extract useful features from unstructured text and semi-structured fields.

   Key Transformations:
     1. Job Title Normalization:
        - Converts to lowercase, trims whitespace, and standardizes variations of "senior" and "junior".

     2. Location Type Detection:
        - Classifies job listings as `remote`, `hybrid`, `on_site`, or `other` based on keywords in 
          the job title and job description.

     3. Employment Type Classification:
        - Detects employment types such as full-time, part-time, contract, internship, freelance, 
          temporary, consultant, or other using keyword searches.

     4. Salary Parsing:
        - Extracts and cleans salary ranges from string values formatted like "$75K-$120K".
        - Generates `salary_range` (as a readable string), and numeric `min_salary` and `max_salary` fields
          by parsing and converting the salary components to integer values (without the 'K' suffix).

     5. Company Rating and Name Cleanup:
        - Replaces negative ratings with zero.
        - Cleans company names based on rating conditions, extracting the name suffix for display.

     6. Date and Age Processing:
        - Parses the `founded` year from valid values and calculates the `company_age`.

     7. Skill Detection (Binary Encoding):
        - Flags the presence (1) or absence (0) of key tech skills based on keyword matches in 
          the job description. Skills include: Python, Java, Scala, SQL, Tableau, PowerBI, Excel, 
          AWS, Azure, Databricks, Hadoop, Spark, Kafka, BigData, MongoDB, NoSQL, BigQuery, TensorFlow.

     8. Job Role Categorization:
        - Determines job seniority (`senior` or `n/a`) and classifies roles into specific categories 
          like data scientist, data engineer, BI analyst, etc., based on job title content.

   Output:
     - A cleaned dataset with standardized fields, binary skill indicators, structured salary info, 
       and categorized job roles, ready for downstream tasks like analysis or machine learning.

   Assumptions:
     - Salary values are consistently formatted using the "$" and "K" notation.
     - Keyword matching is sufficient for inferring job types and tech skills.
     
   Author:
     - Muhammad Munsif
   
   LinkedIn:
     - linkedin.com/in/muhammad-munsif/
     
   Tableau:
     - public.tableau.com/app/profile/muhammadmunsif
   ==================================================================================================== */



WITH Cleaned_DS_Jobs AS (
    SELECT

        -- Normalize job title
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(LOWER(TRIM(job_title)), '(sr.)', 'sr'),
                        'sr.', 'sr'
                    ),
                    'jr.', 'jr'
                ),
                'senior', 'sr'
            ),
            'junior', 'jr'
        ) AS job_title,

        -- Detect location type
        CASE
            WHEN LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%remote%' 
              OR LOWER(job_title) LIKE '%remote%' THEN 'remote'
            WHEN LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%hybrid%' 
              OR LOWER(job_title) LIKE '%hybrid%' THEN 'hybrid'
            WHEN LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%on_site%' 
              OR LOWER(job_title) LIKE '%on_site%' 
              OR LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%onsite%' 
              OR LOWER(job_title) LIKE '%onsite%' THEN 'onsite'
            ELSE 'other'
        END AS location_type,

        -- Employment type
        CASE
            WHEN LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%fulltime%' 
              OR LOWER(job_title) LIKE '%fulltime%' 
              OR LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%full_time%' 
              OR LOWER(job_title) LIKE '%full_time%' THEN 'fulltime'
            WHEN LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%parttime%' 
              OR LOWER(job_title) LIKE '%parttime%' 
              OR LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%part_time%' 
              OR LOWER(job_title) LIKE '%part_time%' THEN 'parttime'
            WHEN LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%contract%' 
              OR LOWER(job_title) LIKE '%contract%' THEN 'contract'
            WHEN LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%intern%' 
              OR LOWER(job_title) LIKE '%intern%' THEN 'internship'
            WHEN LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%freelancer%' 
              OR LOWER(job_title) LIKE '%freelancer%' 
              OR LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%freelance%' 
              OR LOWER(job_title) LIKE '%freelance%' THEN 'freelance'
            WHEN LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%temp%' 
              OR LOWER(job_title) LIKE '%temp%' THEN 'temporary'
            WHEN LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%consultant%' 
              OR LOWER(job_title) LIKE '%consultant%' 
              OR LOWER(CAST(job_description AS VARCHAR(MAX))) LIKE '%consulting%' 
              OR LOWER(job_title) LIKE '%consulting%' THEN 'consultant'
            ELSE 'other'
        END AS employment_type,

        -- Extract salary range without the 'K'
        SUBSTRING(salary_estimate, 2, CHARINDEX('K', salary_estimate) - 2) + '-' +
        SUBSTRING(
            salary_estimate,
            CHARINDEX('$', salary_estimate, 2) + 1,
            CHARINDEX('K', salary_estimate, CHARINDEX('$', salary_estimate, 2)) - 2
              - CHARINDEX('$', salary_estimate, 2) + 1
        ) AS salary_range,

        -- Extract min salary
        CAST(SUBSTRING(
            salary_estimate,
            CHARINDEX('$', salary_estimate) + 1,
            CHARINDEX('K', salary_estimate) - CHARINDEX('$', salary_estimate) - 1
        ) AS INT) * 1000 AS min_salary,

        -- Extract max salary
        CAST(SUBSTRING(
            salary_estimate,
            CHARINDEX('$', salary_estimate, CHARINDEX('K', salary_estimate)) + 1,
            CHARINDEX('K', salary_estimate, CHARINDEX('K', salary_estimate) + 1) - 
            CHARINDEX('$', salary_estimate, CHARINDEX('K', salary_estimate)) - 1
        ) AS INT) * 1000 AS max_salary,

        CAST(job_description AS VARCHAR(MAX)) AS job_description,

        -- Handle rating
        CASE
            WHEN rating < 0 THEN 0
            ELSE rating
        END AS rating,

        -- Extract cleaned company name
        CASE
            WHEN rating < 0 THEN company_name
            ELSE REVERSE(
                SUBSTRING(
                    REVERSE(company_name),
                    CHARINDEX(' ', REVERSE(company_name)),
                    LEN(company_name)
                )
            )
        END AS company_name,

        location,
        headquarters,
        size,

        -- Handle founded year
        YEAR(CAST(NULLIF(CAST(founded AS VARCHAR), '-1') AS DATE)) AS founded,
        DATEDIFF(
            YEAR,
            CAST(NULLIF(CAST(founded AS VARCHAR), '-1') AS DATE),
            GETDATE()
        ) AS company_age,

        type_of_ownership,
        industry,
        sector,
        revenue,
        competitors,

        -- Tech skills detection
        CASE WHEN job_description LIKE '%Python%' THEN 1 ELSE 0 END AS Python,
        CASE WHEN job_description LIKE '%Java%' THEN 1 ELSE 0 END AS Java,
        CASE WHEN job_description LIKE '%Scala%' THEN 1 ELSE 0 END AS Scala,
        CASE WHEN job_description LIKE '%SQL%' THEN 1 ELSE 0 END AS SQL,
        CASE WHEN job_description LIKE '%Tableau%' THEN 1 ELSE 0 END AS Tableau,
        CASE 
            WHEN job_description LIKE '%PowerBI%' OR job_description LIKE '%Power_BI%' THEN 1 
            ELSE 0 
        END AS PowerBI,
        CASE WHEN job_description LIKE '%Excel%' THEN 1 ELSE 0 END AS Excel,
        CASE WHEN job_description LIKE '%AWS%' THEN 1 ELSE 0 END AS AWS,
        CASE WHEN job_description LIKE '%Azure%' THEN 1 ELSE 0 END AS Azure,
        CASE 
            WHEN job_description LIKE '%Databricks%' OR job_description LIKE '%Data_bricks%' THEN 1 
            ELSE 0 
        END AS Databricks,
        CASE WHEN job_description LIKE '%Hadoop%' THEN 1 ELSE 0 END AS Hadoop,
        CASE WHEN job_description LIKE '%Spark%' THEN 1 ELSE 0 END AS Spark,
        CASE WHEN job_description LIKE '%Kafka%' THEN 1 ELSE 0 END AS Kafka,
        CASE 
            WHEN job_description LIKE '%BigData%' OR job_description LIKE '%Big_Data%' THEN 1 
            ELSE 0 
        END AS BigData,
        CASE 
            WHEN job_description LIKE '%Mongo_DB%' OR job_description LIKE '%MongoDB%' THEN 1 
            ELSE 0 
        END AS MongoDB,
        CASE 
            WHEN job_description LIKE '%No_SQL%' OR job_description LIKE '%NoSQL%' THEN 1 
            ELSE 0 
        END AS NoSQL,
        CASE 
            WHEN job_description LIKE '%Big_Query%' OR job_description LIKE '%BigQuery%' THEN 1 
            ELSE 0 
        END AS BigQuery,
        CASE 
            WHEN job_description LIKE '%Tensor_Flow%' OR job_description LIKE '%TensorFlow%' THEN 1 
            ELSE 0 
        END AS TensorFlow,

        -- Seniority classification
        CASE
            WHEN job_title LIKE '%sr%' OR job_title LIKE '%senior%' THEN 'senior'
            ELSE 'n/a'
        END AS seniority,

        -- Job category classification
        CASE
            WHEN job_title LIKE '%data%scientist%' THEN 'data scientist'
            WHEN job_title LIKE '%data%engineer%' THEN 'data engineer'
            WHEN job_title LIKE '%machine%learning%engineer%' THEN 'machine learning engineer'
            WHEN job_title LIKE '%machine%learning%scientist%' THEN 'machine learning scientist'
            WHEN job_title LIKE '%business%intelligence%analyst%' THEN 'BI analyst'
            WHEN job_title LIKE '%data%analyst%' THEN 'data analyst'
            WHEN job_title LIKE '%data%modeler%' THEN 'data modeler'
            WHEN job_title LIKE '%software%engineer%' THEN 'software engineer'
            WHEN job_title LIKE '%director%' THEN 'director'
            WHEN job_title LIKE '%data%science%manager%' THEN 'data science manager'
            WHEN job_title LIKE '%manager%' THEN 'manager'
            ELSE 'other'
        END AS job_category

    FROM Uncleaned_DS_jobs
)

SELECT *
FROM Cleaned_DS_Jobs;
