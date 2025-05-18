-- Set role, warehouse, database, schema once at start
USE ROLE SYSADMIN;
USE WAREHOUSE TUTIFY_WH;
USE DATABASE TUTIFY_DB;
USE SCHEMA TUTIFY_SCHEMA;

-- Create stage if not exists
CREATE OR REPLACE STAGE my_stage;

-- ===========================
-- Create dimension tables
-- ===========================

CREATE OR REPLACE TABLE DIM_STUDENT (
  student_id INT,
  full_name STRING,
  father_name STRING,
  board STRING,
  grade_level STRING,
  school_type STRING,
  school_name STRING,
  country STRING,
  city STRING,
  birth_date DATE,
  status STRING
);
CREATE OR REPLACE TABLE DIM_TEACHER (
  teacher_id INT,
  full_name STRING,
  email STRING,
  phone STRING,
  university STRING,
  qualification_level STRING,
  birth_date DATE,
  birth_city STRING,
  current_city STRING,
  languages STRING,
  status STRING
);

COPY INTO DIM_TEACHER
FROM @my_stage/DIM_TEACHER.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


CREATE OR REPLACE TABLE DIM_DATE (
  date_id INT,
  full_date DATE,
  day INT,
  month INT,
  year INT,
  day_name STRING,
  month_name STRING,
  quarter STRING
);

CREATE OR REPLACE TABLE DIM_SUBJECT (
  subject_id INT,
  subject_name STRING,
  level STRING,
  board_id INT,
  standard_price FLOAT
);

CREATE OR REPLACE TABLE DIM_TEACHER_SUBJECT_LEVEL (
    id INT,
    teacher_id INT,
    subject_id INT,
    level STRING,           -- IGCSE, AS, A2
    rate_per_hour FLOAT,
    PRIMARY KEY (id)
);

COPY INTO DIM_TEACHER_SUBJECT_LEVEL
FROM @my_stage/DIM_TEACHER_SUBJECT_LEVEL.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);



CREATE OR REPLACE TABLE DIM_PAYMENT_METHOD (
  payment_method_id INT,
  payment_channel STRING,
  currency STRING,
  is_online BOOLEAN,
  gateway_used STRING
);

-- ===========================
-- Load dimensions from stage
-- ===========================

COPY INTO DIM_STUDENT
FROM @my_stage/DIM_STUDENT.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO DIM_DATE
FROM @my_stage/DIM_DATE.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO DIM_SUBJECT
FROM @my_stage/DIM_SUBJECT.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO DIM_PAYMENT_METHOD
FROM @my_stage/DIM_PAYMENT_METHOD.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- ===========================
-- Create staging tables for facts
-- ===========================

CREATE OR REPLACE TABLE STG_STUDENT_PAYMENT (
  payment_id INT,
  student_id INT,
  subject_id INT,
  date_id INT,
  currency STRING,
  amount_original FLOAT,
  exchange_rate FLOAT,
  amount_converted_pkr FLOAT,
  payment_method_id INT,
  invoice_sent BOOLEAN
);

CREATE OR REPLACE TABLE STG_TEACHER_PAYOUT (
  payout_id INT,
  teacher_id INT,
  date_id INT,
  hours_taught FLOAT,
  rate_per_hour FLOAT,
  rate_basis STRING,
  total_payout FLOAT,
  currency STRING,
  bonus FLOAT
);

show tables;

-- ===========================
-- Load staging tables from stage
-- ===========================

COPY INTO STG_STUDENT_PAYMENT
FROM @my_stage/FACT_STUDENT_PAYMENT_FINAL_30K_CLEAN.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO STG_TEACHER_PAYOUT
FROM @my_stage/FACT_TEACHER_PAYOUT_FINAL_NO_TEXT_CLEAN.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- ===========================
-- Drop old fact tables if exist
-- ===========================

DROP TABLE IF EXISTS FACT_STUDENT_PAYMENT;
DROP TABLE IF EXISTS FACT_TEACHER_PAYOUT;

-- ===========================
-- Create fact tables from staging
-- ===========================

CREATE OR REPLACE TABLE FACT_STUDENT_PAYMENT AS
SELECT
  payment_id,
  student_id,
  subject_id,
  date_id,
  currency,
  amount_original,
  exchange_rate,
  amount_converted_pkr,
  payment_method_id,
  invoice_sent
FROM STG_STUDENT_PAYMENT;

CREATE OR REPLACE TABLE FACT_TEACHER_PAYOUT AS
SELECT
  payout_id,
  teacher_id,
  date_id,
  hours_taught,
  rate_per_hour,
  rate_basis,
  total_payout,
  currency,
  bonus
FROM STG_TEACHER_PAYOUT;

-- ===========================
-- Sample validation queries
-- ===========================

SELECT 
  f.amount_original,
  s.full_name AS student_name,
  d.month_name AS payment_month,
  pm.payment_channel,
  sub.subject_name
FROM FACT_STUDENT_PAYMENT f
JOIN DIM_STUDENT s ON f.student_id = s.student_id
JOIN DIM_DATE d ON f.date_id = d.date_id
JOIN DIM_PAYMENT_METHOD pm ON f.payment_method_id = pm.payment_method_id
JOIN DIM_SUBJECT sub ON f.subject_id = sub.subject_id
LIMIT 20;

SELECT
  t.full_name,
  SUM(f.total_payout) AS total_earned
FROM FACT_TEACHER_PAYOUT f
JOIN DIM_TEACHER t ON f.teacher_id = t.teacher_id
GROUP BY t.full_name
ORDER BY total_earned DESC
LIMIT 10;


-- Aggregate monthly student revenue and teacher payouts for financial overview
SELECT 
  d.month_name AS month,
  SUM(fsp.amount_converted_pkr) AS total_revenue_pkr,
  SUM(ftp.total_payout) AS total_teacher_payout_pkr
FROM FACT_STUDENT_PAYMENT fsp
JOIN DIM_DATE d ON fsp.date_id = d.date_id
JOIN FACT_TEACHER_PAYOUT ftp ON ftp.date_id = d.date_id
GROUP BY d.month_name, d.month
ORDER BY d.month;  -- or ORDER BY month


-- Total payout amount distributed to teachers in each current city
SELECT 
  dt.current_city,
  SUM(ftp.total_payout) AS total_payout_pkr
FROM FACT_TEACHER_PAYOUT ftp
JOIN DIM_TEACHER dt ON ftp.teacher_id = dt.teacher_id
GROUP BY dt.current_city
ORDER BY total_payout_pkr DESC
LIMIT 10;


-- Subjects generating the most payment revenue from students
SELECT 
  ds.subject_name,
  SUM(fsp.amount_converted_pkr) AS revenue_pkr
FROM FACT_STUDENT_PAYMENT fsp
JOIN DIM_SUBJECT ds ON fsp.subject_id = ds.subject_id
GROUP BY ds.subject_name
ORDER BY revenue_pkr DESC
LIMIT 10;


-- Analyze revenue generated by students in different grade levels
SELECT 
  ds.grade_level,
  SUM(fsp.amount_converted_pkr) AS revenue_pkr
FROM FACT_STUDENT_PAYMENT fsp
JOIN DIM_STUDENT ds ON fsp.student_id = ds.student_id
GROUP BY ds.grade_level
ORDER BY revenue_pkr DESC;



-- Identify highest paying students by total payment amount
SELECT 
  ds.full_name,
  SUM(fsp.amount_converted_pkr) AS total_paid_pkr
FROM FACT_STUDENT_PAYMENT fsp
JOIN DIM_STUDENT ds ON fsp.student_id = ds.student_id
GROUP BY ds.full_name
ORDER BY total_paid_pkr DESC
LIMIT 10;

-- Show total payout grouped by teacher qualification level
SELECT 
  dt.qualification_level,
  SUM(ftp.total_payout) AS total_payout_pkr
FROM FACT_TEACHER_PAYOUT ftp
JOIN DIM_TEACHER dt ON ftp.teacher_id = dt.teacher_id
GROUP BY dt.qualification_level
ORDER BY total_payout_pkr DESC;




-- Calculate average hourly payout rate per subject
SELECT 
  ds.subject_name,
  AVG(ftp.rate_per_hour) AS avg_hourly_rate
FROM FACT_TEACHER_PAYOUT ftp
JOIN DIM_TEACHER_SUBJECT_LEVEL tsl ON ftp.teacher_id = tsl.teacher_id
JOIN DIM_SUBJECT ds ON tsl.subject_id = ds.subject_id
GROUP BY ds.subject_name
ORDER BY avg_hourly_rate DESC;



list @my_stage;



