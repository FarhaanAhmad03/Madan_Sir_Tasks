"""
Script Name: Incremental Load
Objective:To implement a simple incremental load
Created By : Farhaan , Kaviya , Krithika
Created Date: 29/12/25
# =====================================================
# Script Name: Incremental Load
# Objective:To implement a simple incremental load
# Created By : Farhaan , Kaviya , Krithika
# Created Date: 29/12/25
# Modified Date : 29/12/25
# =====================================================
"""


"""
INCREMENTAL DATA LOAD USING PYTHON + MYSQL
 
PSEUDOCODE:
1. Truncate staging table
2. Extract new and updated records from source into staging
3. Update existing records in target
4. Insert new records into target
5. Update control table with latest processed dates
6. Log every step
"""

import mysql.connector
import os
import logging
from datetime import datetime
 
# -------- CREATE LOG DIRECTORY --------
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
 
# -------- CREATE UNIQUE LOG FILE --------
run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(LOG_DIR, f"incremental_load_{run_timestamp}.log")
 
# -------- LOGGING CONFIG --------
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
 
# -------- JOB START TIME --------
job_start_time = datetime.now()
logging.info(f"Farhaan_incrementalload_imp.py Started at {job_start_time}")
 
 
# ---------------- DB CONNECTION ----------------
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="task"
    )
    cursor = conn.cursor()
    logging.info("Connected to MySQL successfully")
except Exception as e:
    logging.error(f"Database connection failed: {e}")
    raise
 
# ---------------- STEP 1: TRUNCATE STAGE ----------------
logging.info("STEP 1: Started Truncating Stage")
cursor.execute("TRUNCATE TABLE stg_dm")
conn.commit()
 
logging.info("Stage table truncated")
 
# ---------------- STEP 2: LOAD DELTA INTO STAGE ----------------
logging.info("STEP 2: STARTED LOAD DELTA INTO STAGE")
 
stage_insert_sql = """
INSERT INTO stg_dm (cust_id, customer_name, customer_phone, created_date, modified_date)
SELECT cust_id, customer_name, customer_phone, created_date, modified_date
FROM src_dm
WHERE created_date > (
    SELECT MAX(max_created_date) FROM ctrl_dm WHERE table_id='SRC_DM'
)
OR modified_date > (
    SELECT MAX(max_modified_date) FROM ctrl_dm WHERE table_id='SRC_DM'
)
"""
cursor.execute(stage_insert_sql)
conn.commit()
logging.info("Delta records loaded into stage")
 
# ---------------- STEP 3: UPDATE TARGET ----------------
logging.info("STEP 3:STARTED UPDATE TARGET")
 
 
update_target_sql = """
UPDATE trgt_dm t
JOIN stg_dm s ON t.cust_id = s.cust_id
SET
    t.customer_name = s.customer_name,
    t.customer_phone = s.customer_phone,
    t.created_date = s.created_date,
    t.modified_date = s.modified_date
WHERE s.modified_date > t.modified_date
"""
cursor.execute(update_target_sql)
conn.commit()
logging.info("Target table updated with modified records")
 
# ---------------- STEP 4: INSERT NEW RECORDS ----------------
logging.info("STEP 4: STARTED INSERT NEW RECORDS")
 
insert_target_sql = """
INSERT INTO trgt_dm (cust_id, customer_name, customer_phone, created_date, modified_date)
SELECT s.cust_id, s.customer_name, s.customer_phone, s.created_date, s.modified_date
FROM stg_dm s
LEFT JOIN trgt_dm t ON s.cust_id = t.cust_id
WHERE t.cust_id IS NULL
"""
cursor.execute(insert_target_sql)
conn.commit()
logging.info("New records inserted into target")
 
# ---------------- STEP 5: UPDATE CONTROL TABLE ----------------
logging.info("STEP 5 : STARTED UPDATE CONTROL TABLE")
update_ctrl_sql = """
UPDATE ctrl_dm
SET
    max_created_date = (SELECT MAX(created_date) FROM stg_dm),
    max_modified_date = (SELECT MAX(modified_date) FROM stg_dm)
WHERE table_id = 'SRC_DM'
"""
cursor.execute(update_ctrl_sql)
conn.commit()
logging.info("Control table updated")
 
# ---------------- CLOSE CONNECTION ----------------
cursor.close()
conn.close()
 
 
 
# -------- JOB END TIME --------
job_end_time = datetime.now()
execution_time = (job_end_time - job_start_time).total_seconds()
 
logging.info("Incremental Load Job Completed Successfully")
logging.info(f"Farhaan_incrementalload_imp.py ended at : {job_end_time}")
logging.info(f"Total Run Time (seconds): {execution_time}")
 