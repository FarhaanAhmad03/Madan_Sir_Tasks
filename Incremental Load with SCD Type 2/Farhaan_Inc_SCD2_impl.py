"""
# =====================================================
# Script Name: Farhaan_Inc_SCD2_impl
# Objective:To implement a incremental Load with SCD2
# Created By : Farhaan 
# Created Date: 30/12/25
# Modified Date : 30/12/25
# =====================================================
"""

"""
Pseudocode

===========================================================
FARHAAN INCREMENTAL LOAD WITH SCD TYPE 2 (MYSQL + PYTHON)
===========================================================

FLOW OF EXECUTION:

1. Job starts
   - Logger initialized
   - Log file created with timestamp

2. Database connection established
   - Connect to MySQL using config values
   - Cursor created

3. Control table validation
   - Ensure entry exists for source table
   - Read last processed created & modified timestamps

4. Staging table preparation
   - Truncate staging table
   - Ensure clean state for current run

5. Delta extraction
   - Fetch new records where created_date > control table
   - Fetch updated records where modified_date > control table
   - Load only delta records into staging

6. SCD Type 2 processing – Updates
   - Identify changed records
   - Expire existing dimension records
   - Set is_current = 0
   - Set effective_end_dt = modified_date

7. SCD Type 2 processing – Inserts
   - Insert new version of updated records
   - Insert completely new customers
   - Set is_current = 1
   - Set effective_end_dt = 9999-12-31

8. Control table update
   - Update max_created_date
   - Update max_modified_date
   - Mark successful processing of this batch

9. Cleanup
   - Commit transactions
   - Close cursor and connection

10. Job ends
    - Log successful completion
    - Print status to console

FAILURE HANDLING:
- Any failure triggers rollback
- Error logged to file
- Error printed to console
- Script exits safely

===========================================================
"""


import mysql.connector
import sys
from config import DB_CONFIG, TABLE_ID, setup_logger

# =========================
# INITIALIZE LOGGER
# =========================
logger = setup_logger("inc_scd2_impl")

logger.info("Farhaan inc_scd2_impl started")
print("Farhaan inc_scd2_impl started")

# =========================
# DB CONNECTION
# =========================
try:
    logger.info("STEP 0: Database connection started")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    logger.info("STEP 0: Database connection successful")
except Exception as e:
    print(f"DB connection failed: {e}")
    sys.exit(1)

try:
    # =========================
    # STEP 1: CONTROL TABLE CHECK
    # =========================
    logger.info("STEP 1: Control table validation started")
    cursor.execute(
        "SELECT COUNT(*) FROM ctrl_customer WHERE table_id = %s",
        (TABLE_ID,)
    )
    if cursor.fetchone()[0] == 0:
        raise Exception("Control table not initialized")
    logger.info("STEP 1: Control table validation successful")

    # =========================
    # STEP 2: TRUNCATE STAGING
    # =========================
    logger.info("STEP 2: Truncate staging started")
    cursor.execute("TRUNCATE TABLE stg_customer")
    conn.commit()
    logger.info("STEP 2: Truncate staging completed successfully")

    # =========================
    # STEP 3: LOAD DELTA
    # =========================
    logger.info("STEP 3: Delta load into staging started")
    cursor.execute("""
        INSERT INTO stg_customer
        SELECT *
        FROM src_customer
        WHERE created_date > (
            SELECT max_created_date FROM ctrl_customer WHERE table_id = %s
        )
        OR modified_date > (
            SELECT max_modified_date FROM ctrl_customer WHERE table_id = %s
        )
    """, (TABLE_ID, TABLE_ID))
    conn.commit()

    delta_count = cursor.rowcount
    logger.info(f"STEP 3: Delta load completed successfully | Rows = {delta_count}")

    if delta_count == 0:
        logger.info("No delta records found. Job exiting safely.")
        print("No delta records found. Job exiting.")
        sys.exit(0)

    # =========================
    # STEP 4: EXPIRE OLD RECORDS
    # =========================
    logger.info("STEP 4: Expiring old dimension records started")
    cursor.execute("""
        UPDATE dim_customer d
        JOIN stg_customer s
        ON d.cust_id = s.cust_id
        SET
            d.effective_end_dt = s.modified_date,
            d.is_current = 0
        WHERE d.is_current = 1
          AND (
              d.customer_name <> s.customer_name
           OR d.customer_phone <> s.customer_phone
          )
    """)
    conn.commit()
    logger.info(f"STEP 4: Expire completed | Rows affected = {cursor.rowcount}")

    # =========================
    # STEP 5: INSERT NEW VERSION
    # =========================
    logger.info("STEP 5: Insert new SCD2 versions started")
    cursor.execute("""
        INSERT INTO dim_customer (
            cust_id,
            customer_name,
            customer_phone,
            effective_start_dt,
            effective_end_dt,
            is_current
        )
        SELECT
            s.cust_id,
            s.customer_name,
            s.customer_phone,
            s.modified_date,
            '9999-12-31',
            1
        FROM stg_customer s
        JOIN dim_customer d
        ON s.cust_id = d.cust_id
        WHERE d.is_current = 0
          AND d.effective_end_dt = s.modified_date
    """)
    conn.commit()
    logger.info(f"STEP 5: Insert new versions completed | Rows inserted = {cursor.rowcount}")

    # =========================
    # STEP 6: INSERT NEW CUSTOMERS
    # =========================
    logger.info("STEP 6: Insert new customers started")
    cursor.execute("""
        INSERT INTO dim_customer (
            cust_id,
            customer_name,
            customer_phone,
            effective_start_dt,
            effective_end_dt,
            is_current
        )
        SELECT
            s.cust_id,
            s.customer_name,
            s.customer_phone,
            s.created_date,
            '9999-12-31',
            1
        FROM stg_customer s
        LEFT JOIN dim_customer d
        ON s.cust_id = d.cust_id
        WHERE d.cust_id IS NULL
    """)
    conn.commit()
    logger.info(f"STEP 6: Insert new customers completed | Rows inserted = {cursor.rowcount}")

    # =========================
    # STEP 7: UPDATE CONTROL TABLE
    # =========================
    logger.info("STEP 7: Control table update started")
    cursor.execute("""
        UPDATE ctrl_customer
        SET
            max_created_date  = (SELECT MAX(created_date) FROM stg_customer),
            max_modified_date = (SELECT MAX(modified_date) FROM stg_customer)
        WHERE table_id = %s
    """, (TABLE_ID,))
    conn.commit()
    logger.info("STEP 7: Control table updated successfully")

except Exception as e:
    conn.rollback()
    error_msg = f"Farhaan inc_scd2_impl FAILED: {e}"
    logger.error(error_msg)
    print(error_msg)
    sys.exit(1)

finally:
    cursor.close()
    conn.close()
    logger.info("Farhaan inc_scd2_impl completed")
    print("Farhaan inc_scd2_impl completed")


