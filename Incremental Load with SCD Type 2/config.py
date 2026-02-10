# config.py

import logging
import os

# =========================
# DATABASE CONFIG
# =========================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "1234",
    "database": "scd2andincload"
}

TABLE_ID = "SRC_CUSTOMER"
LOG_DIR = "logs"

# =========================
# LOGGING CONFIG
# =========================
def setup_logger(job_name: str):
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    log_file = os.path.join(LOG_DIR, f"{job_name}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(job_name)

