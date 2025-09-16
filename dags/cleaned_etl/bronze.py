# ETL_pipeline.ipynb
# Medallion Architecture ETL (Bronze → Silver → Gold)
# Clean, documented, with logging and CSV output

import pandas as pd
import logging
import os

# ------------------------------
# Setup
# ------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------------------
# BRONZE: Load Raw Data
# ------------------------------
def load_bronze_data():
    """
    Load raw data into a pandas DataFrame and save as bronze.csv.
    """
    logging.info("Loading raw data (Bronze)")
    raw_data = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Evan"],
        "age": [25, 30, None, 22, 35],
        "country": ["US", "UK", "US", "UK", None]
    })
    bronze_path = os.path.join(DATA_DIR, "bronze.csv")
    raw_data.to_csv(bronze_path, index=False)
    logging.info("Bronze data saved to %s", bronze_path)

