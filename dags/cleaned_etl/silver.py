import pandas as pd
import logging
import os
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
# ------------------------------
# SILVER: Clean Data
# ------------------------------
def clean_silver_data() :
    """
    Clean raw data:
        - Drop rows with missing age or country
        - Convert names to uppercase
    Save cleaned data as silver.csv.
    """
    logging.info("Cleaning data (Silver)")
    silver_data = pd.read_csv( os.path.join(DATA_DIR, "bronze.csv"))
    silver_data = silver_data.dropna(subset=["age", "country"])
    silver_data["name"] = silver_data["name"].str.upper()

    silver_path = os.path.join(DATA_DIR, "silver.csv")
    silver_data.to_csv(silver_path, index=False)
    logging.info("Silver data saved to %s", silver_path)
    return silver_data


