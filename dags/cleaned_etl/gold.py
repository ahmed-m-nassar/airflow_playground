import pandas as pd
import logging
import os
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------------------
# Setup
# ------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
# ------------------------------
# GOLD: Aggregate Data
# ------------------------------
def aggregate_gold_data() :
    """
    Aggregate cleaned data by country to compute average age.
    Save aggregated data as gold.csv.
    """
    gold_data = pd.read_csv( os.path.join(DATA_DIR, "bronze.csv"))

    logging.info("Aggregating data (Gold)")
    gold_data = gold_data.groupby("country").agg({"age": "mean"}).reset_index()
    gold_data.rename(columns={"age": "avg_age"}, inplace=True)

    gold_path = os.path.join(DATA_DIR, "gold.csv")
    gold_data.to_csv(gold_path, index=False)
    logging.info("Gold data saved to %s", gold_path)


