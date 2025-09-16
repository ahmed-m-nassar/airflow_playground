from bronze import load_bronze_data
from silver import clean_silver_data
from gold import aggregate_gold_data
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------------------
# MAIN: Run ETL
# ------------------------------
if __name__ == "__main__":
    """Run the full Bronze → Silver → Gold ETL pipeline"""
    load_bronze_data()
    clean_silver_data()
    aggregate_gold_data()
    logging.info("ETL pipeline completed successfully!")

