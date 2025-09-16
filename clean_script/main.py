import time
import logging
import yaml
from data_extractor import extract_orders
from transformations import transform_orders
from data_loader import load_orders



def setup_logging(log_file: str = "pipeline.log") -> None:
    """Configure logging."""
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.getLogger().addHandler(logging.StreamHandler())


def run_pipeline():
    """Run the ETL pipeline."""
    setup_logging()

    start = time.time()
    logging.info("Pipeline started")

    raw_df = extract_orders("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv")
    transformed_df = transform_orders(raw_df)
    load_orders(transformed_df, "data", "cleaned.csv")

    end = time.time()
    logging.info(f"Pipeline finished in {end - start:.2f} seconds")


if __name__ == "__main__":
    run_pipeline()
