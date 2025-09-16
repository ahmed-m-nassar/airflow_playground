import os
import pandas as pd
import logging


def load_orders(df: pd.DataFrame, output_dir: str, filename: str) -> None:
    """
    Load (save) transformed dataset to CSV.

    Args:
        df (pd.DataFrame): Transformed data.
        output_dir (str): Output directory.
        filename (str): Output file name.
    """
    logging.info("Saving transformed data")

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)
    df.to_csv(output_path, index=False)

    logging.info(f"Data saved at {output_path}")
