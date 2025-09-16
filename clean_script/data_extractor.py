import pandas as pd
import logging


def extract_orders(url: str) -> pd.DataFrame:
    """
    Extract (load) orders dataset from a URL.

    Args:
        url (str): URL of the CSV file.

    Returns:
        pd.DataFrame: Orders DataFrame.
    """
    logging.info(f"Extracting data from {url}")
    try:
        return pd.read_csv(url)
    except Exception as e:
        logging.error(f"Failed to extract data from {url}: {e}")
        raise
