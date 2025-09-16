import pandas as pd
import logging


def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the dataset:
    - Drop null values
    - Keep only rows where 'size' > 2 (larger groups)
    - Add total_bill_per_person column

    Args:
        df (pd.DataFrame): Raw data.

    Returns:
        pd.DataFrame: Transformed data.
    """
    logging.info("Transforming dataset")
    df = df.dropna()
    df = df[df["size"] > 2]
    df["total_bill_per_person"] = df["total_bill"] / df["size"]
    return df
