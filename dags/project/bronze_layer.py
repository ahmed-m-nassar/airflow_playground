import duckdb
import pandas as pd
import re
import logging
import os

# ------------------------------------------------------------------------------
# Setup logging
# ------------------------------------------------------------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/bronze.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bronze")

# ------------------------------------------------------------------------------
# Helper: Convert Google Drive share link to direct link
# ------------------------------------------------------------------------------
def gdrive_to_direct(link: str) -> str:
    """Convert Google Drive share link to direct download link."""
    file_id = re.search(r"/d/([a-zA-Z0-9_-]+)", link).group(1)
    return f"https://drive.google.com/uc?id={file_id}"

# ------------------------------------------------------------------------------
# Extract
# ------------------------------------------------------------------------------
def extract_bronze():
    """Read raw CSVs from Google Drive into Pandas DataFrames."""
    logger.info("Extracting Bronze data...")

    # CRM datasets
    prd_info_link = "https://drive.google.com/file/d/12MK4_d1YPdMMKJUX0EKoFFXssvs-QYwC/view?usp=sharing"
    cust_info_link = "https://drive.google.com/file/d/1_oQHtA5rEuROph9IcUaS3rH4V_dF7oh-/view?usp=sharing"
    sales_details_link = "https://drive.google.com/file/d/13Ya15aidlc03VtFeZuAVuTczIUikvyvR/view?usp=sharing"

    # ERP datasets
    cust_az12_link = "https://drive.google.com/file/d/11tqEvaiYC0OxOP4EAxsZz0qMi-MU-jCu/view?usp=sharing"
    px_cat_link = "https://drive.google.com/file/d/1ep0aluUMF50Ep0T3o1yhSrMox-bjGkXb/view?usp=sharing"
    loc_a101_link = "https://drive.google.com/file/d/1IojhfyZ4ZceITrwgBf6xIJKXXNGpI7lW/view?usp=sharing"

    # Load CRM DataFrames
    prd_info = pd.read_csv(gdrive_to_direct(prd_info_link))
    cust_info = pd.read_csv(gdrive_to_direct(cust_info_link))
    sales_details = pd.read_csv(gdrive_to_direct(sales_details_link))

    # Load ERP DataFrames
    cust_az12 = pd.read_csv(gdrive_to_direct(cust_az12_link))
    px_cat = pd.read_csv(gdrive_to_direct(px_cat_link))
    loc_a101 = pd.read_csv(gdrive_to_direct(loc_a101_link))

    logger.info("Extraction complete!")
    return cust_info, prd_info, sales_details, cust_az12, px_cat, loc_a101

# ------------------------------------------------------------------------------
# Transform
# ------------------------------------------------------------------------------
def transform_bronze(dfs):
    """Minimal cleanup and type conversions for Bronze layer."""
    logger.info("Transforming Bronze data...")
    cust_info, prd_info, sales_details, cust_az12, px_cat, loc_a101 = dfs

    # --- CRM ---
    cust_info.columns = cust_info.columns.str.lower()
    cust_info['cst_create_date'] = pd.to_datetime(cust_info['cst_create_date'], errors='coerce')
    cust_info['cst_id'] = pd.to_numeric(cust_info['cst_id'], errors='coerce').astype("Int64")

    prd_info.columns = prd_info.columns.str.lower()
    prd_info['prd_start_dt'] = pd.to_datetime(prd_info['prd_start_dt'], errors='coerce')
    prd_info['prd_end_dt'] = pd.to_datetime(prd_info['prd_end_dt'], errors='coerce')
    prd_info['prd_id'] = pd.to_numeric(prd_info['prd_id'], errors='coerce').astype("Int64")
    prd_info['prd_cost'] = pd.to_numeric(prd_info['prd_cost'], errors='coerce')

    sales_details.columns = sales_details.columns.str.lower()
    sales_details['sls_order_dt'] = pd.to_datetime(sales_details['sls_order_dt'], format='%Y%m%d', errors='coerce')
    sales_details['sls_ship_dt'] = pd.to_datetime(sales_details['sls_ship_dt'], format='%Y%m%d', errors='coerce')
    sales_details['sls_due_dt'] = pd.to_datetime(sales_details['sls_due_dt'], format='%Y%m%d', errors='coerce')
    sales_details['sls_sales'] = pd.to_numeric(sales_details['sls_sales'], errors='coerce')
    sales_details['sls_price'] = pd.to_numeric(sales_details['sls_price'], errors='coerce')
    sales_details['sls_quantity'] = pd.to_numeric(sales_details['sls_quantity'], errors='coerce')

    # --- ERP ---
    cust_az12.columns = cust_az12.columns.str.lower()
    cust_az12['bdate'] = pd.to_datetime(cust_az12['bdate'], errors='coerce')

    loc_a101.columns = loc_a101.columns.str.lower()

    px_cat.columns = px_cat.columns.str.lower()

    logger.info("Transformation complete!")
    return cust_info, prd_info, sales_details, cust_az12, px_cat, loc_a101

# ------------------------------------------------------------------------------
# Load
# ------------------------------------------------------------------------------
def load_bronze(dfs):
    """Save DataFrames to Bronze DuckDB."""
    logger.info("Loading Bronze data into DuckDB...")

    cust_info, prd_info, sales_details, cust_az12, px_cat, loc_a101 = dfs

    os.makedirs("data", exist_ok=True)
    conn = duckdb.connect("data/bronze.duckdb")

    # CRM
    conn.register("cust_info_df", cust_info)
    conn.execute("CREATE OR REPLACE TABLE crm_cust_info AS SELECT * FROM cust_info_df")
    conn.unregister("cust_info_df")

    conn.register("prd_info_df", prd_info)
    conn.execute("CREATE OR REPLACE TABLE crm_prd_info AS SELECT * FROM prd_info_df")
    conn.unregister("prd_info_df")

    conn.register("sales_details_df", sales_details)
    conn.execute("CREATE OR REPLACE TABLE crm_sales_details AS SELECT * FROM sales_details_df")
    conn.unregister("sales_details_df")

    # ERP
    conn.register("cust_az12_df", cust_az12)
    conn.execute("CREATE OR REPLACE TABLE erp_cust_az12 AS SELECT * FROM cust_az12_df")
    conn.unregister("cust_az12_df")

    conn.register("px_cat_df", px_cat)
    conn.execute("CREATE OR REPLACE TABLE erp_px_cat_g1v2 AS SELECT * FROM px_cat_df")
    conn.unregister("px_cat_df")

    conn.register("loc_a101_df", loc_a101)
    conn.execute("CREATE OR REPLACE TABLE erp_loc_a101 AS SELECT * FROM loc_a101_df")
    conn.unregister("loc_a101_df")

    conn.close()
    logger.info("Bronze load complete!")

# ------------------------------------------------------------------------------
# Orchestrator
# ------------------------------------------------------------------------------
def load_bronze_layer():
    """Orchestration function for Bronze layer (Extract → Transform → Load)."""
    logger.info("Starting Bronze layer ETL...")
    raw_dfs = extract_bronze()
    transformed_dfs = transform_bronze(raw_dfs)
    load_bronze(transformed_dfs)
    logger.info("Bronze ETL complete!")
