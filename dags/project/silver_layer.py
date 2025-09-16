import duckdb
import logging
import os
import pandas as pd

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/silver.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("silver")

def extract_silver():
    """Read from Bronze DB into Pandas."""
    logger.info("Extracting data from Bronze...")
    conn = duckdb.connect("data/bronze.duckdb")

    cust_info = conn.execute("SELECT * FROM crm_cust_info").df()
    prd_info = conn.execute("SELECT * FROM crm_prd_info").df()
    sales_details = conn.execute("SELECT * FROM crm_sales_details").df()
    cust_az12 = conn.execute("SELECT * FROM erp_cust_az12").df()
    px_cat = conn.execute("SELECT * FROM erp_px_cat_g1v2").df()
    loc_a101 = conn.execute("SELECT * FROM erp_loc_a101").df()

    conn.close()
    return cust_info, prd_info, sales_details, cust_az12, px_cat, loc_a101

def transform_silver(dfs):
    """Clean and normalize data according to Silver rules."""
    logger.info("Transforming Bronze -> Silver...")
    cust_info, prd_info, sales_details, cust_az12, px_cat, loc_a101 = dfs

    # CRM customers
    ###############################################################################
    cust_info = cust_info.sort_values('cst_create_date', ascending=False).drop_duplicates(subset='cst_id', keep='first')
    cust_info['cst_firstname'] = cust_info['cst_firstname'].astype(str).str.strip().str.title()
    cust_info['cst_lastname'] = cust_info['cst_lastname'].astype(str).str.strip().str.title()
    cust_info['cst_marital_status'] = cust_info['cst_marital_status'].str.upper().map(
        {"S": "Single", "M": "Married"}
    ).fillna("n/a")
    cust_info['cst_gndr'] = cust_info['cst_gndr'].str.upper().map(
        {"F": "Female", "M": "Male"}
    ).fillna("n/a")
    ###############################################################################
    
    # CRM products
    ###############################################################################
    line_map = {"M": "Mountain", "R": "Road", "S": "Other Sales", "T": "Touring"}
    prd_info['prd_line'] = prd_info['prd_line'].str.upper().map(line_map).fillna("n/a")
    prd_info['prd_cost'] = prd_info['prd_cost'].fillna(0)
    prd_info['cat_id'] = prd_info['prd_key'].str[:5].str.replace("-", "_")
    prd_info['prd_key'] = prd_info['prd_key'].str[6:]
    ###############################################################################

    # CRM sales details
    ###############################################################################
    sales_details['sls_price'] = sales_details.apply(
        lambda row: row['sls_sales']/row['sls_quantity'] if pd.isna(row['sls_price']) else row['sls_price'], axis=1
    )

    sales_details['sls_price'] = sales_details['sls_price'].abs()

    # Recalculate sales
    sales_details['sls_sales'] = sales_details.apply(
        lambda row: row['sls_quantity']*abs(row['sls_price']) if pd.isna(row['sls_sales']) or row['sls_sales'] != row['sls_quantity']*abs(row['sls_price']) else row['sls_sales'], axis=1
    )

    sales_details['sls_sales'] = sales_details['sls_quantity'] * sales_details['sls_price']
    ###############################################################################

    # ERP customers
    ###############################################################################
    cust_az12['cid'] = cust_az12['cid'].str.replace("^NAS", "", regex=True)
    cust_az12['bdate'] = pd.to_datetime(cust_az12['bdate'], errors="coerce")
    cust_az12.loc[cust_az12['bdate'] > pd.Timestamp.today(), 'bdate'] = pd.NaT
    cust_az12['gen'] = cust_az12['gen'].astype(str).str.strip().str.upper().map(
        {"F": "Female", "M": "Male"}
    ).fillna("n/a")
    ###############################################################################

    # ERP locations
    ###############################################################################
    loc_a101['cid'] = loc_a101['cid'].str.replace("-", "")
    loc_a101['cntry'] = loc_a101['cntry'].str.strip().replace(
        {"DE": "Germany", "US": "United States", "USA": "United States", "": "n/a"}
    ).fillna("n/a")
    ###############################################################################


    return cust_info, prd_info, sales_details, cust_az12, px_cat, loc_a101

def load_silver(dfs):
    """Save clean DataFrames into Silver DB."""
    logger.info("Loading Silver data into DuckDB...")
    cust_info, prd_info, sales_details, cust_az12, px_cat, loc_a101 = dfs

    conn = duckdb.connect("data/silver.duckdb")

    conn.register("cust_info", cust_info)
    conn.execute("CREATE OR REPLACE TABLE crm_cust_info AS SELECT * FROM cust_info")
    conn.unregister("cust_info")

    conn.register("prd_info", prd_info)
    conn.execute("CREATE OR REPLACE TABLE crm_prd_info AS SELECT * FROM prd_info")
    conn.unregister("prd_info")

    conn.register("sales_details", sales_details)
    conn.execute("CREATE OR REPLACE TABLE crm_sales_details AS SELECT * FROM sales_details")
    conn.unregister("sales_details")

    conn.register("cust_az12", cust_az12)
    conn.execute("CREATE OR REPLACE TABLE erp_cust_az12 AS SELECT * FROM cust_az12")
    conn.unregister("cust_az12")

    conn.register("px_cat", px_cat)
    conn.execute("CREATE OR REPLACE TABLE erp_px_cat_g1v2 AS SELECT * FROM px_cat")
    conn.unregister("px_cat")

    conn.register("loc_a101", loc_a101)
    conn.execute("CREATE OR REPLACE TABLE erp_loc_a101 AS SELECT * FROM loc_a101")
    conn.unregister("loc_a101")

    conn.close()
    logger.info("Silver load complete!")

def load_silver_layer():
    """Orchestration for Silver layer."""
    dfs = extract_silver()
    clean_dfs = transform_silver(dfs)
    load_silver(clean_dfs)
