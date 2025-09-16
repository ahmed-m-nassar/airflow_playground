import duckdb
import logging
import os

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/gold.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("gold")

def extract_gold():
    """Read Silver tables."""
    logger.info("Extracting from Silver...")
    conn = duckdb.connect("data/silver.duckdb")

    crm_cust_info = conn.execute("SELECT * FROM crm_cust_info").df()
    crm_prd_info = conn.execute("SELECT * FROM crm_prd_info").df()
    crm_sales = conn.execute("SELECT * FROM crm_sales_details").df()
    erp_cust = conn.execute("SELECT * FROM erp_cust_az12").df()
    erp_loc = conn.execute("SELECT * FROM erp_loc_a101").df()
    erp_cat = conn.execute("SELECT * FROM erp_px_cat_g1v2").df()
    conn.close()

    return crm_cust_info, crm_prd_info, crm_sales, erp_cust, erp_loc, erp_cat

def transform_gold(dfs):
    """Build fact and dimension tables."""
    logger.info("Transforming Silver -> Gold...")
    crm_cust_info, crm_prd_info, crm_sales, erp_cust, erp_loc, erp_cat = dfs

    # Dimension: Customers
    dim_customers = crm_cust_info.merge(
        erp_cust, left_on="cst_key", right_on="cid", how="left"
    ).merge(
        erp_loc, left_on="cst_key", right_on="cid", how="left"
    )

    # Dimension: Products
    dim_products = crm_prd_info.merge(
        erp_cat, left_on="cat_id", right_on="id", how="left"
    )
    dim_products = dim_products[dim_products['prd_end_dt'].isna()]

    # Fact: Sales
    fact_sales = crm_sales.merge(
        dim_products[["prd_id", "prd_key"]], left_on="sls_prd_key", right_on="prd_key", how="left"
    ).merge(
        dim_customers[["cst_id", "cst_key"]], left_on="sls_cust_id", right_on="cst_id", how="left"
    )

    return dim_customers, dim_products, fact_sales

def load_gold(dfs):
    """Save fact + dim tables to Gold DB."""
    logger.info("Loading Gold tables...")
    dim_customers, dim_products, fact_sales = dfs

    conn = duckdb.connect("data/gold.duckdb")

    conn.register("dim_customers", dim_customers)
    conn.execute("CREATE OR REPLACE TABLE dim_customers AS SELECT * FROM dim_customers")
    conn.unregister("dim_customers")

    conn.register("dim_products", dim_products)
    conn.execute("CREATE OR REPLACE TABLE dim_products AS SELECT * FROM dim_products")
    conn.unregister("dim_products")

    conn.register("fact_sales", fact_sales)
    conn.execute("CREATE OR REPLACE TABLE fact_sales AS SELECT * FROM fact_sales")
    conn.unregister("fact_sales")

    conn.close()
    logger.info("Gold load complete!")

def load_gold_layer():
    """Orchestration for Gold layer."""
    dfs = extract_gold()
    gold_dfs = transform_gold(dfs)
    load_gold(gold_dfs)
