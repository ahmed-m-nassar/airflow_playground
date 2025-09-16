from bronze_layer import load_bronze_layer
from silver_layer import load_silver_layer
from gold_layer import load_gold_layer

if __name__ == "__main__":
    load_bronze_layer()
    load_silver_layer()
    load_gold_layer()
    print("✅ ETL Pipeline finished successfully!")