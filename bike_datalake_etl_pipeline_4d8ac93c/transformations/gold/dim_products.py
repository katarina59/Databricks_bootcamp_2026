"""
Gold Layer - Product Dimension
===============================
SCD Type 2 dimension for product history tracking
Includes surrogate key, product ID extraction, and validity tracking
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# GOLD LAYER - dim_products
# =============================================================================

@dp.materialized_view(
    name="gold.gold_dim_products",
    comment="Product dimension with SCD Type 2 columns for history tracking"
)
@dp.expect_or_fail("unique_product_key", "product_key IS NOT NULL")
def gold_dim_products():
    """
    Create product dimension table with SCD Type 2 structure
    
    Transformations:
    - product_key_sk: ROW_NUMBER() surrogate key
    - product_id: Extract from product_key ('BK-M68B-42' -> 'M68B')
    - SCD Type 2 columns:
      * start_date: current_date()
      * end_date: NULL (active records)
      * is_current: TRUE (all records initially current)
    
    Note: Spark Declarative Pipelines does not support IDENTITY columns,
    so we use ROW_NUMBER() for surrogate keys.
    """
    
    window_spec = Window.orderBy("product_key")
    
    return (
        spark.read.table("silver.silver_erp_product")
        .withColumn(
            "product_key_sk",
            F.row_number().over(window_spec)
        )
        .withColumn(
            "product_id",
            # Extract middle part from product_key format: 'BK-M68B-42' -> 'M68B'
            F.regexp_extract(F.col("product_key"), r'^[^-]+-([^-]+)-', 1)
        )
        .withColumn(
            "start_date",
            F.current_date()
        )
        .withColumn(
            "end_date",
            F.lit(None).cast("date")
        )
        .withColumn(
            "is_current",
            F.lit(True)
        )
        .select(
            F.col("product_key_sk"),
            F.col("product_id"),
            F.col("product_key"),
            F.col("category"),
            F.col("subcategory"),
            F.col("maintenance"),
            F.col("start_date"),
            F.col("end_date"),
            F.col("is_current")
        )
    )
