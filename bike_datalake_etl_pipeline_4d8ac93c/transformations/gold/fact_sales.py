"""
Gold Layer - Sales Fact Table
==============================
Fact table with dimension foreign keys and calculated measures
Includes referential integrity checks to prevent orphan facts
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# =============================================================================
# GOLD LAYER - fact_sales
# =============================================================================

@dp.materialized_view(
    name="gold.gold_fact_sales",
    comment="Sales fact table with dimension keys and revenue calculation"
)
@dp.expect_or_drop("has_dimension_keys", "customer_key IS NOT NULL AND product_key_sk IS NOT NULL")
@dp.expect_or_fail("positive_revenue", "revenue >= 0")
@dp.expect("valid_dates_order", "sales_ship_date IS NULL OR sales_order_date <= sales_ship_date")
def gold_fact_sales():
    """
    Create sales fact table
    
    Transformations:
    - Lookup dimension keys:
      * customer_key from gold_dim_customers
      * product_key_sk from gold_dim_products (only is_current = TRUE)
    - Calculate revenue: sales_amount * sales_order_qty
    - Drop orphan facts (no matching dimensions)
    - Fail pipeline if revenue is negative
    
    Data Quality:
    - Drop rows without dimension keys
    - Fail on negative revenue (business rule violation)
    - Warn if ship date is before order date
    """
    
    # Read Silver sales
    sales = spark.read.table("workspace.silver.silver_erp_sales")
    
    # Read Gold dimensions
    dim_customers = spark.read.table("workspace.gold.gold_dim_customers")
    dim_products = (
        spark.read.table("workspace.gold.gold_dim_products")
        .filter(F.col("is_current") == True)
    )
    
    # Join with customer dimension
    fact = (
        sales
        .join(
            dim_customers.select("customer_key", "customer_id"),
            sales["customer_id"] == dim_customers["customer_id"],
            how="left"
        )
    )
    
    # Join with product dimension (only current products)
    fact = (
        fact
        .join(
            dim_products.select("product_key_sk", "product_key"),
            fact["product_key"] == dim_products["product_key"],
            how="left"
        )
    )
    
    # Calculate revenue and select final columns
    return (
        fact
        .withColumn(
            "revenue",
            F.col("sales_amount") * F.col("sales_order_qty")
        )
        .select(
            F.col("sales_order_number"),
            F.col("customer_key"),
            F.col("product_key_sk"),
            F.col("sales_order_date"),
            F.col("sales_ship_date"),
            F.col("sales_due_date"),
            F.col("sales_order_qty"),
            F.col("sales_amount"),
            F.col("revenue")
        )
    )
