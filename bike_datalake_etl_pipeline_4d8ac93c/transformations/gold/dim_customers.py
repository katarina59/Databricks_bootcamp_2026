"""
Gold Layer - Customer Dimension
================================
SCD Type 1 dimension combining CRM, ERP customer, and ERP location data
Business logic: CRM as master with ERP enrichment
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# GOLD LAYER - dim_customers
# =============================================================================

@dp.materialized_view(
    name="gold.gold_dim_customers",
    comment="Customer dimension with CRM as master, enriched with ERP data"
)
@dp.expect_or_fail("unique_customer_id", "customer_id IS NOT NULL")
@dp.expect("has_gender", "gender IS NOT NULL")
def gold_dim_customers():
    """
    Create customer dimension table
    
    JOIN Logic:
    - CRM customers as master (LEFT JOIN)
    - Normalize CRM customer_id: remove 'NAS-' prefix  
    - JOIN ERP customer ON normalized customer_id -> birthday_date, erp_gender
    - JOIN ERP location ON normalized customer_id -> country
    
    Gender Logic:
    - COALESCE(crm_gender IF != 'n/a', erp_gender)
    - CRM is primary, ERP is fallback
    
    Surrogate Key:
    - ROW_NUMBER() over customer_id (sorted)
    """
    
    # Step 1: Prepare each source table with required columns only
    crm_prep = (
        spark.read.table("silver.silver_crm_customers")
        .withColumn(
            "customer_id_normalized",
            F.regexp_replace(F.col("customer_id"), r'NAS-', '')
        )
        .select(
            F.col("customer_id_normalized"),
            F.col("customer_id"),
            F.col("firstname"),
            F.col("lastname"),
            F.col("gender").alias("crm_gender"),
            F.col("marital_status"),
            F.col("create_date")
        )
    )
    
    erp_cust_prep = (
        spark.read.table("silver.silver_erp_cust")
        .select(
            F.col("customer_id").alias("erp_cust_key"),
            F.col("birthday_date"),
            F.col("gender").alias("erp_gender")
        )
    )
    
    erp_loc_prep = (
        spark.read.table("silver.silver_erp_loc")
        .select(
            F.col("customer_id").alias("erp_loc_key"),
            F.col("country")
        )
    )
    
    # Step 2: JOIN all three tables
    joined = (
        crm_prep
        .join(
            erp_cust_prep,
            crm_prep["customer_id_normalized"] == erp_cust_prep["erp_cust_key"],
            how="left"
        )
        .join(
            erp_loc_prep,
            crm_prep["customer_id_normalized"] == erp_loc_prep["erp_loc_key"],
            how="left"
        )
    )
    
    # Step 3: Create final dimension with surrogate key and business logic
    window_spec = Window.orderBy("customer_id")
    
    return (
        joined
        .withColumn(
            "customer_key",
            F.row_number().over(window_spec)
        )
        .withColumn(
            "gender",
            F.when(
                (F.col("crm_gender").isNotNull()) & (F.col("crm_gender") != "n/a"),
                F.col("crm_gender")
            ).otherwise(
                F.col("erp_gender")
            )
        )
        .select(
            F.col("customer_key"),
            F.col("customer_id"),
            F.col("firstname"),
            F.col("lastname"),
            F.col("gender"),
            F.col("marital_status"),
            F.col("birthday_date"),
            F.col("country"),
            F.col("create_date")
        )
    )
