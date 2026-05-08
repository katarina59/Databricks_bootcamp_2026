"""
Silver Layer - Cleansed & Normalized Data
==========================================
Materialized views with data quality expectations
Transformations: normalization, type casting, data validation
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# =============================================================================
# SILVER LAYER - Materialized Views with Data Quality
# =============================================================================

@dp.materialized_view(
    name="silver.silver_crm_customers",
    comment="Cleansed CRM customer data with normalization"
)
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("customer_id_not_empty", "LENGTH(TRIM(customer_id)) > 0")
@dp.expect("valid_firstname", "firstname IS NOT NULL")
def silver_crm_customers():
    """
    Cleanse and normalize CRM customer data
    - Normalize customer_id: uppercase, trim
    - Convert create_date to DATE type
    - Validate customer_id and firstname
    """
    return (
        spark.read.table("bronze.crm_cust_info")
        .withColumn("customer_id", F.upper(F.trim(F.col("cst_key"))))
        .withColumn("firstname", F.trim(F.col("cst_firstname")))
        .withColumn("lastname", F.trim(F.col("cst_lastname")))
        .withColumn("gender", F.lower(F.trim(F.col("cst_gndr"))))
        .withColumn("marital_status", F.trim(F.col("cst_marital_status")))
        .withColumn(
            "create_date",
            F.to_date(F.col("cst_create_date"))
        )
        .select(
            "customer_id",
            "firstname",
            "lastname",
            "gender",
            "marital_status",
            "create_date",
            "_ingest_timestamp"
        )
    )


@dp.materialized_view(
    name="silver.silver_erp_cust",
    comment="Cleansed ERP customer data with birthday validation"
)
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("customer_id_not_empty", "LENGTH(TRIM(customer_id)) > 0")
@dp.expect("valid_birthday", "birthday_date IS NULL OR birthday_date < current_date()")
def silver_erp_cust():
    """
    Cleanse and normalize ERP customer data
    - Remove 'NAS' prefix from customer_id
    - Convert birthday_date from YYYYMMDD format, handle invalid (0) -> NULL
    - Validate customer_id and birthday logic
    """
    return (
        spark.read.table("bronze.erp_cust")
        .withColumn(
            "customer_id",
            F.upper(F.trim(F.regexp_replace(F.col("CID"), "^NAS", "")))
        )
        .withColumn("gender", F.lower(F.trim(F.col("GEN"))))
        .withColumn(
            "birthday_date",
            F.when(
                # Handle invalid values: NULL, 0, '0', empty string
                (F.col("BDATE").isNull()) |
                (F.col("BDATE") == 0) |
                (F.col("BDATE").cast("string") == "0") |
                (F.col("BDATE").cast("string") == ""),
                F.lit(None).cast("date")
            ).when(
                # YYYYMMDD format (8 digits)
                F.col("BDATE").cast("string").rlike(r'^\d{8}$'),
                F.to_date(F.col("BDATE").cast("string"), "yyyyMMdd")
            ).otherwise(
                # Try default conversion
                F.to_date(F.col("BDATE").cast("string"))
            )
        )
        .select(
            "customer_id",
            "gender",
            "birthday_date",
            "_ingest_timestamp"
        )
    )


@dp.materialized_view(
    name="silver.silver_erp_loc",
    comment="Cleansed ERP location data"
)
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_country", "country IS NOT NULL AND LENGTH(TRIM(country)) > 0")
def silver_erp_loc():
    """
    Cleanse and normalize ERP location data
    - Remove 'NAS' prefix from customer_id
    - Normalize country names
    - Validate customer_id and country
    """
    return (
        spark.read.table("bronze.erp_loc")
        .withColumn(
            "customer_id",
            F.upper(F.trim(F.regexp_replace(F.col("CID"), "^NAS", "")))
        )
        .withColumn("country", F.trim(F.col("CNTRY")))
        .select(
            "customer_id",
            "country",
            "_ingest_timestamp"
        )
    )


@dp.materialized_view(
    name="silver.silver_erp_product",
    comment="Cleansed ERP product data with cost validation"
)
@dp.expect("valid_product_key", "product_key IS NOT NULL")
@dp.expect_or_drop("product_key_not_empty", "LENGTH(TRIM(product_key)) > 0")
@dp.expect("valid_category", "category IS NOT NULL")
def silver_erp_product():
    """
    Cleanse and normalize ERP product data
    - Normalize product_key: uppercase, trim
    - Cast maintenance to DECIMAL, invalid -> 0.00
    - Validate product_key and category
    """
    return (
        spark.read.table("bronze.erp_px_cat")
        .withColumn("product_key", F.upper(F.trim(F.col("ID"))))
        .withColumn("category", F.trim(F.col("CAT")))
        .withColumn("subcategory", F.trim(F.col("SUBCAT")))
        .withColumn(
            "maintenance",
            F.coalesce(
                F.col("MAINTENANCE").cast("decimal(10,2)"),
                F.lit(0.00)
            )
        )
        .select(
            "product_key",
            "category",
            "subcategory",
            "maintenance",
            "_ingest_timestamp"
        )
    )


@dp.materialized_view(
    name="silver.silver_erp_sales",
    comment="Cleansed ERP sales data with quantity and amount validation"
)
@dp.expect("valid_order_number", "sales_order_number IS NOT NULL")
@dp.expect_or_drop("valid_quantity", "sales_order_qty > 0")
@dp.expect_or_drop("valid_amount", "sales_amount >= 0")
@dp.expect("valid_order_date", "sales_order_date IS NOT NULL")
@dp.expect("valid_customer_product", "customer_id IS NOT NULL AND product_key IS NOT NULL")
def silver_erp_sales():
    """
    Cleanse and normalize ERP sales data
    - Normalize IDs: uppercase, trim
    - Convert dates from YYYYMMDD format, handle invalid (0) -> NULL
    - Cast numeric fields, invalid -> 0
    - Drop rows with invalid quantity or negative amount
    """
    def fix_date_col(df, col_name):
        """Convert YYYYMMDD to DATE, handle invalid values -> NULL"""
        return df.withColumn(
            col_name,
            F.when(
                # Handle invalid values
                (F.col(col_name).isNull()) |
                (F.col(col_name) == 0) |
                (F.col(col_name).cast("string") == "0") |
                (F.col(col_name).cast("string") == ""),
                F.lit(None).cast("date")
            ).when(
                # YYYYMMDD format (8 digits)
                F.col(col_name).cast("string").rlike(r'^\d{8}$'),
                F.to_date(F.col(col_name).cast("string"), "yyyyMMdd")
            ).otherwise(
                # Try default conversion
                F.to_date(F.col(col_name).cast("string"))
            )
        )
    
    df = spark.read.table("bronze.crm_sales_details")
    
    # Normalize IDs
    df = (
        df
        .withColumn("sales_order_number", F.upper(F.trim(F.col("sls_ord_num"))))
        .withColumn("product_key", F.upper(F.trim(F.col("sls_prd_key"))))
        .withColumn("customer_id", F.upper(F.trim(F.col("sls_cust_id"))))
    )
    
    # Fix date columns
    for date_col in ["sls_order_dt", "sls_ship_dt", "sls_due_dt"]:
        df = fix_date_col(df, date_col)
    
    # Rename date columns after conversion
    df = (
        df
        .withColumnRenamed("sls_order_dt", "sales_order_date")
        .withColumnRenamed("sls_ship_dt", "sales_ship_date")
        .withColumnRenamed("sls_due_dt", "sales_due_date")
    )
    
    # Cast numeric columns
    df = (
        df
        .withColumn(
            "sales_order_qty",
            F.coalesce(F.col("sls_quantity").cast("int"), F.lit(0))
        )
        .withColumn(
            "sales_amount",
            F.coalesce(F.col("sls_sales").cast("decimal(10,2)"), F.lit(0.00))
        )
    )
    
    return df.select(
        "sales_order_number",
        "product_key",
        "customer_id",
        "sales_order_date",
        "sales_ship_date",
        "sales_due_date",
        "sales_order_qty",
        "sales_amount",
        "_ingest_timestamp"
    )
