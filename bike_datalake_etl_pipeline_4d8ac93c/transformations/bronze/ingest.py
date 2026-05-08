"""
Bronze Layer - Raw Data Ingestion
==================================
Auto Loader streaming tables for ingesting CSV files
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# =============================================================================
# BRONZE LAYER - Streaming Tables with Auto Loader
# =============================================================================

@dp.table(
    name="bronze.crm_cust_info"
)
def bronze_crm_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("pathGlobFilter", "cust_info.csv")
        .load(f"/Volumes/workspace/bronze/source_systems/source_crm/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze.crm_sales_details"
)
def bronze_crm_sales_details():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("pathGlobFilter", "sales_details.csv")
        .load(f"/Volumes/workspace/bronze/source_systems/source_crm/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze.crm_prd_info"
)
def bronze_crm_prd_info():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("pathGlobFilter", "prd_info.csv")
        .load(f"/Volumes/workspace/bronze/source_systems/source_crm/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze.erp_cust"
)
def bronze_erp_cust():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("pathGlobFilter", "CUST_AZ12.csv")
        .load(f"/Volumes/workspace/bronze/source_systems/source_erp/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze.erp_loc"
)
def bronze_erp_loc():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("pathGlobFilter", "LOC_A101.csv")
        .load(f"/Volumes/workspace/bronze/source_systems/source_erp/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze.erp_px_cat"
)
def bronze_erp_px_cat():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("pathGlobFilter", "PX_CAT_G1V2.csv")
        .load(f"/Volumes/workspace/bronze/source_systems/source_erp/")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
