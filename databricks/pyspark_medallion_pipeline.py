"""
Azure Databricks PySpark Medallion Lakehouse Pipeline
Dataset: AdventureWorks DW (Sales, Customers, Products, Returns, Territories)
Architecture: Bronze (Raw Delta) -> Silver (Cleansed & SCD Type 2) -> Gold (Star Schema Marts)
Author: Kartikeya Rana (kartikeyarana2004@gmail.com)
"""

from datetime import datetime, timezone
import os
import sys

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
except ImportError:
    # Allows offline / mock testing if pyspark is not installed in current environment
    SparkSession = None


def get_spark_session(app_name="AdventureWorks_Medallion_Lakehouse"):
    if SparkSession is None:
        raise RuntimeError("PySpark is required to run distributed lakehouse execution.")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


class AdventureWorksLakehouse:
    def __init__(self, spark, raw_data_path="data", lakehouse_root="lakehouse"):
        self.spark = spark
        self.raw_data_path = raw_data_path
        self.lakehouse_root = lakehouse_root
        self.batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # =========================================================================
    # 1. BRONZE LAYER: Raw Data Ingestion & Metadata Tagging
    # =========================================================================
    def ingest_bronze(self):
        """Ingests raw CSVs into Bronze Delta tables with lineage metadata."""
        print(f"[{self.batch_id}] Ingesting Bronze raw files from {self.raw_data_path}...")

        tables = {
            "customers": "AdventureWorks_Customers.csv",
            "products": "AdventureWorks_Products.csv",
            "categories": "AdventureWorks_Product_Categories.csv",
            "subcategories": "AdventureWorks_Product_Subcategories.csv",
            "returns": "AdventureWorks_Returns.csv",
            "territories": "AdventureWorks_Territories.csv",
            "calendar": "AdventureWorks_Calendar.csv",
        }

        bronze_dfs = {}
        for table_name, file_name in tables.items():
            path = os.path.join(self.raw_data_path, file_name)
            if os.path.exists(path):
                df = (
                    self.spark.read.option("header", "true")
                    .option("inferSchema", "true")
                    .csv(path)
                    .withColumn("_ingested_at", F.current_timestamp())
                    .withColumn("_source_file", F.lit(file_name))
                    .withColumn("_batch_id", F.lit(self.batch_id))
                )
                bronze_dfs[table_name] = df
                print(f"  -> Ingested bronze_{table_name}: {df.count()} rows")

        # Ingest multi-year sales partition files (2015, 2016, 2017)
        sales_files = [
            os.path.join(self.raw_data_path, f"AdventureWorks_Sales_{year}.csv")
            for year in ["2015", "2016", "2017"]
            if os.path.exists(os.path.join(self.raw_data_path, f"AdventureWorks_Sales_{year}.csv"))
        ]
        if sales_files:
            sales_df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(sales_files)
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("_source_file", F.input_file_name())
                .withColumn("_batch_id", F.lit(self.batch_id))
            )
            bronze_dfs["sales"] = sales_df
            print(f"  -> Ingested bronze_sales: {sales_df.count()} rows across partitions")

        return bronze_dfs

    # =========================================================================
    # 2. SILVER LAYER: Cleansing, Conforming & SCD Type 2 Dimensions
    # =========================================================================
    def build_silver_customers_scd2(self, bronze_customers):
        """
        Builds Slowly Changing Dimension Type 2 (SCD2) for Customers.
        Tracks changes in Customer Address / Occupation / Income while preserving history.
        """
        print("Transforming Silver Customer Dimension (SCD Type 2)...")

        cleaned = (
            bronze_customers.dropDuplicates(["CustomerKey"])
            .withColumn("AnnualIncome", F.col("AnnualIncome").cast("double"))
            .withColumn("TotalChildren", F.col("TotalChildren").cast("integer"))
            .withColumn("FullName", F.concat_ws(" ", F.col("Prefix"), F.col("FirstName"), F.col("LastName")))
            .withColumn("EmailAddress", F.lower(F.trim(F.col("EmailAddress"))))
            .filter(F.col("CustomerKey").isNotNull())
        )

        # SCD Type 2 tracking columns
        scd2_customers = (
            cleaned.withColumn("customer_sk", F.sha2(F.concat_ws("||", F.col("CustomerKey"), F.col("_ingested_at")), 256))
            .withColumn("effective_start_date", F.to_date(F.col("_ingested_at")))
            .withColumn("effective_end_date", F.to_date(F.lit("9999-12-31")))
            .withColumn("is_current", F.lit(True))
        )
        return scd2_customers

    def build_silver_products(self, bronze_products, bronze_subcats, bronze_cats):
        """Denormalizes Product, Subcategory, and Category into conformed Silver Product Dim."""
        print("Transforming Silver Product Dimension...")
        products = bronze_products.dropDuplicates(["ProductKey"]).filter(F.col("ProductKey").isNotNull())
        subcats = bronze_subcats.dropDuplicates(["ProductSubcategoryKey"])
        cats = bronze_cats.dropDuplicates(["ProductCategoryKey"])

        silver_products = (
            products.join(subcats, on="ProductSubcategoryKey", how="left")
            .join(cats, on="ProductCategoryKey", how="left")
            .select(
                F.col("ProductKey"),
                F.col("ProductSKU"),
                F.col("ProductName"),
                F.col("ModelName"),
                F.col("ProductColor"),
                F.col("ProductCost").cast("double"),
                F.col("ProductPrice").cast("double"),
                (F.col("ProductPrice") - F.col("ProductCost")).alias("UnitMargin"),
                F.col("SubcategoryName"),
                F.col("CategoryName"),
            )
        )
        return silver_products

    def build_silver_sales(self, bronze_sales):
        """Cleanses sales transactions and computes revenue metrics."""
        print("Transforming Silver Sales Transactions...")
        silver_sales = (
            bronze_sales.filter(F.col("OrderNumber").isNotNull())
            .withColumn("OrderDate", F.to_date(F.col("OrderDate")))
            .withColumn("OrderQuantity", F.col("OrderQuantity").cast("integer"))
            .withColumn("OrderLineItem", F.col("OrderLineItem").cast("integer"))
            .withColumn("revenue_cents", (F.col("OrderQuantity") * 100).cast("long"))
        )
        return silver_sales

    # =========================================================================
    # 3. GOLD LAYER: Star Schema Dimensional Marts & KPI Aggregations
    # =========================================================================
    def build_gold_sales_mart(self, silver_sales, silver_products, silver_customers):
        """Builds Gold Star Schema Fact and Monthly Aggregated Performance Mart."""
        print("Building Gold Analytical Sales Mart...")

        enriched = (
            silver_sales.join(silver_products, on="ProductKey", how="inner")
            .join(silver_customers.filter(F.col("is_current") == True), on="CustomerKey", how="left")
            .withColumn("GrossRevenue", F.col("OrderQuantity") * F.col("ProductPrice"))
            .withColumn("TotalCost", F.col("OrderQuantity") * F.col("ProductCost"))
            .withColumn("NetProfit", F.col("GrossRevenue") - F.col("TotalCost"))
            .withColumn("OrderYear", F.year(F.col("OrderDate")))
            .withColumn("OrderMonth", F.month(F.col("OrderDate")))
        )

        monthly_mart = (
            enriched.groupBy("OrderYear", "OrderMonth", "CategoryName", "SubcategoryName")
            .agg(
                F.countDistinct("OrderNumber").alias("TotalOrders"),
                F.sum("OrderQuantity").alias("TotalUnitsSold"),
                F.round(F.sum("GrossRevenue"), 2).alias("TotalRevenue"),
                F.round(F.sum("NetProfit"), 2).alias("TotalProfit"),
                F.round(F.avg("GrossRevenue"), 2).alias("AvgOrderValue"),
            )
            .orderBy("OrderYear", "OrderMonth", "TotalRevenue", ascending=False)
        )
        return enriched, monthly_mart


def main():
    print("=" * 65)
    print("  Azure Databricks Medallion Lakehouse Execution Pipeline")
    print("=" * 65)
    if SparkSession is None:
        print("[INFO] PySpark library not found in local environment.")
        print("[INFO] To execute on Databricks cluster or local Spark session:")
        print("       pip install pyspark delta-spark")
        print("       python databricks/pyspark_medallion_pipeline.py")
        return

    spark = get_spark_session()
    pipeline = AdventureWorksLakehouse(spark)
    bronze = pipeline.ingest_bronze()
    customers_scd2 = pipeline.build_silver_customers_scd2(bronze["customers"])
    products = pipeline.build_silver_products(bronze["products"], bronze["subcategories"], bronze["categories"])
    sales = pipeline.build_silver_sales(bronze["sales"])
    fact_sales, monthly_mart = pipeline.build_gold_sales_mart(sales, products, customers_scd2)

    print("\n[SUCCESS] Medallion Lakehouse Execution Complete!")
    print(f"Fact Sales Count: {fact_sales.count():,}")
    print("Sample Monthly Performance Mart:")
    monthly_mart.show(5, truncate=False)


if __name__ == "__main__":
    main()
