"""
Multi-Source Insurance Data Mart Build
----------------------------------------
Ingests three intentionally messy, inconsistent source extracts (policy,
claims, CRM) and builds a single, clean, unified data mart using PySpark.

Run with: python3 build_data_mart.py
Requires: pyspark (pip install pyspark), Java runtime
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("InsuranceDataMart").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# --- Load raw sources ---
policy = spark.read.csv("source_policy_export.csv", header=True, inferSchema=True)
claims = spark.read.csv("source_claims_export.csv", header=True, inferSchema=True)
crm = spark.read.csv("source_customer_crm.csv", header=True, inferSchema=True)

raw_policy_count = policy.count()
raw_claims_count = claims.count()
raw_crm_count = crm.count()

# --- Clean & standardize policy status/product casing ---
policy_clean = policy.withColumn("status_clean", F.upper(F.trim(F.col("status"))))
policy_clean = policy_clean.withColumn(
    "status_clean", F.when(F.col("status_clean") == "", "UNKNOWN").otherwise(F.col("status_clean"))
)
policy_clean = policy_clean.withColumn("product_clean", F.upper(F.trim(F.col("product_type"))))

# Data quality decision: drop rows with missing premium, but count and log it
# rather than silently discarding them.
missing_premium = policy_clean.filter(F.col("monthly_premium").isNull()).count()
policy_clean = policy_clean.filter(F.col("monthly_premium").isNotNull())

# --- Standardize customer_id format across all sources (uppercase, trimmed) ---
crm_clean = crm.withColumn("customer_id", F.upper(F.trim(F.col("customer_id_raw"))))

# De-duplicate CRM records that differ only by case (e.g. "CUST0001" vs "cust0001")
before_dedupe = crm_clean.count()
crm_clean = crm_clean.dropDuplicates(["customer_id"])
after_dedupe = crm_clean.count()
duplicates_removed = before_dedupe - after_dedupe

crm_clean = crm_clean.withColumn(
    "region_clean",
    F.when((F.col("region").isNull()) | (F.col("region") == ""), "UNSPECIFIED").otherwise(F.col("region")),
)

# --- Standardize claims: normalize customer id, parse DD/MM/YYYY dates ---
claims_clean = claims.withColumn("customer_id", F.upper(F.trim(F.col("customer_id_raw"))))
claims_clean = claims_clean.withColumn("date_filed", F.to_date(F.col("date_filed_raw"), "dd/MM/yyyy"))

# --- Build unified data mart: join policy + crm + claims summary ---
policy_customer = policy_clean.join(
    crm_clean.select("customer_id", "region_clean", "tenure_years"), on="customer_id", how="left"
)

claims_summary = claims_clean.groupBy("policy_id").agg(
    F.count("claim_id").alias("claim_count"), F.sum("claim_amount").alias("total_claim_amount")
)

data_mart = policy_customer.join(claims_summary, on="policy_id", how="left")
data_mart = data_mart.fillna({"claim_count": 0, "total_claim_amount": 0.0})

final_mart_count = data_mart.count()

# --- KPIs derived from the mart ---
active_count = data_mart.filter(F.col("status_clean") == "ACTIVE").count()
unspecified_region_count = data_mart.filter(F.col("region_clean") == "UNSPECIFIED").count()
total_premium = data_mart.agg(F.sum("monthly_premium")).collect()[0][0]
policies_with_claims = data_mart.filter(F.col("claim_count") > 0).count()

print("=== DATA MART BUILD SUMMARY ===")
print(f"Raw source rows: policy={raw_policy_count}, claims={raw_claims_count}, crm={raw_crm_count}")
print(f"Duplicate CRM records removed: {duplicates_removed}")
print(f"Policy records dropped for missing premium: {missing_premium}")
print(f"Final unified data mart rows: {final_mart_count}")
print(f"Active policies: {active_count}")
print(f"Policies with unspecified region: {unspecified_region_count}")
print(f"Total monthly premium (mart): ${round(total_premium, 2)}")
print(f"Policies with at least one claim: {policies_with_claims}")

data_mart.select(
    "policy_id", "customer_id", "status_clean", "product_clean", "monthly_premium",
    "region_clean", "tenure_years", "claim_count", "total_claim_amount",
).toPandas().to_csv("unified_data_mart.csv", index=False)

spark.stop()
