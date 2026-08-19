#!/usr/bin/env python
# coding: utf-8

# ## NB_02_LOAD_BRONZE
# 
# null

# In[1]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
files = notebookutils.fs.ls("Files/landing/olist")
print(f"Au fost gasite {len(files)} fisiere:/n")
for file in files:
    print(file.name)


# In[5]:


from datetime import datetime, timezone
from pyspark.sql import functions as F

BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
SOURCE_PATH = "Files/landing/olist/customers"

df_customers = (
      spark.read  
           .option("header", "true")
           .option("delimiter", ",")
           .option("encoding", "UTF-8")
           .option("mode", "PERMISSIVE")
           .csv(SOURCE_PATH)
           .withColumn("_ingestion_timestamp", F.current_timestamp())
           .withColumn("_ingestion_date", F.current_date())
           .withColumn(
                "_source_file",
                 F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)
           )
           .withColumn("_batch_id", F.lit(BATCH_ID))    
)

print(f"BATCH ID: {BATCH_ID}")
print(f"Numar de randuri: {df_customers.count():,}")

df_customers.printSchema()
display(df_customers.limit(10))


# In[6]:


(
    df_customers.write 
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("bronze.customers")
)

print("Tabelul bronze.customers a fost creat cu succes.")


# In[7]:


validation_customers = spark.sql("""
   SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        COUNT(DISTINCT _batch_id) AS batch_count,
        MIN(_ingestion_timestamp) AS ingestion_start,
        MAX(_ingestion_timestamp) AS ingestion_end
    FROM bronze.customers""")

display(validation_customers)


# In[10]:


landing_items = notebookutils.fs.ls("Files/landing/olist")

print(f"Ellemente gasite: {len(landing_items)}/n")

for item in landing_items:
    print(f"{item.name} -> {item.path}")


# In[1]:


# Get all Bronze table names
bronze_tables = [
    table.name
    for table in spark.catalog.listTables("bronze")
    if not table.isTemporary
]

# Add and populate the _source_dataset audit column
for table_name in bronze_tables:

    full_table_name = f"bronze.{table_name}"
    table_columns = spark.table(full_table_name).columns

    if "_source_dataset" not in table_columns:
        spark.sql(f"""
            ALTER TABLE {full_table_name}
            ADD COLUMNS (_source_dataset STRING)
        """)

    spark.sql(f"""
        UPDATE {full_table_name}
        SET _source_dataset = '{table_name}'
        WHERE _source_dataset IS NULL
    """)

    print(f"Audit column verified: {full_table_name}")

print("The _source_dataset column is available in all Bronze tables.")


# In[10]:


from pyspark.sql import functions as F

EXPECTED_ROWS = {
    "product_category_translation": 71,
    "customers": 99441,
    "geolocation": 1000163,
    "order_items": 112650,
    "orders": 99441,
    "order_payments": 103886,
    "products": 32951,
    "order_reviews": 99224,
    "sellers": 3095
}

validation_results = []

for table_name, expected_rows in EXPECTED_ROWS.items():
    full_table_name = f"bronze.{table_name}"
    df = spark.table(full_table_name)

    actual_rows = df.count()

    missing_audit_values = df.filter(
        F.col("_ingestion_timestamp").isNull() |
        F.col("_ingestion_date").isNull() |
        F.col("_source_file").isNull() |
        F.col("_source_dataset").isNull() |
        F.col("_batch_id").isNull()
    ).count()

    batch_count = df.select("_batch_id").distinct().count()

    status = (
        "PASS"
        if (
            actual_rows == expected_rows
            and missing_audit_values == 0
            and batch_count == 1
        )
        else "FAIL"
    )

    # Acest bloc trebuie să fie în interiorul buclei for
    validation_results.append((
        full_table_name,
        expected_rows,
        actual_rows,
        missing_audit_values,
        batch_count,
        status
    ))

validation_df = spark.createDataFrame(
    validation_results,
    [
        "table_name",
        "expected_rows",
        "actual_rows",
        "missing_audit_values",
        "batch_count",
        "status"
    ]
)

print(f"Tabele validate: {len(validation_results)}")
display(validation_df.orderBy("table_name"))


# In[9]:


from pyspark.sql import functions as F

SOURCE_PATH = "Files/landing/olist/reviews"


batch_row = (
    spark.table("bronze.customers")
        .select("_batch_id")
        .filter(F.col("_batch_id").isNotNull())
        .first()
)

BATCH_ID = batch_row["_batch_id"]

df_reviews = (
    spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(SOURCE_PATH)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
        .withColumn(
            "_source_file",
            F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)
        )
        .withColumn("_source_dataset", F.lit("reviews"))
        .withColumn("_batch_id", F.lit(BATCH_ID))
)

print(f"Rânduri citite corect: {df_reviews.count():,}")

(
    df_reviews.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("bronze.order_reviews")
)

print("Tabelul bronze.order_reviews a fost recreat corect.")


# In[2]:


display(spark.sql("SHOW TABLES IN bronze"))


# In[1]:


# Get all Bronze table names
bronze_tables = [
    table.name
    for table in spark.catalog.listTables("bronze")
    if not table.isTemporary
]

# Add and populate _source_dataset
for table_name in bronze_tables:

    full_table_name = f"bronze.{table_name}"
    table_columns = spark.table(full_table_name).columns

    if "_source_dataset" not in table_columns:
        spark.sql(f"""
            ALTER TABLE {full_table_name}
            ADD COLUMNS (_source_dataset STRING)
        """)

    spark.sql(f"""
        UPDATE {full_table_name}
        SET _source_dataset = '{table_name}'
        WHERE _source_dataset IS NULL
    """)

    print(f"Audit column verified: {full_table_name}")

print("The _source_dataset column is available in all Bronze tables.")


# In[3]:


from datetime import datetime, timezone
from pyspark.sql import functions as F


batch_row = (
    spark.table("bronze.customers")
        .select("_batch_id")
        .filter(F.col("_batch_id").isNotNull())
        .first()
)

BATCH_ID = (
    batch_row["_batch_id"]
    if batch_row
    else datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
)

MISSING_DATASETS = {
    "category_translation": "product_category_translation",
    "geolocation": "geolocation",
    "order_items": "order_items",
    "orders": "orders",
    "payments": "order_payments",
    "products": "products",
    "reviews": "order_reviews"
}

for source_folder, target_table in MISSING_DATASETS.items():

    source_path = f"Files/landing/olist/{source_folder}"
    full_table_name = f"bronze.{target_table}"

    print(f"\nÎncarc {source_folder} -> {full_table_name}")

    df = (
        spark.read
            .option("header", "true")
            .option("delimiter", ",")
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .csv(source_path)
            .withColumn("_ingestion_timestamp", F.current_timestamp())
            .withColumn("_ingestion_date", F.current_date())
            .withColumn(
                "_source_file",
                F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)
            )
            .withColumn("_source_dataset", F.lit(source_folder))
            .withColumn("_batch_id", F.lit(BATCH_ID))
    )

    (
        df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_table_name)
    )

    row_count = spark.table(full_table_name).count()
    print(f"Finalizat: {full_table_name} — {row_count:,} randuri")

print("\nIncarcarea tabelelor lipsa s-a terminat.")


# In[4]:


files = notebookutils.fs.ls("Files/landing/olist")

customer_files = [
    file.path
    for file in files
    if "customers" in file.name.lower()
]

print(customer_files)

