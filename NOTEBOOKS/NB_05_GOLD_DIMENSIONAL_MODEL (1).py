#!/usr/bin/env python
# coding: utf-8

# ## NB_05_GOLD_DIMENSIONAL_MODEL
# 
# null

# In[1]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
# Import the PySpark functions required for the Gold transformations
from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("PySpark functions imported successfully.")


# In[2]:


# Display all tables available in the silver schema
silver_tables = spark.sql("SHOW TABLES IN silver")
print("Silver tables availabel:")
display(silver_tables)
print("Number of Silver tables:", silver_tables.count())


# In[3]:


#Create the Gold schema if it does not already exist
spark.sql("""
      CREATE SCHEMA IF NOT EXISTS gold""")
print("Gold schema created successfully.")


# In[4]:


# Display the tables currently available in the Gold schema
gold_tables = spark.sql("SHOW TABLES IN gold")
print("Tables currently available in the Gold schema:")
display(gold_tables)


# In[6]:


# Read the Orders from the Silver layer
silver_orders = spark.table("silver.orders")
print("Silver orders loaded successfully")
print("Number of Silver rows:", silver_orders.count())

display(silver_orders.limit(10))


# In[7]:


# Find the minimum purchase date and the maximum estimated delivery date
date_limits = silver_orders.select(
    F.min(
        F.to_date("order_purchase_timestamp")
    ).alias("minimum_date"),

    F.max(
        F.to_date("order_estimated_delivery_date")
    ).alias("maximum_date")
)

print("Date limits identified successfully.")
display(date_limits)


# In[8]:


# Extend the date limits to complete calendar years
calendar_limits = date_limits.select(
    F.trunc(
        F.col("minimum_date"),
        "year"
    ).alias("calendar_start_date"),

    F.last_day(
        F.add_months(
            F.trunc(F.col("maximum_date"), "year"),
            11
        )
    ).alias("calendar_end_date")
)

print("Calendar limits created successfully.")
display(calendar_limits)


# In[10]:


# Generate one row for every date in the calendar interval
calendar_dates = calendar_limits.select(
    F.explode(
        F.sequence(
            F.col("calendar_start_date"),
            F.col("calendar_end_date"),
            F.expr("INTERVAL 1 DAY")
        )
    ).alias("date")
)

print("Calendar dates generated successfully.")
print("Number of calendar dates:", calendar_dates.count())

display(calendar_dates.limit(10))


# In[11]:


# Create the Date dimension with analytical calendar columns
dim_date = calendar_dates.select(
    F.date_format("date", "yyyyMMdd")
        .cast("int")
        .alias("date_key"),

    F.col("date"),

    F.year("date")
        .alias("year"),

    F.quarter("date")
        .alias("quarter_number"),

    F.concat(
        F.lit("Q"),
        F.quarter("date")
    ).alias("quarter_name"),

    F.concat(
        F.year("date"),
        F.lit("-Q"),
        F.quarter("date")
    ).alias("year_quarter"),

    F.month("date")
        .alias("month_number"),

    F.date_format("date", "MMMM")
        .alias("month_name"),

    F.date_format("date", "yyyy-MM")
        .alias("year_month"),

    F.weekofyear("date")
        .alias("week_of_year"),

    F.dayofmonth("date")
        .alias("day_of_month"),

    F.when(
        F.dayofweek("date") == 1,
        7
    ).otherwise(
        F.dayofweek("date") - 1
    ).alias("day_of_week_number"),

    F.date_format("date", "EEEE")
        .alias("day_name"),

    F.when(
        F.dayofweek("date").isin(1, 7),
        True
    ).otherwise(
        False
    ).alias("is_weekend")
)

print("Date dimension created successfully.")
display(dim_date.orderBy("date").limit(20))


# In[12]:


# Count the rows in the Date dimension
date_row_count = dim_date.count()

# Check for duplicate date keys
duplicate_date_keys = (
    dim_date
    .groupBy("date_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Check for missing keys or dates
missing_date_values = (
    dim_date
    .filter(
        F.col("date_key").isNull() |
        F.col("date").isNull()
    )
    .count()
)

print("Date dimension validation:")
print("Number of rows:", date_row_count)
print("Duplicate date keys:", duplicate_date_keys)
print("Missing date values:", missing_date_values)


# In[13]:


# Save the Date dimension as a Gold Delta table
(
    dim_date
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_date")
)

print("gold.dim_date saved successfully.")


# In[14]:


# Read the saved Date dimension
saved_dim_date = spark.table("gold.dim_date")

print("Gold Date dimension loaded successfully.")
print("Number of Gold rows:", saved_dim_date.count())

display(
    saved_dim_date
    .orderBy("date")
    .limit(20)
)


# In[15]:


# Read the Customers table from the Silver layer
silver_customers = spark.table("silver.customers")

print("Silver customers loaded successfully.")
print("Number of Silver rows:", silver_customers.count())

display(silver_customers.limit(10))


# In[16]:


# Display the structure of the Silver Customers table
silver_customers.printSchema()


# In[17]:


# Compare customer IDs with unique customer IDs
customer_identifier_summary = silver_customers.select(
    F.count("*").alias("number_of_rows"),

    F.countDistinct("customer_id")
        .alias("distinct_customer_ids"),

    F.countDistinct("customer_unique_id")
        .alias("distinct_unique_customer_ids")
)

print("Customer identifier summary:")
display(customer_identifier_summary)


# In[18]:


# Create the Customer dimension
dim_customer = silver_customers.select(
    F.col("customer_id"),

    F.col("customer_unique_id"),

    F.col("customer_zip_code_prefix")
        .cast("int")
        .alias("customer_zip_code_prefix"),

    F.initcap(
        F.trim(F.col("customer_city"))
    ).alias("customer_city"),

    F.upper(
        F.trim(F.col("customer_state"))
    ).alias("customer_state"),

    F.when(
        F.upper(F.trim(F.col("customer_state"))).isin(
            "AC", "AP", "AM", "PA", "RO", "RR", "TO"
        ),
        "North"
    ).when(
        F.upper(F.trim(F.col("customer_state"))).isin(
            "AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"
        ),
        "Northeast"
    ).when(
        F.upper(F.trim(F.col("customer_state"))).isin(
            "DF", "GO", "MT", "MS"
        ),
        "Central-West"
    ).when(
        F.upper(F.trim(F.col("customer_state"))).isin(
            "ES", "MG", "RJ", "SP"
        ),
        "Southeast"
    ).when(
        F.upper(F.trim(F.col("customer_state"))).isin(
            "PR", "RS", "SC"
        ),
        "South"
    ).otherwise(
        "Unknown"
    ).alias("customer_region")
)

print("Customer dimension created successfully.")
display(dim_customer.limit(20))


# In[19]:


# Count the rows in the Customer dimension
customer_row_count = dim_customer.count()

# Count the distinct customer keys
distinct_customer_keys = (
    dim_customer
    .select("customer_id")
    .distinct()
    .count()
)

# Check for duplicate customer keys
duplicate_customer_keys = (
    dim_customer
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Check for missing customer keys
missing_customer_keys = (
    dim_customer
    .filter(F.col("customer_id").isNull())
    .count()
)

print("Customer dimension validation:")
print("Number of rows:", customer_row_count)
print("Distinct customer keys:", distinct_customer_keys)
print("Duplicate customer keys:", duplicate_customer_keys)
print("Missing customer keys:", missing_customer_keys)


# In[20]:


# Check for missing geographical values
missing_customer_geography = dim_customer.select(
    F.sum(
        F.when(F.col("customer_zip_code_prefix").isNull(), 1).otherwise(0)
    ).alias("missing_zip_codes"),

    F.sum(
        F.when(F.col("customer_city").isNull(), 1).otherwise(0)
    ).alias("missing_cities"),

    F.sum(
        F.when(F.col("customer_state").isNull(), 1).otherwise(0)
    ).alias("missing_states"),

    F.sum(
        F.when(F.col("customer_region") == "Unknown", 1).otherwise(0)
    ).alias("unknown_regions")
)

print("Customer geography validation:")
display(missing_customer_geography)


# In[21]:


# Display the number of customer records by region
customers_by_region = (
    dim_customer
    .groupBy("customer_region")
    .agg(
        F.count("*").alias("customer_records"),
        F.countDistinct("customer_unique_id").alias("unique_customers")
    )
    .orderBy(
        F.desc("customer_records")
    )
)

print("Customer distribution by region:")
display(customers_by_region)


# In[22]:


# Save the Customer dimension as a Gold Delta table
(
    dim_customer
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_customer")
)

print("gold.dim_customer saved successfully.")


# In[23]:


# Read the saved Customer dimension
saved_dim_customer = spark.table("gold.dim_customer")

print("Gold Customer dimension loaded successfully.")
print("Number of Gold rows:", saved_dim_customer.count())

display(
    saved_dim_customer
    .orderBy("customer_id")
    .limit(20)
)


# In[1]:


# Read the Products table from the Silver layer
silver_products = spark.table("silver.products")

# Read the Product Category Translation table from the Silver layer
silver_category_translation = spark.table(
    "silver.product_category_translation"
)

# Correct the original Olist spelling if it was not corrected in Silver
silver_products = (
    silver_products
    .withColumnRenamed(
        "product_name_lenght",
        "product_name_length"
    )
    .withColumnRenamed(
        "product_description_lenght",
        "product_description_length"
    )
)

print("Silver product tables loaded successfully.")
print("Number of products:", silver_products.count())
print(
    "Number of category translations:",
    silver_category_translation.count()
)


# In[2]:


# Display the structure of the Products table
print("Products schema:")
silver_products.printSchema()

# Display the structure of the Translation table
print("Category Translation schema:")
silver_category_translation.printSchema()


# In[5]:


# Import PySpark functions
from pyspark.sql import functions as F

print("PySpark functions imported successfully.")


# In[6]:


# Check the uniqueness of the Product key
product_identifier_summary = silver_products.select(
    F.count("*")
        .alias("number_of_rows"),

    F.countDistinct("product_id")
        .alias("distinct_product_ids"),

    F.sum(
        F.when(
            F.col("product_id").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_product_ids")
)

print("Product identifier summary:")
display(product_identifier_summary)


# In[7]:


# Check the uniqueness of the category translation key
translation_summary = silver_category_translation.select(
    F.count("*")
        .alias("number_of_rows"),

    F.countDistinct("product_category_name")
        .alias("distinct_category_names")
)

print("Category translation summary:")
display(translation_summary)


# In[8]:


# Create short aliases for the two source tables
products = silver_products.alias("p")
translations = silver_category_translation.alias("t")

# Join Products with Product Category Translation
dim_product = (
    products
    .join(
        translations,
        F.col("p.product_category_name") ==
        F.col("t.product_category_name"),
        "left"
    )
    .select(
        # Product key
        F.col("p.product_id")
            .alias("product_id"),

        # Portuguese category formatted for reporting
        F.when(
            F.col("p.product_category_name").isNull(),
            "Unknown"
        ).otherwise(
            F.initcap(
                F.regexp_replace(
                    F.col("p.product_category_name"),
                    "_",
                    " "
                )
            )
        ).alias("product_category_portuguese"),

        # English category formatted for reporting
        F.when(
            F.col("p.product_category_name").isNull(),
            "Unknown"
        ).when(
            F.col("t.product_category_name_english").isNull(),
            F.concat(
                F.initcap(
                    F.regexp_replace(
                        F.col("p.product_category_name"),
                        "_",
                        " "
                    )
                ),
                F.lit(" (Untranslated)")
            )
        ).otherwise(
            F.initcap(
                F.regexp_replace(
                    F.col("t.product_category_name_english"),
                    "_",
                    " "
                )
            )
        ).alias("product_category_english"),

        # Translation quality status
        F.when(
            F.col("p.product_category_name").isNull(),
            "Missing category"
        ).when(
            F.col("t.product_category_name_english").isNull(),
            "Untranslated"
        ).otherwise(
            "Translated"
        ).alias("category_translation_status"),

        # Product descriptive attributes
        F.col("p.product_name_length")
            .cast("int")
            .alias("product_name_length"),

        F.col("p.product_description_length")
            .cast("int")
            .alias("product_description_length"),

        F.col("p.product_photos_qty")
            .cast("int")
            .alias("product_photos_qty"),

        # Product measurements
        F.col("p.product_weight_g")
            .cast("double")
            .alias("product_weight_g"),

        F.round(
            F.col("p.product_weight_g") / 1000,
            3
        ).alias("product_weight_kg"),

        F.col("p.product_length_cm")
            .cast("double")
            .alias("product_length_cm"),

        F.col("p.product_height_cm")
            .cast("double")
            .alias("product_height_cm"),

        F.col("p.product_width_cm")
            .cast("double")
            .alias("product_width_cm"),

        # Product volume: length × height × width
        F.round(
            F.col("p.product_length_cm") *
            F.col("p.product_height_cm") *
            F.col("p.product_width_cm"),
            2
        ).alias("product_volume_cm3"),

        # Indicates whether all physical measurements are available
        (
            F.col("p.product_weight_g").isNotNull() &
            F.col("p.product_length_cm").isNotNull() &
            F.col("p.product_height_cm").isNotNull() &
            F.col("p.product_width_cm").isNotNull()
        ).alias("has_complete_measurements")
    )
)

print("Product dimension created successfully.")
display(dim_product.limit(20))


# In[9]:


# Count the rows in the Product dimension
product_row_count = dim_product.count()

# Count the distinct Product keys
distinct_product_keys = (
    dim_product
    .select("product_id")
    .distinct()
    .count()
)

# Check for duplicate Product keys
duplicate_product_keys = (
    dim_product
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Check for missing Product keys
missing_product_keys = (
    dim_product
    .filter(F.col("product_id").isNull())
    .count()
)

print("Product dimension validation:")
print("Number of rows:", product_row_count)
print("Distinct product keys:", distinct_product_keys)
print("Duplicate product keys:", duplicate_product_keys)
print("Missing product keys:", missing_product_keys)


# In[10]:


# Count products by translation status
translation_coverage = (
    dim_product
    .groupBy("category_translation_status")
    .agg(
        F.count("*").alias("number_of_products")
    )
    .orderBy(
        F.desc("number_of_products")
    )
)

print("Category translation coverage:")
display(translation_coverage)


# In[11]:


# Display categories without an English translation
untranslated_categories = (
    dim_product
    .filter(
        F.col("category_translation_status") == "Untranslated"
    )
    .groupBy(
        "product_category_portuguese",
        "product_category_english"
    )
    .agg(
        F.count("*").alias("number_of_products")
    )
    .orderBy(
        F.desc("number_of_products")
    )
)

print("Categories without an English translation:")
display(untranslated_categories)


# In[12]:


# Check the completeness of the product measurements
product_measurement_quality = dim_product.select(
    F.sum(
        F.when(
            F.col("product_weight_g").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_weights"),

    F.sum(
        F.when(
            F.col("product_length_cm").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_lengths"),

    F.sum(
        F.when(
            F.col("product_height_cm").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_heights"),

    F.sum(
        F.when(
            F.col("product_width_cm").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_widths"),

    F.sum(
        F.when(
            F.col("has_complete_measurements") == False,
            1
        ).otherwise(0)
    ).alias("products_with_incomplete_measurements")
)

print("Product measurement quality:")
display(product_measurement_quality)


# In[13]:


# Save the Product dimension as a Gold Delta table
(
    dim_product
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_product")
)

print("gold.dim_product saved successfully.")


# In[14]:


# Read the saved Product dimension
saved_dim_product = spark.table("gold.dim_product")

print("Gold Product dimension loaded successfully.")
print("Number of Gold rows:", saved_dim_product.count())

display(
    saved_dim_product
    .orderBy("product_id")
    .limit(20)
)


# In[15]:


# Read the Sellers table from the Silver layer
silver_sellers = spark.table("silver.sellers")

print("Silver sellers loaded successfully.")
print("Number of Silver rows:", silver_sellers.count())

display(silver_sellers.limit(10))


# In[16]:


# Display the structure of the Silver Sellers table
silver_sellers.printSchema()


# In[17]:


# Check the uniqueness and completeness of the Seller key
seller_identifier_summary = silver_sellers.select(
    F.count("*")
        .alias("number_of_rows"),

    F.countDistinct("seller_id")
        .alias("distinct_seller_ids"),

    F.sum(
        F.when(
            F.col("seller_id").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_seller_ids")
)

print("Seller identifier summary:")
display(seller_identifier_summary)


# In[18]:


# Create the Seller dimension
dim_seller = silver_sellers.select(
    # Seller key
    F.col("seller_id"),

    # Postal code
    F.col("seller_zip_code_prefix")
        .cast("int")
        .alias("seller_zip_code_prefix"),

    # Standardized city name
    F.initcap(
        F.trim(F.col("seller_city"))
    ).alias("seller_city"),

    # Standardized state code
    F.upper(
        F.trim(F.col("seller_state"))
    ).alias("seller_state"),

    # Brazilian geographical region
    F.when(
        F.upper(F.trim(F.col("seller_state"))).isin(
            "AC", "AP", "AM", "PA", "RO", "RR", "TO"
        ),
        "North"
    ).when(
        F.upper(F.trim(F.col("seller_state"))).isin(
            "AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"
        ),
        "Northeast"
    ).when(
        F.upper(F.trim(F.col("seller_state"))).isin(
            "DF", "GO", "MT", "MS"
        ),
        "Central-West"
    ).when(
        F.upper(F.trim(F.col("seller_state"))).isin(
            "ES", "MG", "RJ", "SP"
        ),
        "Southeast"
    ).when(
        F.upper(F.trim(F.col("seller_state"))).isin(
            "PR", "RS", "SC"
        ),
        "South"
    ).otherwise(
        "Unknown"
    ).alias("seller_region")
)

print("Seller dimension created successfully.")
display(dim_seller.limit(20))


# In[19]:


# Count the rows in the Seller dimension
seller_row_count = dim_seller.count()

# Count the distinct Seller keys
distinct_seller_keys = (
    dim_seller
    .select("seller_id")
    .distinct()
    .count()
)

# Check for duplicate Seller keys
duplicate_seller_keys = (
    dim_seller
    .groupBy("seller_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Check for missing Seller keys
missing_seller_keys = (
    dim_seller
    .filter(F.col("seller_id").isNull())
    .count()
)

print("Seller dimension validation:")
print("Number of rows:", seller_row_count)
print("Distinct seller keys:", distinct_seller_keys)
print("Duplicate seller keys:", duplicate_seller_keys)
print("Missing seller keys:", missing_seller_keys)


# In[20]:


# Check for missing Seller geographical values
missing_seller_geography = dim_seller.select(
    F.sum(
        F.when(
            F.col("seller_zip_code_prefix").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_zip_codes"),

    F.sum(
        F.when(
            F.col("seller_city").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_cities"),

    F.sum(
        F.when(
            F.col("seller_state").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_states"),

    F.sum(
        F.when(
            F.col("seller_region") == "Unknown",
            1
        ).otherwise(0)
    ).alias("unknown_regions")
)

print("Seller geography validation:")
display(missing_seller_geography)


# In[21]:


# Display the number of sellers by Brazilian region
sellers_by_region = (
    dim_seller
    .groupBy("seller_region")
    .agg(
        F.count("*").alias("number_of_sellers")
    )
    .orderBy(
        F.desc("number_of_sellers")
    )
)

print("Seller distribution by region:")
display(sellers_by_region)


# In[22]:


# Display the states with the largest number of sellers
sellers_by_state = (
    dim_seller
    .groupBy("seller_state")
    .agg(
        F.count("*").alias("number_of_sellers")
    )
    .orderBy(
        F.desc("number_of_sellers")
    )
)

print("Seller distribution by state:")
display(sellers_by_state)


# In[23]:


# Save the Seller dimension as a Gold Delta table
(
    dim_seller
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_seller")
)

print("gold.dim_seller saved successfully.")


# In[24]:


# Read the saved Seller dimension
saved_dim_seller = spark.table("gold.dim_seller")

print("Gold Seller dimension loaded successfully.")
print("Number of Gold rows:", saved_dim_seller.count())

display(
    saved_dim_seller
    .orderBy("seller_id")
    .limit(20)
)


# In[25]:


# Import PySpark functions
from pyspark.sql import functions as F

# Read the Orders table from the Silver layer
silver_orders = spark.table("silver.orders")

print("Silver orders loaded successfully.")
print("Number of Silver rows:", silver_orders.count())

display(silver_orders.limit(10))


# In[26]:


# Display the structure of the Silver Orders table
silver_orders.printSchema()


# In[27]:


# Check the uniqueness and completeness of the Order key
order_identifier_summary = silver_orders.select(
    F.count("*")
        .alias("number_of_rows"),

    F.countDistinct("order_id")
        .alias("distinct_order_ids"),

    F.countDistinct("customer_id")
        .alias("distinct_customer_ids"),

    F.sum(
        F.when(
            F.col("order_id").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_order_ids"),

    F.sum(
        F.when(
            F.col("customer_id").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_customer_ids")
)

print("Order identifier summary:")
display(order_identifier_summary)


# In[28]:


# Prepare the Orders columns and ensure correct timestamp data types
orders_prepared = silver_orders.select(
    F.col("order_id"),

    F.col("customer_id"),

    F.lower(
        F.trim(F.col("order_status"))
    ).alias("order_status"),

    F.to_timestamp(
        F.col("order_purchase_timestamp")
    ).alias("order_purchase_timestamp"),

    F.to_timestamp(
        F.col("order_approved_at")
    ).alias("order_approved_at"),

    F.to_timestamp(
        F.col("order_delivered_carrier_date")
    ).alias("order_delivered_carrier_date"),

    F.to_timestamp(
        F.col("order_delivered_customer_date")
    ).alias("order_delivered_customer_date"),

    F.to_timestamp(
        F.col("order_estimated_delivery_date")
    ).alias("order_estimated_delivery_date")
)

print("Order timestamps prepared successfully.")
display(orders_prepared.limit(10))


# In[30]:


# Create the Orders fact table
fact_orders = orders_prepared.select(
    # Order and Customer keys
    F.col("order_id"),
    F.col("customer_id"),

    # Degenerate dimension
    F.col("order_status"),

    # Date keys connected to gold.dim_date
    F.date_format(
        F.col("order_purchase_timestamp"),
        "yyyyMMdd"
    ).cast("int").alias("purchase_date_key"),

    F.date_format(
        F.col("order_approved_at"),
        "yyyyMMdd"
    ).cast("int").alias("approved_date_key"),

    F.date_format(
        F.col("order_delivered_carrier_date"),
        "yyyyMMdd"
    ).cast("int").alias("carrier_date_key"),

    F.date_format(
        F.col("order_delivered_customer_date"),
        "yyyyMMdd"
    ).cast("int").alias("delivered_date_key"),

    F.date_format(
        F.col("order_estimated_delivery_date"),
        "yyyyMMdd"
    ).cast("int").alias("estimated_delivery_date_key"),

    # Original timestamps
    F.col("order_purchase_timestamp"),
    F.col("order_approved_at"),
    F.col("order_delivered_carrier_date"),
    F.col("order_delivered_customer_date"),
    F.col("order_estimated_delivery_date"),

    # Time between purchase and approval
    F.round(
        (
            F.col("order_approved_at").cast("long") -
            F.col("order_purchase_timestamp").cast("long")
        ) / 3600,
        2
    ).alias("approval_time_hours"),

    # Calendar days from purchase to carrier
    F.datediff(
        F.col("order_delivered_carrier_date"),
        F.col("order_purchase_timestamp")
    ).alias("purchase_to_carrier_days"),

    # Calendar days from carrier to customer
    F.datediff(
        F.col("order_delivered_customer_date"),
        F.col("order_delivered_carrier_date")
    ).alias("shipping_days"),

    # Total calendar days from purchase to delivery
F.datediff(
    F.col("order_delivered_customer_date"),
    F.col("order_purchase_timestamp")
).alias("delivery_days"),

    # Expected delivery interval
    F.datediff(
        F.col("order_estimated_delivery_date"),
        F.col("order_purchase_timestamp")
    ).alias("estimated_delivery_days"),

    # Positive = late; negative = early
    F.datediff(
        F.col("order_delivered_customer_date"),
        F.col("order_estimated_delivery_date")
    ).alias("delivery_variance_days"),

    # Number of late days, without negative values
    F.when(
        F.col("order_delivered_customer_date").isNull(),
        F.lit(None).cast("int")
    ).otherwise(
        F.greatest(
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_estimated_delivery_date")
            ),
            F.lit(0)
        )
    ).alias("delivery_delay_days"),

    # Delivery performance classification
    F.when(
        F.col("order_delivered_customer_date").isNull(),
        "Not evaluated"
    ).when(
        F.to_date(F.col("order_delivered_customer_date")) >
        F.to_date(F.col("order_estimated_delivery_date")),
        "Late"
    ).otherwise(
        "On time"
    ).alias("delivery_performance_status"),

    # Boolean indicators
    (
        F.col("order_status") == "delivered"
    ).alias("is_delivered_status"),

    (
        F.col("order_status") == "canceled"
    ).alias("is_canceled"),

    F.col(
        "order_delivered_customer_date"
    ).isNotNull().alias("has_delivery_timestamp"),

    F.when(
        F.col("order_delivered_customer_date").isNull(),
        F.lit(None).cast("boolean")
    ).otherwise(
        F.to_date(F.col("order_delivered_customer_date")) >
        F.to_date(F.col("order_estimated_delivery_date"))
    ).alias("is_late_delivery"),

    # Flag incorrect timestamp sequences
    (
        (
            F.col("order_approved_at").isNotNull() &
            (
                F.col("order_approved_at") <
                F.col("order_purchase_timestamp")
            )
        ) |
        (
            F.col("order_delivered_carrier_date").isNotNull() &
            (
                F.col("order_delivered_carrier_date") <
                F.col("order_purchase_timestamp")
            )
        ) |
        (
            F.col("order_delivered_customer_date").isNotNull() &
            F.col("order_delivered_carrier_date").isNotNull() &
            (
                F.col("order_delivered_customer_date") <
                F.col("order_delivered_carrier_date")
            )
        ) |
        (
            F.col("order_delivered_customer_date").isNotNull() &
            (
                F.col("order_delivered_customer_date") <
                F.col("order_purchase_timestamp")
            )
        )
    ).alias("has_timeline_anomaly"),

    # Additive measure for Power BI
    F.lit(1).cast("int").alias("order_count")
)

print("Orders fact table created successfully.")
display(fact_orders.limit(20))


# In[31]:


# Count the rows in the Orders fact table
order_row_count = fact_orders.count()

# Count distinct Order keys
distinct_order_keys = (
    fact_orders
    .select("order_id")
    .distinct()
    .count()
)

# Check for duplicate Order keys
duplicate_order_keys = (
    fact_orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Check required keys
missing_required_keys = (
    fact_orders
    .filter(
        F.col("order_id").isNull() |
        F.col("customer_id").isNull() |
        F.col("purchase_date_key").isNull()
    )
    .count()
)

print("Orders fact table validation:")
print("Number of rows:", order_row_count)
print("Distinct order keys:", distinct_order_keys)
print("Duplicate order keys:", duplicate_order_keys)
print("Rows with missing required keys:", missing_required_keys)


# In[32]:


# Display the number of orders by status
orders_by_status = (
    fact_orders
    .groupBy("order_status")
    .agg(
        F.count("*").alias("number_of_orders")
    )
    .orderBy(
        F.desc("number_of_orders")
    )
)

print("Order distribution by status:")
display(orders_by_status)


# In[33]:


# Check for missing Order timestamps
missing_order_timestamps = fact_orders.select(
    F.sum(
        F.when(
            F.col("order_purchase_timestamp").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_purchase_timestamps"),

    F.sum(
        F.when(
            F.col("order_approved_at").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_approval_timestamps"),

    F.sum(
        F.when(
            F.col("order_delivered_carrier_date").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_carrier_timestamps"),

    F.sum(
        F.when(
            F.col("order_delivered_customer_date").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_delivery_timestamps"),

    F.sum(
        F.when(
            F.col("order_estimated_delivery_date").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_estimated_delivery_timestamps")
)

print("Order timestamp completeness:")
display(missing_order_timestamps)


# In[34]:


# Display the delivery performance distribution
delivery_performance_summary = (
    fact_orders
    .groupBy("delivery_performance_status")
    .agg(
        F.count("*").alias("number_of_orders")
    )
    .orderBy(
        F.desc("number_of_orders")
    )
)

print("Delivery performance summary:")
display(delivery_performance_summary)


# In[35]:


# Count orders with incorrect timestamp sequences
timeline_quality_summary = (
    fact_orders
    .groupBy("has_timeline_anomaly")
    .agg(
        F.count("*").alias("number_of_orders")
    )
    .orderBy(
        F.desc("number_of_orders")
    )
)

print("Order timeline quality summary:")
display(timeline_quality_summary)


# In[36]:


# Read the Customer dimension
gold_dim_customer = spark.table("gold.dim_customer")

# Find Customer keys that do not exist in the dimension
unmatched_customer_keys = (
    fact_orders.alias("f")
    .join(
        gold_dim_customer.alias("d"),
        F.col("f.customer_id") == F.col("d.customer_id"),
        "left_anti"
    )
    .count()
)

print("Foreign key validation:")
print("Unmatched customer keys:", unmatched_customer_keys)


# In[37]:


# Read the Date dimension
gold_dim_date = (
    spark.table("gold.dim_date")
    .select("date_key")
    .distinct()
)

# List of Date foreign keys from the Orders fact table
date_key_columns = [
    "purchase_date_key",
    "approved_date_key",
    "carrier_date_key",
    "delivered_date_key",
    "estimated_delivery_date_key"
]

print("Date foreign key validation:")

for date_key_column in date_key_columns:

    # Select only the available Date keys
    fact_date_keys = (
        fact_orders
        .filter(
            F.col(date_key_column).isNotNull()
        )
        .select(
            F.col(date_key_column).alias("date_key")
        )
        .distinct()
    )

    # Find dates that are missing from dim_date
    unmatched_date_keys = (
        fact_date_keys
        .join(
            gold_dim_date,
            "date_key",
            "left_anti"
        )
        .count()
    )

    print(
        date_key_column,
        "- unmatched keys:",
        unmatched_date_keys
    )


# In[38]:


# Save the Orders fact table as a Gold Delta table
(
    fact_orders
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_orders")
)

print("gold.fact_orders saved successfully.")


# In[39]:


# Read the saved Orders fact table
saved_fact_orders = spark.table("gold.fact_orders")

print("Gold Orders fact table loaded successfully.")
print("Number of Gold rows:", saved_fact_orders.count())

display(
    saved_fact_orders
    .orderBy("order_purchase_timestamp")
    .limit(20)
)


# In[40]:


# Import PySpark functions
from pyspark.sql import functions as F

# Read the Order Items table from the Silver layer
silver_order_items = spark.table("silver.order_items")

print("Silver order items loaded successfully.")
print("Number of Silver rows:", silver_order_items.count())

display(silver_order_items.limit(10))


# In[41]:


# Display the structure of the Silver Order Items table
silver_order_items.printSchema()


# In[42]:


# Check the Order Item source granularity
order_item_identifier_summary = silver_order_items.select(
    F.count("*")
        .alias("number_of_rows"),

    F.countDistinct(
        "order_id",
        "order_item_id"
    ).alias("distinct_order_item_keys"),

    F.countDistinct("order_id")
        .alias("distinct_order_ids"),

    F.countDistinct("product_id")
        .alias("distinct_product_ids"),

    F.countDistinct("seller_id")
        .alias("distinct_seller_ids"),

    F.sum(
        F.when(
            F.col("order_id").isNull() |
            F.col("order_item_id").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_order_item_keys")
)

print("Order Item identifier summary:")
display(order_item_identifier_summary)


# In[43]:


# Prepare and standardize the Order Item columns
order_items_prepared = silver_order_items.select(
    F.col("order_id"),

    F.col("order_item_id")
        .cast("int")
        .alias("order_item_id"),

    F.col("product_id"),

    F.col("seller_id"),

    F.to_timestamp(
        F.col("shipping_limit_date")
    ).alias("shipping_limit_timestamp"),

    F.col("price")
        .cast("decimal(12,2)")
        .alias("item_price"),

    F.col("freight_value")
        .cast("decimal(12,2)")
        .alias("freight_value")
)

print("Order Item columns prepared successfully.")
display(order_items_prepared.limit(10))


# In[44]:


# Convert the Shipping Limit timestamp to a calendar date
shipping_limit_calendar_date = F.to_date(
    F.col("shipping_limit_timestamp")
)

# Check whether the date exists in gold.dim_date
shipping_limit_is_in_date_dimension = (
    shipping_limit_calendar_date.between(
        F.lit("2016-01-01").cast("date"),
        F.lit("2018-12-31").cast("date")
    )
)

# Create the Order Items fact table
fact_order_items = order_items_prepared.select(
    # Composite fact table key
    F.col("order_id"),
    F.col("order_item_id"),

    # Foreign keys
    F.col("product_id"),
    F.col("seller_id"),

    # Date key
    F.when(
        shipping_limit_is_in_date_dimension,
        F.date_format(
            F.col("shipping_limit_timestamp"),
            "yyyyMMdd"
        ).cast("int")
    ).otherwise(
        F.lit(None).cast("int")
    ).alias("shipping_limit_date_key"),

    # Original timestamp
    F.col("shipping_limit_timestamp"),

    # Financial measures
    F.col("item_price"),
    F.col("freight_value"),

    (
        F.col("item_price") +
        F.col("freight_value")
    ).cast("decimal(12,2)").alias("item_total_value"),

    # Data-quality indicator
    (
        ~shipping_limit_is_in_date_dimension
    ).alias("is_shipping_limit_date_out_of_range"),

    # Additive measure
    F.lit(1)
        .cast("int")
        .alias("item_count")
)

print("Order Items fact table created successfully.")
display(fact_order_items.limit(20))


# In[45]:


# Count all rows
order_item_row_count = fact_order_items.count()

# Count distinct composite keys
distinct_order_item_keys = (
    fact_order_items
    .select(
        "order_id",
        "order_item_id"
    )
    .distinct()
    .count()
)

# Check for duplicate composite keys
duplicate_order_item_keys = (
    fact_order_items
    .groupBy(
        "order_id",
        "order_item_id"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

# Check required keys
missing_required_item_keys = (
    fact_order_items
    .filter(
        F.col("order_id").isNull() |
        F.col("order_item_id").isNull() |
        F.col("product_id").isNull() |
        F.col("seller_id").isNull()
    )
    .count()
)

print("Order Items fact table validation:")
print("Number of rows:", order_item_row_count)
print("Distinct composite keys:", distinct_order_item_keys)
print("Duplicate composite keys:", duplicate_order_item_keys)
print("Rows with missing required keys:", missing_required_item_keys)


# In[46]:


# Check the quality of the financial values
financial_value_quality = fact_order_items.select(
    F.sum(
        F.when(
            F.col("item_price").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_item_prices"),

    F.sum(
        F.when(
            F.col("freight_value").isNull(),
            1
        ).otherwise(0)
    ).alias("missing_freight_values"),

    F.sum(
        F.when(
            F.col("item_price") < 0,
            1
        ).otherwise(0)
    ).alias("negative_item_prices"),

    F.sum(
        F.when(
            F.col("freight_value") < 0,
            1
        ).otherwise(0)
    ).alias("negative_freight_values"),

    F.sum(
        F.when(
            F.col("item_price") == 0,
            1
        ).otherwise(0)
    ).alias("zero_item_prices"),

    F.sum(
        F.when(
            F.col("freight_value") == 0,
            1
        ).otherwise(0)
    ).alias("zero_freight_values")
)

print("Financial value quality:")
display(financial_value_quality)


# In[47]:


# Calculate the financial totals
order_item_financial_summary = fact_order_items.select(
    F.sum("item_price")
        .alias("total_item_price"),

    F.sum("freight_value")
        .alias("total_freight_value"),

    F.sum("item_total_value")
        .alias("total_item_value")
)

print("Order Item financial summary:")
display(order_item_financial_summary)


# In[48]:


# Count the number of Order Items in each Order
order_level_item_counts = (
    fact_order_items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("number_of_items")
    )
)

# Summarize the Order Item distribution
order_item_count_summary = order_level_item_counts.select(
    F.count("*")
        .alias("orders_with_items"),

    F.sum(
        F.when(
            F.col("number_of_items") == 1,
            1
        ).otherwise(0)
    ).alias("orders_with_one_item"),

    F.sum(
        F.when(
            F.col("number_of_items") > 1,
            1
        ).otherwise(0)
    ).alias("orders_with_multiple_items"),

    F.max("number_of_items")
        .alias("maximum_items_in_one_order")
)

print("Order Item count summary:")
display(order_item_count_summary)


# In[49]:


# Display the Shipping Limit date quality
shipping_limit_date_quality = (
    fact_order_items
    .groupBy(
        "is_shipping_limit_date_out_of_range"
    )
    .agg(
        F.count("*").alias("number_of_items")
    )
    .orderBy(
        "is_shipping_limit_date_out_of_range"
    )
)

print("Shipping Limit date quality:")
display(shipping_limit_date_quality)


# In[50]:


# Read the Orders fact table
gold_fact_orders = spark.table("gold.fact_orders")

# Display the Shipping Limit date anomalies
shipping_limit_date_anomalies = (
    fact_order_items.alias("i")
    .filter(
        F.col("i.is_shipping_limit_date_out_of_range") == True
    )
    .join(
        gold_fact_orders
        .select(
            "order_id",
            "order_purchase_timestamp"
        )
        .alias("o"),
        F.col("i.order_id") == F.col("o.order_id"),
        "left"
    )
    .select(
        F.col("i.order_id"),
        F.col("i.order_item_id"),
        F.col("o.order_purchase_timestamp"),
        F.col("i.shipping_limit_timestamp"),
        F.col("i.product_id"),
        F.col("i.seller_id")
    )
    .orderBy(
        "shipping_limit_timestamp"
    )
)

print("Shipping Limit date anomalies:")
display(shipping_limit_date_anomalies)


# In[51]:


# Read the required Gold tables
gold_dim_product = spark.table("gold.dim_product")
gold_dim_seller = spark.table("gold.dim_seller")
gold_fact_orders = spark.table("gold.fact_orders")

# Check Order keys
unmatched_order_keys = (
    fact_order_items
    .select("order_id")
    .distinct()
    .join(
        gold_fact_orders
        .select("order_id")
        .distinct(),
        "order_id",
        "left_anti"
    )
    .count()
)

# Check Product keys
unmatched_product_keys = (
    fact_order_items
    .select("product_id")
    .distinct()
    .join(
        gold_dim_product
        .select("product_id")
        .distinct(),
        "product_id",
        "left_anti"
    )
    .count()
)

# Check Seller keys
unmatched_seller_keys = (
    fact_order_items
    .select("seller_id")
    .distinct()
    .join(
        gold_dim_seller
        .select("seller_id")
        .distinct(),
        "seller_id",
        "left_anti"
    )
    .count()
)

print("Foreign key validation:")
print("Unmatched Order keys:", unmatched_order_keys)
print("Unmatched Product keys:", unmatched_product_keys)
print("Unmatched Seller keys:", unmatched_seller_keys)


# In[52]:


# Read the Date dimension
gold_dim_date = (
    spark.table("gold.dim_date")
    .select("date_key")
    .distinct()
)

# Select the available Shipping Limit Date keys
available_shipping_limit_keys = (
    fact_order_items
    .filter(
        F.col("shipping_limit_date_key").isNotNull()
    )
    .select(
        F.col("shipping_limit_date_key")
            .alias("date_key")
    )
    .distinct()
)

# Find Date keys missing from the Date dimension
unmatched_shipping_limit_date_keys = (
    available_shipping_limit_keys
    .join(
        gold_dim_date,
        "date_key",
        "left_anti"
    )
    .count()
)

# Count rows without a valid Date key
missing_shipping_limit_date_keys = (
    fact_order_items
    .filter(
        F.col("shipping_limit_date_key").isNull()
    )
    .count()
)

print("Shipping Limit Date foreign key validation:")
print(
    "Unmatched non-null Date keys:",
    unmatched_shipping_limit_date_keys
)
print(
    "Rows with null Date keys because of anomalies:",
    missing_shipping_limit_date_keys
)


# In[53]:


# Find Orders without any Order Items
orders_without_items = (
    gold_fact_orders.alias("o")
    .join(
        fact_order_items
        .select("order_id")
        .distinct()
        .alias("i"),
        F.col("o.order_id") == F.col("i.order_id"),
        "left_anti"
    )
)

print(
    "Number of Orders without Order Items:",
    orders_without_items.count()
)

display(
    orders_without_items
    .groupBy("order_status")
    .agg(
        F.count("*").alias("number_of_orders")
    )
    .orderBy(
        F.desc("number_of_orders")
    )
)


# In[54]:


# Save the Order Items fact table as a Gold Delta table
(
    fact_order_items
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_order_items")
)

print("gold.fact_order_items saved successfully.")


# In[55]:


# Read the saved Order Items fact table
saved_fact_order_items = spark.table(
    "gold.fact_order_items"
)

print("Gold Order Items fact table loaded successfully.")
print(
    "Number of Gold rows:",
    saved_fact_order_items.count()
)

display(
    saved_fact_order_items
    .orderBy(
        "order_id",
        "order_item_id"
    )
    .limit(20)
)


# In[1]:


# Import PySpark functions
from pyspark.sql import functions as F

# Read the Order Payments table from the Silver layer
silver_order_payments = spark.table(
    "silver.order_payments"
)

print("Silver order payments loaded successfully.")
print(
    "Number of Silver rows:",
    silver_order_payments.count()
)

display(silver_order_payments.limit(10))


# In[2]:


# Display the structure of the Silver Order Payments table
silver_order_payments.printSchema()


# In[3]:


# Check the Order Payment source granularity
payment_identifier_summary = (
    silver_order_payments.select(
        F.count("*")
            .alias("number_of_rows"),

        F.countDistinct(
            "order_id",
            "payment_sequential"
        ).alias("distinct_payment_keys"),

        F.countDistinct("order_id")
            .alias("distinct_order_ids"),

        F.countDistinct("payment_type")
            .alias("distinct_payment_types"),

        F.sum(
            F.when(
                F.col("order_id").isNull() |
                F.col("payment_sequential").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_payment_keys")
    )
)

print("Order Payment identifier summary:")
display(payment_identifier_summary)


# In[4]:


# Prepare and standardize the Order Payment columns
order_payments_prepared = (
    silver_order_payments.select(
        F.col("order_id"),

        F.col("payment_sequential")
            .cast("int")
            .alias("payment_sequential"),

        F.lower(
            F.trim(F.col("payment_type"))
        ).alias("payment_type"),

        F.col("payment_installments")
            .cast("int")
            .alias("payment_installments"),

        F.col("payment_value")
            .cast("decimal(12,2)")
            .alias("payment_value")
    )
)

print("Order Payment columns prepared successfully.")
display(order_payments_prepared.limit(10))


# In[5]:


# Create the Order Payments fact table
fact_order_payments = (
    order_payments_prepared.select(
        # Composite fact table key
        F.col("order_id"),
        F.col("payment_sequential"),

        # Payment attributes
        F.col("payment_type"),
        F.col("payment_installments"),

        # Financial measure
        F.col("payment_value"),

        # True when the payment uses more than one installment
        (
            F.col("payment_installments") > 1
        ).alias("is_installment_payment"),

        # Flag installment counts smaller than one
        (
            F.col("payment_installments") < 1
        ).alias("has_invalid_installment_count"),

        # Flag undefined payment methods
        (
            F.col("payment_type") == "not_defined"
        ).alias("is_undefined_payment_type"),

        # A zero payment is preserved but marked
        (
            F.col("payment_value") == 0
        ).alias("is_zero_value_payment"),

        # Additive measure for Power BI
        F.lit(1)
            .cast("int")
            .alias("payment_count")
    )
)

print("Order Payments fact table created successfully.")
display(fact_order_payments.limit(20))


# In[6]:


# Count all Payment rows
payment_row_count = fact_order_payments.count()

# Count distinct composite keys
distinct_payment_keys = (
    fact_order_payments
    .select(
        "order_id",
        "payment_sequential"
    )
    .distinct()
    .count()
)

# Check for duplicate composite keys
duplicate_payment_keys = (
    fact_order_payments
    .groupBy(
        "order_id",
        "payment_sequential"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

# Check required Payment fields
missing_required_payment_fields = (
    fact_order_payments
    .filter(
        F.col("order_id").isNull() |
        F.col("payment_sequential").isNull() |
        F.col("payment_type").isNull() |
        F.col("payment_installments").isNull() |
        F.col("payment_value").isNull()
    )
    .count()
)

print("Order Payments fact table validation:")
print("Number of rows:", payment_row_count)
print("Distinct composite keys:", distinct_payment_keys)
print("Duplicate composite keys:", duplicate_payment_keys)
print(
    "Rows with missing required fields:",
    missing_required_payment_fields
)


# In[7]:


# Display the Payment Type distribution
payment_type_summary = (
    fact_order_payments
    .groupBy("payment_type")
    .agg(
        F.count("*")
            .alias("number_of_payments"),

        F.sum("payment_value")
            .alias("total_payment_value")
    )
    .orderBy(
        F.desc("number_of_payments")
    )
)

print("Payment Type summary:")
display(payment_type_summary)


# In[8]:


# Check Payment value and installment quality
payment_quality_summary = (
    fact_order_payments.select(
        F.sum(
            F.when(
                F.col("payment_value").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_payment_values"),

        F.sum(
            F.when(
                F.col("payment_value") < 0,
                1
            ).otherwise(0)
        ).alias("negative_payment_values"),

        F.sum(
            F.when(
                F.col("is_zero_value_payment") == True,
                1
            ).otherwise(0)
        ).alias("zero_payment_values"),

        F.sum(
            F.when(
                F.col("has_invalid_installment_count") == True,
                1
            ).otherwise(0)
        ).alias("invalid_installment_counts"),

        F.sum(
            F.when(
                F.col("is_undefined_payment_type") == True,
                1
            ).otherwise(0)
        ).alias("undefined_payment_types"),

        F.max("payment_installments")
            .alias("maximum_installments"),

        F.max("payment_sequential")
            .alias("maximum_payment_sequence")
    )
)

print("Payment quality summary:")
display(payment_quality_summary)


# In[9]:


# Calculate the overall Payment value summary
payment_financial_summary = (
    fact_order_payments.select(
        F.sum("payment_value")
            .alias("total_payment_value"),

        F.min("payment_value")
            .alias("minimum_payment_value"),

        F.max("payment_value")
            .alias("maximum_payment_value")
    )
)

print("Payment financial summary:")
display(payment_financial_summary)


# In[10]:


# Aggregate Payment records at Order level
payments_per_order = (
    fact_order_payments
    .groupBy("order_id")
    .agg(
        F.count("*")
            .alias("number_of_payment_records"),

        F.countDistinct("payment_type")
            .alias("number_of_payment_methods")
    )
)

# Summarize the Order-level Payment distribution
multiple_payment_summary = (
    payments_per_order.select(
        F.count("*")
            .alias("orders_with_payments"),

        F.sum(
            F.when(
                F.col("number_of_payment_records") == 1,
                1
            ).otherwise(0)
        ).alias("orders_with_one_payment_record"),

        F.sum(
            F.when(
                F.col("number_of_payment_records") > 1,
                1
            ).otherwise(0)
        ).alias("orders_with_multiple_payment_records"),

        F.sum(
            F.when(
                F.col("number_of_payment_methods") > 1,
                1
            ).otherwise(0)
        ).alias("orders_with_multiple_payment_methods"),

        F.max("number_of_payment_records")
            .alias("maximum_payment_records_per_order")
    )
)

print("Multiple Payment summary:")
display(multiple_payment_summary)


# In[11]:


# Read the Gold Orders fact table
gold_fact_orders = spark.table("gold.fact_orders")

# Find Payment Order keys missing from fact_orders
unmatched_payment_order_keys = (
    fact_order_payments
    .select("order_id")
    .distinct()
    .join(
        gold_fact_orders
        .select("order_id")
        .distinct(),
        "order_id",
        "left_anti"
    )
    .count()
)

print("Foreign key validation:")
print(
    "Unmatched Payment Order keys:",
    unmatched_payment_order_keys
)


# In[12]:


# Find Orders without Payment records
orders_without_payments = (
    gold_fact_orders.alias("o")
    .join(
        fact_order_payments
        .select("order_id")
        .distinct()
        .alias("p"),
        F.col("o.order_id") == F.col("p.order_id"),
        "left_anti"
    )
)

print(
    "Number of Orders without Payments:",
    orders_without_payments.count()
)

display(
    orders_without_payments.select(
        "order_id",
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date"
    )
)


# In[13]:


# Read the Gold Order Items fact table
gold_fact_order_items = spark.table(
    "gold.fact_order_items"
)

# Calculate the total Payment value for each Order
payments_by_order = (
    fact_order_payments
    .groupBy("order_id")
    .agg(
        F.sum("payment_value")
            .alias("payment_total")
    )
)

# Calculate the total Item value for each Order
items_by_order = (
    gold_fact_order_items
    .groupBy("order_id")
    .agg(
        F.sum("item_total_value")
            .alias("item_total")
    )
)

# Join the two Order-level totals
payment_item_reconciliation = (
    payments_by_order.alias("p")
    .join(
        items_by_order.alias("i"),
        F.col("p.order_id") == F.col("i.order_id"),
        "full"
    )
    .select(
        F.coalesce(
            F.col("p.order_id"),
            F.col("i.order_id")
        ).alias("order_id"),

        F.col("p.payment_total"),
        F.col("i.item_total")
    )
)

# Classify every Order
payment_item_reconciliation = (
    payment_item_reconciliation
    .withColumn(
        "reconciliation_status",
        F.when(
            F.col("payment_total").isNull(),
            "Item only"
        ).when(
            F.col("item_total").isNull(),
            "Payment only"
        ).when(
            F.abs(
                F.col("payment_total") -
                F.col("item_total")
            ) <= F.lit(0.01),
            "Amounts match"
        ).otherwise(
            "Amounts differ"
        )
    )
    .withColumn(
        "value_difference",
        F.coalesce(
            F.col("payment_total"),
            F.lit(0)
        ) -
        F.coalesce(
            F.col("item_total"),
            F.lit(0)
        )
    )
)

# Summarize the reconciliation results
reconciliation_summary = (
    payment_item_reconciliation
    .groupBy("reconciliation_status")
    .agg(
        F.count("*")
            .alias("number_of_orders"),

        F.sum(
            F.coalesce(
                F.col("payment_total"),
                F.lit(0)
            )
        ).alias("total_payment_value"),

        F.sum(
            F.coalesce(
                F.col("item_total"),
                F.lit(0)
            )
        ).alias("total_item_value"),

        F.sum("value_difference")
            .alias("total_difference")
    )
    .orderBy("reconciliation_status")
)

print("Payment and Item reconciliation:")
display(reconciliation_summary)


# In[14]:


# Save the Order Payments fact table as a Gold Delta table
(
    fact_order_payments
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_order_payments")
)

print("gold.fact_order_payments saved successfully.")


# In[15]:


# Read the saved Order Payments fact table
saved_fact_order_payments = spark.table(
    "gold.fact_order_payments"
)

print("Gold Order Payments fact table loaded successfully.")
print(
    "Number of Gold rows:",
    saved_fact_order_payments.count()
)

display(
    saved_fact_order_payments
    .orderBy(
        "order_id",
        "payment_sequential"
    )
    .limit(20)
)


# In[1]:


# Import PySpark functions
from pyspark.sql import functions as F

# Read the Order Reviews table from the Silver layer
silver_order_reviews = spark.table(
    "silver.order_reviews"
)

print("Silver order reviews loaded successfully.")
print(
    "Number of Silver rows:",
    silver_order_reviews.count()
)

display(silver_order_reviews.limit(10))


# In[2]:


# Display the structure of the Silver Order Reviews table
silver_order_reviews.printSchema()


# In[3]:


# Check the Order Review source granularity
review_identifier_summary = (
    silver_order_reviews.select(
        F.count("*")
            .alias("number_of_rows"),

        F.countDistinct(
            "review_id",
            "order_id"
        ).alias("distinct_review_order_keys"),

        F.countDistinct("review_id")
            .alias("distinct_review_ids"),

        F.countDistinct("order_id")
            .alias("distinct_order_ids"),

        F.sum(
            F.when(
                F.col("review_id").isNull() |
                F.col("order_id").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_review_keys")
    )
)

print("Order Review identifier summary:")
display(review_identifier_summary)


# In[4]:


# Prepare and standardize the Order Review columns
order_reviews_prepared = (
    silver_order_reviews.select(
        F.col("review_id"),

        F.col("order_id"),

        F.col("review_score")
            .cast("int")
            .alias("review_score"),

        F.when(
            F.length(
                F.trim(F.col("review_comment_title"))
            ) > 0,
            F.trim(F.col("review_comment_title"))
        ).otherwise(
            F.lit(None).cast("string")
        ).alias("review_comment_title"),

        F.when(
            F.length(
                F.trim(F.col("review_comment_message"))
            ) > 0,
            F.trim(F.col("review_comment_message"))
        ).otherwise(
            F.lit(None).cast("string")
        ).alias("review_comment_message"),

        F.to_date(
            F.col("review_creation_date")
        ).alias("review_creation_date"),

        F.to_timestamp(
            F.col("review_answer_timestamp")
        ).alias("review_answer_timestamp")
    )
)

print("Order Review columns prepared successfully.")
display(order_reviews_prepared.limit(10))


# In[5]:


# Create the Order Reviews fact table
fact_order_reviews = (
    order_reviews_prepared.select(
        # Composite fact table key
        F.col("review_id"),
        F.col("order_id"),

        # Date foreign keys
        F.date_format(
            F.col("review_creation_date"),
            "yyyyMMdd"
        ).cast("int").alias(
            "review_creation_date_key"
        ),

        F.date_format(
            F.to_date(
                F.col("review_answer_timestamp")
            ),
            "yyyyMMdd"
        ).cast("int").alias(
            "review_answer_date_key"
        ),

        # Original dates
        F.col("review_creation_date"),
        F.col("review_answer_timestamp"),

        # Review score
        F.col("review_score"),

        # Review classification
        F.when(
            F.col("review_score").isin(1, 2),
            "Negative"
        ).when(
            F.col("review_score") == 3,
            "Neutral"
        ).when(
            F.col("review_score").isin(4, 5),
            "Positive"
        ).otherwise(
            "Invalid"
        ).alias("satisfaction_category"),

        # Written feedback
        F.col("review_comment_title"),
        F.col("review_comment_message"),

        F.col("review_comment_title")
            .isNotNull()
            .alias("has_comment_title"),

        F.col("review_comment_message")
            .isNotNull()
            .alias("has_comment_message"),

        (
            F.col("review_comment_title").isNotNull() |
            F.col("review_comment_message").isNotNull()
        ).alias("has_written_feedback"),

        # Calendar days until the customer answered
        F.datediff(
            F.to_date(
                F.col("review_answer_timestamp")
            ),
            F.col("review_creation_date")
        ).alias("review_response_days"),

        # Long response indicator
        (
            F.datediff(
                F.to_date(
                    F.col("review_answer_timestamp")
                ),
                F.col("review_creation_date")
            ) > 30
        ).alias("is_response_over_30_days"),

        # Additive measure
        F.lit(1)
            .cast("int")
            .alias("review_count")
    )
)

print("Order Reviews fact table created successfully.")
display(fact_order_reviews.limit(20))


# In[6]:


# Count all Review rows
review_row_count = fact_order_reviews.count()

# Count distinct composite keys
distinct_review_order_keys = (
    fact_order_reviews
    .select(
        "review_id",
        "order_id"
    )
    .distinct()
    .count()
)

# Check for duplicate composite keys
duplicate_review_order_keys = (
    fact_order_reviews
    .groupBy(
        "review_id",
        "order_id"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

# Check required Review fields
missing_required_review_fields = (
    fact_order_reviews
    .filter(
        F.col("review_id").isNull() |
        F.col("order_id").isNull() |
        F.col("review_score").isNull() |
        F.col("review_creation_date").isNull() |
        F.col("review_answer_timestamp").isNull()
    )
    .count()
)

print("Order Reviews fact table validation:")
print("Number of rows:", review_row_count)
print(
    "Distinct composite keys:",
    distinct_review_order_keys
)
print(
    "Duplicate composite keys:",
    duplicate_review_order_keys
)
print(
    "Rows with missing required fields:",
    missing_required_review_fields
)


# In[7]:


# Find Review IDs used in more than one record
reused_review_ids = (
    fact_order_reviews
    .groupBy("review_id")
    .agg(
        F.count("*")
            .alias("number_of_review_records"),

        F.countDistinct("order_id")
            .alias("number_of_orders")
    )
    .filter(
        F.col("number_of_review_records") > 1
    )
)

# Summarize reused Review IDs
reused_review_id_summary = (
    reused_review_ids.select(
        F.count("*")
            .alias("reused_review_ids"),

        F.sum("number_of_review_records")
            .alias("affected_review_records"),

        F.max("number_of_review_records")
            .alias("maximum_records_per_review_id")
    )
)

print("Reused Review ID summary:")
display(reused_review_id_summary)


# In[8]:


# Display the Review Score distribution
review_score_summary = (
    fact_order_reviews
    .groupBy(
        "review_score",
        "satisfaction_category"
    )
    .agg(
        F.count("*")
            .alias("number_of_reviews")
    )
    .orderBy("review_score")
)

print("Review Score summary:")
display(review_score_summary)

# Calculate the average Review Score
average_review_score = (
    fact_order_reviews.select(
        F.round(
            F.avg("review_score"),
            4
        ).alias("average_review_score")
    )
)

print("Average Review Score:")
display(average_review_score)


# In[9]:


# Summarize written Review feedback
review_comment_summary = (
    fact_order_reviews.select(
        F.sum(
            F.when(
                F.col("has_comment_title") == True,
                1
            ).otherwise(0)
        ).alias("reviews_with_title"),

        F.sum(
            F.when(
                F.col("has_comment_message") == True,
                1
            ).otherwise(0)
        ).alias("reviews_with_message"),

        F.sum(
            F.when(
                F.col("has_written_feedback") == True,
                1
            ).otherwise(0)
        ).alias("reviews_with_written_feedback"),

        F.sum(
            F.when(
                F.col("has_written_feedback") == False,
                1
            ).otherwise(0)
        ).alias("reviews_without_written_feedback"),

        F.sum(
            F.when(
                F.col("has_comment_title") &
                F.col("has_comment_message"),
                1
            ).otherwise(0)
        ).alias("reviews_with_title_and_message")
    )
)

print("Review Comment summary:")
display(review_comment_summary)


# In[10]:


# Check the Review response time
review_response_summary = (
    fact_order_reviews.select(
        F.min("review_response_days")
            .alias("minimum_response_days"),

        F.max("review_response_days")
            .alias("maximum_response_days"),

        F.round(
            F.avg("review_response_days"),
            4
        ).alias("average_response_days"),

        F.sum(
            F.when(
                F.col("review_response_days") < 0,
                1
            ).otherwise(0)
        ).alias("answers_before_creation"),

        F.sum(
            F.when(
                F.col("is_response_over_30_days") == True,
                1
            ).otherwise(0)
        ).alias("responses_over_30_days")
    )
)

print("Review response summary:")
display(review_response_summary)


# In[11]:


# Count Review records for each Order
reviews_per_order = (
    fact_order_reviews
    .groupBy("order_id")
    .agg(
        F.count("*")
            .alias("number_of_reviews")
    )
)

# Summarize Reviews at Order level
reviews_per_order_summary = (
    reviews_per_order.select(
        F.count("*")
            .alias("orders_with_reviews"),

        F.sum(
            F.when(
                F.col("number_of_reviews") == 1,
                1
            ).otherwise(0)
        ).alias("orders_with_one_review"),

        F.sum(
            F.when(
                F.col("number_of_reviews") > 1,
                1
            ).otherwise(0)
        ).alias("orders_with_multiple_reviews"),

        F.max("number_of_reviews")
            .alias("maximum_reviews_per_order")
    )
)

print("Reviews per Order summary:")
display(reviews_per_order_summary)


# In[12]:


# Read the required Gold tables
gold_fact_orders = spark.table(
    "gold.fact_orders"
)

gold_dim_date = (
    spark.table("gold.dim_date")
    .select("date_key")
    .distinct()
)

# Check Order keys
unmatched_review_order_keys = (
    fact_order_reviews
    .select("order_id")
    .distinct()
    .join(
        gold_fact_orders
        .select("order_id")
        .distinct(),
        "order_id",
        "left_anti"
    )
    .count()
)

print("Unmatched Review Order keys:", unmatched_review_order_keys)

# List of Date foreign keys
review_date_key_columns = [
    "review_creation_date_key",
    "review_answer_date_key"
]

print("Review Date foreign key validation:")

for date_key_column in review_date_key_columns:

    available_review_date_keys = (
        fact_order_reviews
        .filter(
            F.col(date_key_column).isNotNull()
        )
        .select(
            F.col(date_key_column).alias("date_key")
        )
        .distinct()
    )

    unmatched_review_date_keys = (
        available_review_date_keys
        .join(
            gold_dim_date,
            "date_key",
            "left_anti"
        )
        .count()
    )

    print(
        date_key_column,
        "- unmatched keys:",
        unmatched_review_date_keys
    )


# In[13]:


# Find Orders without Review records
orders_without_reviews = (
    gold_fact_orders
    .join(
        fact_order_reviews
        .select("order_id")
        .distinct(),
        "order_id",
        "left_anti"
    )
)

print(
    "Number of Orders without Reviews:",
    orders_without_reviews.count()
)

display(
    orders_without_reviews
    .groupBy("order_status")
    .agg(
        F.count("*")
            .alias("number_of_orders")
    )
    .orderBy(
        F.desc("number_of_orders")
    )
)


# In[14]:


# Save the Order Reviews fact table as a Gold Delta table
(
    fact_order_reviews
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_order_reviews")
)

print("gold.fact_order_reviews saved successfully.")


# In[15]:


# Read the saved Order Reviews fact table
saved_fact_order_reviews = spark.table(
    "gold.fact_order_reviews"
)

print("Gold Order Reviews fact table loaded successfully.")
print(
    "Number of Gold rows:",
    saved_fact_order_reviews.count()
)

display(
    saved_fact_order_reviews
    .orderBy(
        "order_id",
        "review_id"
    )
    .limit(20)
)


# In[17]:


# Display all tables available in the Gold schema
gold_tables_list = (
    spark.sql("SHOW TABLES IN gold")
    .select(
        "tableName",
        "isTemporary"
    )
    .orderBy("tableName")
)

print("Tables available in the Gold layer:")
display(gold_tables_list)


# In[18]:


# Import PySpark functions
from pyspark.sql import functions as F

# Read all Gold dimension tables
gold_dim_customer = spark.table("gold.dim_customer")
gold_dim_product = spark.table("gold.dim_product")
gold_dim_seller = spark.table("gold.dim_seller")
gold_dim_date = spark.table("gold.dim_date")

# Read all Gold fact tables
gold_fact_orders = spark.table("gold.fact_orders")
gold_fact_order_items = spark.table("gold.fact_order_items")
gold_fact_order_payments = spark.table(
    "gold.fact_order_payments"
)
gold_fact_order_reviews = spark.table(
    "gold.fact_order_reviews"
)

print("All Gold tables loaded successfully.")


# In[19]:


# Create a list containing all Gold tables
gold_tables = [
    ("dim_customer", gold_dim_customer),
    ("dim_product", gold_dim_product),
    ("dim_seller", gold_dim_seller),
    ("dim_date", gold_dim_date),
    ("fact_orders", gold_fact_orders),
    ("fact_order_items", gold_fact_order_items),
    ("fact_order_payments", gold_fact_order_payments),
    ("fact_order_reviews", gold_fact_order_reviews)
]

# Store the table statistics
gold_table_statistics = []

for table_name, table_dataframe in gold_tables:

    number_of_rows = table_dataframe.count()
    number_of_columns = len(table_dataframe.columns)

    gold_table_statistics.append(
        (
            table_name,
            number_of_rows,
            number_of_columns
        )
    )

# Create a DataFrame with the statistics
gold_table_summary = spark.createDataFrame(
    gold_table_statistics,
    [
        "table_name",
        "number_of_rows",
        "number_of_columns"
    ]
)

print("Gold table summary:")
display(
    gold_table_summary.orderBy("table_name")
)


# In[20]:


# Create a reusable function for validating table keys
def validate_table_key(
    table_name,
    table_dataframe,
    key_columns
):

    # Count all rows
    number_of_rows = table_dataframe.count()

    # Count distinct key combinations
    number_of_distinct_keys = (
        table_dataframe
        .select(*key_columns)
        .distinct()
        .count()
    )

    # Start the missing key condition
    missing_key_condition = (
        F.col(key_columns[0]).isNull()
    )

    # Add the remaining key columns
    for key_column in key_columns[1:]:

        missing_key_condition = (
            missing_key_condition |
            F.col(key_column).isNull()
        )

    # Count rows containing missing keys
    missing_key_rows = (
        table_dataframe
        .filter(missing_key_condition)
        .count()
    )

    # Calculate the number of duplicate rows
    duplicate_key_rows = (
        number_of_rows -
        number_of_distinct_keys
    )

    # Determine the validation status
    if (
        duplicate_key_rows == 0 and
        missing_key_rows == 0
    ):
        validation_status = "PASS"
    else:
        validation_status = "FAIL"

    return (
        table_name,
        " + ".join(key_columns),
        number_of_rows,
        number_of_distinct_keys,
        duplicate_key_rows,
        missing_key_rows,
        validation_status
    )


print("Key validation function created successfully.")


# In[21]:


# Validate the keys of all Gold tables
key_validation_results = [

    validate_table_key(
        "gold.dim_customer",
        gold_dim_customer,
        ["customer_id"]
    ),

    validate_table_key(
        "gold.dim_product",
        gold_dim_product,
        ["product_id"]
    ),

    validate_table_key(
        "gold.dim_seller",
        gold_dim_seller,
        ["seller_id"]
    ),

    validate_table_key(
        "gold.dim_date",
        gold_dim_date,
        ["date_key"]
    ),

    validate_table_key(
        "gold.fact_orders",
        gold_fact_orders,
        ["order_id"]
    ),

    validate_table_key(
        "gold.fact_order_items",
        gold_fact_order_items,
        [
            "order_id",
            "order_item_id"
        ]
    ),

    validate_table_key(
        "gold.fact_order_payments",
        gold_fact_order_payments,
        [
            "order_id",
            "payment_sequential"
        ]
    ),

    validate_table_key(
        "gold.fact_order_reviews",
        gold_fact_order_reviews,
        [
            "review_id",
            "order_id"
        ]
    )
]

# Convert the results into a DataFrame
gold_key_validation = spark.createDataFrame(
    key_validation_results,
    [
        "table_name",
        "key_columns",
        "number_of_rows",
        "number_of_distinct_keys",
        "duplicate_key_rows",
        "missing_key_rows",
        "validation_status"
    ]
)

print("Gold key validation results:")
display(
    gold_key_validation.orderBy("table_name")
)


# In[22]:


# Create a reusable function for validating relationships
def count_unmatched_keys(
    child_dataframe,
    child_key,
    parent_dataframe,
    parent_key
):

    # Select the available child keys
    child_keys = (
        child_dataframe
        .filter(
            F.col(child_key).isNotNull()
        )
        .select(
            F.col(child_key).alias("key_value")
        )
        .distinct()
    )

    # Select the available parent keys
    parent_keys = (
        parent_dataframe
        .filter(
            F.col(parent_key).isNotNull()
        )
        .select(
            F.col(parent_key).alias("key_value")
        )
        .distinct()
    )

    # Find child keys missing from the parent table
    unmatched_keys = (
        child_keys
        .join(
            parent_keys,
            "key_value",
            "left_anti"
        )
        .count()
    )

    return unmatched_keys


print(
    "Foreign key validation function created successfully."
)


# In[23]:


# Define the main Gold relationships
gold_relationships = [

    (
        "fact_orders.customer_id -> dim_customer.customer_id",
        gold_fact_orders,
        "customer_id",
        gold_dim_customer,
        "customer_id"
    ),

    (
        "fact_order_items.order_id -> fact_orders.order_id",
        gold_fact_order_items,
        "order_id",
        gold_fact_orders,
        "order_id"
    ),

    (
        "fact_order_items.product_id -> dim_product.product_id",
        gold_fact_order_items,
        "product_id",
        gold_dim_product,
        "product_id"
    ),

    (
        "fact_order_items.seller_id -> dim_seller.seller_id",
        gold_fact_order_items,
        "seller_id",
        gold_dim_seller,
        "seller_id"
    ),

    (
        "fact_order_payments.order_id -> fact_orders.order_id",
        gold_fact_order_payments,
        "order_id",
        gold_fact_orders,
        "order_id"
    ),

    (
        "fact_order_reviews.order_id -> fact_orders.order_id",
        gold_fact_order_reviews,
        "order_id",
        gold_fact_orders,
        "order_id"
    )
]

# Store the validation results
relationship_validation_results = []

for (
    relationship_name,
    child_dataframe,
    child_key,
    parent_dataframe,
    parent_key
) in gold_relationships:

    unmatched_keys = count_unmatched_keys(
        child_dataframe,
        child_key,
        parent_dataframe,
        parent_key
    )

    if unmatched_keys == 0:
        validation_status = "PASS"
    else:
        validation_status = "FAIL"

    relationship_validation_results.append(
        (
            relationship_name,
            unmatched_keys,
            validation_status
        )
    )

# Create the relationship validation DataFrame
gold_relationship_validation = spark.createDataFrame(
    relationship_validation_results,
    [
        "relationship_name",
        "unmatched_keys",
        "validation_status"
    ]
)

print("Gold relationship validation results:")
display(gold_relationship_validation)


# In[26]:


print("Date key columns from fact_orders:")

for column_name in gold_fact_orders.columns:
    if column_name.endswith("_date_key"):
        print(column_name)


# In[28]:


# Define all relationships with the Date dimension
gold_date_relationships = [

    (
        "fact_orders.purchase_date_key",
        gold_fact_orders,
        "purchase_date_key"
    ),

    (
        "fact_orders.approved_date_key",
        gold_fact_orders,
        "approved_date_key"
    ),

    (
        "fact_orders.carrier_date_key",
        gold_fact_orders,
        "carrier_date_key"
    ),

    (
        "fact_orders.delivered_date_key",
        gold_fact_orders,
        "delivered_date_key"
    ),

    (
        "fact_orders.estimated_delivery_date_key",
        gold_fact_orders,
        "estimated_delivery_date_key"
    ),

    (
        "fact_order_items.shipping_limit_date_key",
        gold_fact_order_items,
        "shipping_limit_date_key"
    ),

    (
        "fact_order_reviews.review_creation_date_key",
        gold_fact_order_reviews,
        "review_creation_date_key"
    ),

    (
        "fact_order_reviews.review_answer_date_key",
        gold_fact_order_reviews,
        "review_answer_date_key"
    )
]


# Create an empty list for storing the validation results
date_relationship_results = []


# Validate every relationship with the Date dimension
for (
    relationship_name,
    child_dataframe,
    child_date_key
) in gold_date_relationships:

    # Count non-null Date keys that do not exist in dim_date
    unmatched_keys = count_unmatched_keys(
        child_dataframe,
        child_date_key,
        gold_dim_date,
        "date_key"
    )

    # Count rows where the Date foreign key is null
    null_date_keys = (
        child_dataframe
        .filter(
            F.col(child_date_key).isNull()
        )
        .count()
    )

    # Determine the validation status
    if unmatched_keys == 0:
        validation_status = "PASS"
    else:
        validation_status = "FAIL"

    # Add the result to the list
    date_relationship_results.append(
        (
            relationship_name,
            unmatched_keys,
            null_date_keys,
            validation_status
        )
    )


# Convert the validation results into a DataFrame
gold_date_relationship_validation = (
    spark.createDataFrame(
        date_relationship_results,
        [
            "relationship_name",
            "unmatched_non_null_keys",
            "null_date_keys",
            "validation_status"
        ]
    )
)


# Display the validation results
print("Gold Date relationship validation:")

display(
    gold_date_relationship_validation
    .orderBy("relationship_name")
)


# In[29]:


# Define the Silver and Gold table pairs
silver_gold_table_pairs = [

    (
        "silver.customers",
        "gold.dim_customer"
    ),

    (
        "silver.products",
        "gold.dim_product"
    ),

    (
        "silver.sellers",
        "gold.dim_seller"
    ),

    (
        "silver.orders",
        "gold.fact_orders"
    ),

    (
        "silver.order_items",
        "gold.fact_order_items"
    ),

    (
        "silver.order_payments",
        "gold.fact_order_payments"
    ),

    (
        "silver.order_reviews",
        "gold.fact_order_reviews"
    )
]

# Store the row count comparisons
row_count_results = []

for silver_table, gold_table in silver_gold_table_pairs:

    silver_row_count = (
        spark.table(silver_table).count()
    )

    gold_row_count = (
        spark.table(gold_table).count()
    )

    row_count_difference = (
        gold_row_count - silver_row_count
    )

    if row_count_difference == 0:
        validation_status = "PASS"
    else:
        validation_status = "FAIL"

    row_count_results.append(
        (
            silver_table,
            gold_table,
            silver_row_count,
            gold_row_count,
            row_count_difference,
            validation_status
        )
    )

# Create the reconciliation DataFrame
silver_gold_row_validation = spark.createDataFrame(
    row_count_results,
    [
        "silver_table",
        "gold_table",
        "silver_rows",
        "gold_rows",
        "row_count_difference",
        "validation_status"
    ]
)

print("Silver and Gold row count reconciliation:")
display(silver_gold_row_validation)


# In[30]:


# Add the validation timestamp
gold_table_summary_to_save = (
    gold_table_summary
    .withColumn(
        "validation_timestamp",
        F.current_timestamp()
    )
)

gold_key_validation_to_save = (
    gold_key_validation
    .withColumn(
        "validation_timestamp",
        F.current_timestamp()
    )
)

gold_relationship_validation_to_save = (
    gold_relationship_validation
    .withColumn(
        "validation_timestamp",
        F.current_timestamp()
    )
)

gold_date_relationship_validation_to_save = (
    gold_date_relationship_validation
    .withColumn(
        "validation_timestamp",
        F.current_timestamp()
    )
)

silver_gold_row_validation_to_save = (
    silver_gold_row_validation
    .withColumn(
        "validation_timestamp",
        F.current_timestamp()
    )
)

# Save the Gold table summary
(
    gold_table_summary_to_save
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("audit.gold_table_summary")
)

# Save the key validation results
(
    gold_key_validation_to_save
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("audit.gold_key_validation")
)

# Save the main relationship results
(
    gold_relationship_validation_to_save
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "audit.gold_relationship_validation"
    )
)

# Save the Date relationship results
(
    gold_date_relationship_validation_to_save
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "audit.gold_date_relationship_validation"
    )
)

# Save the Silver and Gold reconciliation
(
    silver_gold_row_validation_to_save
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "audit.silver_gold_row_validation"
    )
)

print("Gold validation results saved successfully.")


# In[31]:


# Count failed key validations
failed_key_validations = (
    gold_key_validation
    .filter(
        F.col("validation_status") == "FAIL"
    )
    .count()
)

# Count failed main relationships
failed_relationship_validations = (
    gold_relationship_validation
    .filter(
        F.col("validation_status") == "FAIL"
    )
    .count()
)

# Count failed Date relationships
failed_date_relationship_validations = (
    gold_date_relationship_validation
    .filter(
        F.col("validation_status") == "FAIL"
    )
    .count()
)

# Count Silver and Gold row count differences
failed_row_count_validations = (
    silver_gold_row_validation
    .filter(
        F.col("validation_status") == "FAIL"
    )
    .count()
)

# Calculate the total number of failed validations
total_failed_validations = (
    failed_key_validations +
    failed_relationship_validations +
    failed_date_relationship_validations +
    failed_row_count_validations
)

print("Final Gold layer validation:")
print(
    "Failed key validations:",
    failed_key_validations
)
print(
    "Failed relationship validations:",
    failed_relationship_validations
)
print(
    "Failed Date relationship validations:",
    failed_date_relationship_validations
)
print(
    "Failed row count validations:",
    failed_row_count_validations
)
print(
    "Total failed validations:",
    total_failed_validations
)

# Display the final validation result
if total_failed_validations == 0:

    print(
        "GOLD LAYER VALIDATION PASSED SUCCESSFULLY."
    )

else:

    print(
        "GOLD LAYER VALIDATION FAILED."
    )


# In[1]:


print("SILVER SELLERS:")
print(spark.table("silver.sellers").columns)

print("\nSILVER ORDER ITEMS:")
print(spark.table("silver.order_items").columns)

print("\nGOLD DIM SELLER:")
print(spark.table("gold.dim_seller").columns)

print("\nGOLD FACT ORDER ITEMS:")
print(spark.table("gold.fact_order_items").columns)


# In[1]:


# Security mapping table for Dynamic RLS

security_data = [
    ("cristian.diaconescu00@e-uvt.ro", "SP")
]

security_columns = [
    "user_email",
    "seller_state"
]

security_user_region = spark.createDataFrame(
    security_data,
    security_columns
)

display(security_user_region)


# In[2]:


security_user_region.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.security_user_region")

print("Security table created successfully.")


# In[3]:


display(
    spark.table("gold.security_user_region")
)


# In[1]:


# Create Seller State dimension

dim_seller_state = (
    spark.table("gold.dim_seller")
    .select("seller_state")
    .where("seller_state IS NOT NULL")
    .distinct()
    .orderBy("seller_state")
)

print("Number of seller states:", dim_seller_state.count())

display(dim_seller_state)


# In[2]:


# Save Seller State dimension to Gold layer

dim_seller_state.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.dim_seller_state")

print("dim_seller_state created successfully.")


# In[3]:


display(
    spark.table("gold.dim_seller_state")
)

