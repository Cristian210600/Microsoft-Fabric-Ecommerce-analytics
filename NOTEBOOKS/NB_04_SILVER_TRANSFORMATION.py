#!/usr/bin/env python
# coding: utf-8

# ## NB_04_SILVER_TRANSFORMATION
# 
# null

# In[1]:


# Type here in the cell editor to add code!
# Create the Silver schema if it does not already exist
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS silver
""")

print("Silver schema created successfully.")


# In[2]:


# Display all tables available in the Bronze schema
bronze_tables = spark.sql("SHOW TABLES IN bronze")

display(bronze_tables)


# In[3]:


# Import the PySpark functions
from pyspark.sql import functions as F

# Read the Customers table from the Bronze layer
bronze_customers = spark.table("bronze.customers")

print("Bronze customers loaded successfully.")
print("Number of Bronze rows:", bronze_customers.count())

display(bronze_customers.limit(10))


# In[4]:


# Clean and standardize the Customers data
silver_customers = (
    bronze_customers

    # Remove rows without a customer ID
    .filter(F.col("customer_id").isNotNull())

    # Remove unnecessary spaces
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("customer_unique_id", F.trim(F.col("customer_unique_id")))
    .withColumn("customer_city", F.trim(F.col("customer_city")))
    .withColumn("customer_state", F.trim(F.col("customer_state")))

    # Standardize text values
    .withColumn("customer_city", F.lower(F.col("customer_city")))
    .withColumn("customer_state", F.upper(F.col("customer_state")))

    # Convert the ZIP code to a five-character text value
    .withColumn(
        "customer_zip_code_prefix",
        F.lpad(
            F.col("customer_zip_code_prefix").cast("string"),
            5,
            "0"
        )
    )

    # Remove duplicate customer records
    .dropDuplicates(["customer_id"])
)

print("Customer data cleaned successfully.")
print("Number of Silver rows:", silver_customers.count())

display(silver_customers.limit(10))


# In[5]:


# Save the cleaned data as a Silver Delta table
(
    silver_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.customers")
)

print("silver.customers created successfully.")


# In[6]:


# Read the saved Silver table
saved_customers = spark.table("silver.customers")

bronze_count = bronze_customers.count()
silver_count = saved_customers.count()

print("Bronze row count:", bronze_count)
print("Silver row count:", silver_count)
print("Removed rows:", bronze_count - silver_count)

print("Silver table schema:")
saved_customers.printSchema()

display(saved_customers.limit(10))


# In[7]:


# Read the Orders table from the Bronze layer
bronze_orders = spark.table("bronze.orders")

print("Bronze orders loaded successfully.")
print("Number of Bronze rows:", bronze_orders.count())

display(bronze_orders.limit(10))


# In[8]:


# Clean and standardize the Orders data
silver_orders = (
    bronze_orders

    # Remove unnecessary spaces from text columns
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("order_status", F.trim(F.col("order_status")))

    # Standardize the order status
    .withColumn("order_status", F.lower(F.col("order_status")))

    # Remove rows without valid IDs
    .filter(
        F.col("order_id").isNotNull() &
        (F.length(F.col("order_id")) > 0)
    )
    .filter(
        F.col("customer_id").isNotNull() &
        (F.length(F.col("customer_id")) > 0)
    )

    # Convert text columns to timestamp
    .withColumn(
        "order_purchase_timestamp",
        F.to_timestamp(
            F.col("order_purchase_timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn(
        "order_approved_at",
        F.to_timestamp(
            F.col("order_approved_at"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn(
        "order_delivered_carrier_date",
        F.to_timestamp(
            F.col("order_delivered_carrier_date"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn(
        "order_delivered_customer_date",
        F.to_timestamp(
            F.col("order_delivered_customer_date"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn(
        "order_estimated_delivery_date",
        F.to_timestamp(
            F.col("order_estimated_delivery_date"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    # Remove duplicate orders
    .dropDuplicates(["order_id"])
)

print("Order data cleaned successfully.")
print("Number of Silver rows:", silver_orders.count())

display(silver_orders.limit(10))


# In[9]:


# Save the cleaned data as a Silver Delta table
(
    silver_orders.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.orders")
)

print("silver.orders created successfully.")


# In[10]:


# Read the saved Silver table
saved_orders = spark.table("silver.orders")

bronze_count = bronze_orders.count()
silver_count = saved_orders.count()

print("Bronze row count:", bronze_count)
print("Silver row count:", silver_count)
print("Removed rows:", bronze_count - silver_count)

print("Silver Orders schema:")
saved_orders.printSchema()

print("Order statuses:")
saved_orders.groupBy("order_status").count().orderBy(
    F.desc("count")
).show()

display(saved_orders.limit(10))


# In[11]:


# Read the Order Items table from the Bronze layer
bronze_order_items = spark.table("bronze.order_items")

print("Bronze order items loaded successfully.")
print("Number of Bronze rows:", bronze_order_items.count())

display(bronze_order_items.limit(10))



# In[12]:


# Clean and standardize the Order Items data
silver_order_items = (
    bronze_order_items

    # Remove unnecessary spaces from ID columns
    .withColumn("order_id", F.trim(F.col("order_id")))
    .withColumn("product_id", F.trim(F.col("product_id")))
    .withColumn("seller_id", F.trim(F.col("seller_id")))

    # Convert columns to the correct data types
    .withColumn(
        "order_item_id",
        F.col("order_item_id").cast("integer")
    )
    .withColumn(
        "shipping_limit_date",
        F.to_timestamp(
            F.col("shipping_limit_date"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .withColumn(
        "price",
        F.col("price").cast("decimal(12,2)")
    )
    .withColumn(
        "freight_value",
        F.col("freight_value").cast("decimal(12,2)")
    )

    # Remove rows without valid IDs
    .filter(
        F.col("order_id").isNotNull() &
        (F.length(F.col("order_id")) > 0)
    )
    .filter(
        F.col("product_id").isNotNull() &
        (F.length(F.col("product_id")) > 0)
    )
    .filter(
        F.col("seller_id").isNotNull() &
        (F.length(F.col("seller_id")) > 0)
    )

    # Keep only valid item numbers and financial values
    .filter(F.col("order_item_id") > 0)
    .filter(F.col("price") >= 0)
    .filter(F.col("freight_value") >= 0)

    # Remove duplicates using the compound business key
    .dropDuplicates(["order_id", "order_item_id"])
)

print("Order items cleaned successfully.")
print("Number of Silver rows:", silver_order_items.count())

display(silver_order_items.limit(10))


# In[13]:


# Save the cleaned data as a Silver Delta table
(
    silver_order_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.order_items")
)

print("silver.order_items created successfully.")


# In[14]:


# Read the saved Silver table
saved_order_items = spark.table("silver.order_items")

bronze_count = bronze_order_items.count()
silver_count = saved_order_items.count()

# Count duplicate compound keys
duplicate_count = (
    saved_order_items
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Count invalid financial values
invalid_financial_values = (
    saved_order_items
    .filter(
        (F.col("price") < 0) |
        (F.col("freight_value") < 0)
    )
    .count()
)

print("Bronze row count:", bronze_count)
print("Silver row count:", silver_count)
print("Removed rows:", bronze_count - silver_count)
print("Duplicate business keys:", duplicate_count)
print("Invalid financial values:", invalid_financial_values)

print("Silver Order Items schema:")
saved_order_items.printSchema()

display(saved_order_items.limit(10))


# In[15]:


# Read the Order Payments table from the Bronze layer
bronze_order_payments = spark.table("bronze.order_payments")

print("Bronze order payments loaded successfully.")
print("Number of Bronze rows:", bronze_order_payments.count())

display(bronze_order_payments.limit(10))


# In[16]:


# Clean and standardize the Order Payments data
silver_order_payments = (
    bronze_order_payments

    # Remove unnecessary spaces
    .withColumn(
        "order_id",
        F.trim(F.col("order_id"))
    )
    .withColumn(
        "payment_type",
        F.lower(F.trim(F.col("payment_type")))
    )

    # Convert columns to the correct data types
    .withColumn(
        "payment_sequential",
        F.col("payment_sequential").cast("integer")
    )
    .withColumn(
        "payment_installments",
        F.col("payment_installments").cast("integer")
    )
    .withColumn(
        "payment_value",
        F.col("payment_value").cast("decimal(12,2)")
    )

    # Remove rows without a valid order ID
    .filter(
        F.col("order_id").isNotNull() &
        (F.length(F.col("order_id")) > 0)
    )

    # Remove rows without a valid payment type
    .filter(
        F.col("payment_type").isNotNull() &
        (F.length(F.col("payment_type")) > 0)
    )

    # Keep valid payment sequence numbers
    .filter(F.col("payment_sequential") > 0)

    # Keep non-negative installment and payment values
    .filter(F.col("payment_installments") >= 0)
    .filter(F.col("payment_value") >= 0)

    # Remove duplicates using the compound business key
    .dropDuplicates(["order_id", "payment_sequential"])
)

print("Order payments cleaned successfully.")
print("Number of Silver rows:", silver_order_payments.count())

display(silver_order_payments.limit(10))


# In[17]:


# Save the cleaned data as a Silver Delta table
(
    silver_order_payments.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.order_payments")
)

print("silver.order_payments created successfully.")


# In[18]:


# Read the saved Silver table
saved_order_payments = spark.table("silver.order_payments")

bronze_count = bronze_order_payments.count()
silver_count = saved_order_payments.count()

# Count duplicate compound keys
duplicate_count = (
    saved_order_payments
    .groupBy("order_id", "payment_sequential")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Count invalid payment values
invalid_payment_values = (
    saved_order_payments
    .filter(F.col("payment_value") < 0)
    .count()
)

print("Bronze row count:", bronze_count)
print("Silver row count:", silver_count)
print("Removed rows:", bronze_count - silver_count)
print("Duplicate business keys:", duplicate_count)
print("Invalid payment values:", invalid_payment_values)

print("Silver Order Payments schema:")
saved_order_payments.printSchema()

print("Payment types:")
saved_order_payments.groupBy("payment_type").count().orderBy(
    F.desc("count")
).show()

display(saved_order_payments.limit(10))


# In[19]:


# Read the Order Reviews table from the Bronze layer
bronze_order_reviews = spark.table("bronze.order_reviews")

print("Bronze order reviews loaded successfully.")
print("Number of Bronze rows:", bronze_order_reviews.count())

display(bronze_order_reviews.limit(10))


# In[20]:


# Clean and standardize the Order Reviews data
silver_order_reviews = (
    bronze_order_reviews

    # Remove unnecessary spaces from ID columns
    .withColumn(
        "review_id",
        F.trim(F.col("review_id"))
    )
    .withColumn(
        "order_id",
        F.trim(F.col("order_id"))
    )

    # Remove unnecessary spaces from comment columns
    .withColumn(
        "review_comment_title",
        F.trim(F.col("review_comment_title"))
    )
    .withColumn(
        "review_comment_message",
        F.trim(F.col("review_comment_message"))
    )

    # Replace empty comment titles with null
    .withColumn(
        "review_comment_title",
        F.when(
            F.length(F.col("review_comment_title")) == 0,
            F.lit(None)
        ).otherwise(F.col("review_comment_title"))
    )

    # Replace empty comment messages with null
    .withColumn(
        "review_comment_message",
        F.when(
            F.length(F.col("review_comment_message")) == 0,
            F.lit(None)
        ).otherwise(F.col("review_comment_message"))
    )

    # Convert the review score to integer
    .withColumn(
        "review_score",
        F.col("review_score").cast("integer")
    )

    # Convert the creation date to date
    .withColumn(
        "review_creation_date",
        F.to_date(
            F.col("review_creation_date"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    # Convert the answer date to timestamp
    .withColumn(
        "review_answer_timestamp",
        F.to_timestamp(
            F.col("review_answer_timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    # Remove rows without a valid review ID
    .filter(
        F.col("review_id").isNotNull() &
        (F.length(F.col("review_id")) > 0)
    )

    # Remove rows without a valid order ID
    .filter(
        F.col("order_id").isNotNull() &
        (F.length(F.col("order_id")) > 0)
    )

    # Keep review scores between 1 and 5
    .filter(
        F.col("review_score").between(1, 5)
    )

    # Remove duplicates using the compound business key
    .dropDuplicates(["review_id", "order_id"])
)

print("Order reviews cleaned successfully.")
print("Number of Silver rows:", silver_order_reviews.count())

display(silver_order_reviews.limit(10))


# In[21]:


# Save the cleaned data as a Silver Delta table
(
    silver_order_reviews.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.order_reviews")
)

print("silver.order_reviews created successfully.")


# In[22]:


# Read the saved Silver table
saved_order_reviews = spark.table("silver.order_reviews")

bronze_count = bronze_order_reviews.count()
silver_count = saved_order_reviews.count()

# Count duplicate compound keys
duplicate_count = (
    saved_order_reviews
    .groupBy("review_id", "order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Count invalid review scores
invalid_scores = (
    saved_order_reviews
    .filter(
        ~F.col("review_score").between(1, 5)
    )
    .count()
)

# Count reviews without a written message
reviews_without_message = (
    saved_order_reviews
    .filter(F.col("review_comment_message").isNull())
    .count()
)

print("Bronze row count:", bronze_count)
print("Silver row count:", silver_count)
print("Removed rows:", bronze_count - silver_count)
print("Duplicate business keys:", duplicate_count)
print("Invalid review scores:", invalid_scores)
print("Reviews without a written message:", reviews_without_message)

print("Silver Order Reviews schema:")
saved_order_reviews.printSchema()

print("Review score distribution:")
saved_order_reviews.groupBy("review_score").count().orderBy(
    "review_score"
).show()

display(saved_order_reviews.limit(10))


# In[23]:


# Save the cleaned data as a Silver Delta table
(
    silver_order_reviews.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.order_reviews")
)

print("silver.order_reviews created successfully.")


# In[24]:


# Read the saved Silver table
saved_order_reviews = spark.table("silver.order_reviews")

bronze_count = bronze_order_reviews.count()
silver_count = saved_order_reviews.count()

# Count duplicate compound keys
duplicate_count = (
    saved_order_reviews
    .groupBy("review_id", "order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Count invalid review scores
invalid_scores = (
    saved_order_reviews
    .filter(
        ~F.col("review_score").between(1, 5)
    )
    .count()
)

# Count reviews without a written message
reviews_without_message = (
    saved_order_reviews
    .filter(F.col("review_comment_message").isNull())
    .count()
)

print("Bronze row count:", bronze_count)
print("Silver row count:", silver_count)
print("Removed rows:", bronze_count - silver_count)
print("Duplicate business keys:", duplicate_count)
print("Invalid review scores:", invalid_scores)
print("Reviews without a written message:", reviews_without_message)

print("Silver Order Reviews schema:")
saved_order_reviews.printSchema()

print("Review score distribution:")
saved_order_reviews.groupBy("review_score").count().orderBy(
    "review_score"
).show()

display(saved_order_reviews.limit(10))


# In[25]:


# Read the Products table from the Bronze layer
bronze_products = spark.table("bronze.products")

print("Bronze products loaded successfully.")
print("Number of Bronze rows:", bronze_products.count())

display(bronze_products.limit(10))


# In[26]:


# Clean and standardize the Products data
silver_products = (
    bronze_products

    # Remove unnecessary spaces from the product ID
    .withColumn(
        "product_id",
        F.trim(F.col("product_id"))
    )

    # Standardize the product category
    .withColumn(
        "product_category_name",
        F.lower(F.trim(F.col("product_category_name")))
    )

    # Replace missing or empty categories with "unknown"
    .withColumn(
        "product_category_name",
        F.when(
            F.col("product_category_name").isNull() |
            (F.length(F.col("product_category_name")) == 0),
            F.lit("unknown")
        ).otherwise(F.col("product_category_name"))
    )

    # Convert numeric columns to integer
    .withColumn(
        "product_name_lenght",
        F.col("product_name_lenght").cast("integer")
    )
    .withColumn(
        "product_description_lenght",
        F.col("product_description_lenght").cast("integer")
    )
    .withColumn(
        "product_photos_qty",
        F.col("product_photos_qty").cast("integer")
    )
    .withColumn(
        "product_weight_g",
        F.col("product_weight_g").cast("integer")
    )
    .withColumn(
        "product_length_cm",
        F.col("product_length_cm").cast("integer")
    )
    .withColumn(
        "product_height_cm",
        F.col("product_height_cm").cast("integer")
    )
    .withColumn(
        "product_width_cm",
        F.col("product_width_cm").cast("integer")
    )

    # Remove rows without a valid product ID
    .filter(
        F.col("product_id").isNotNull() &
        (F.length(F.col("product_id")) > 0)
    )

    # Remove duplicate products
    .dropDuplicates(["product_id"])

    # Correct the misspelled column names
    .withColumnRenamed(
        "product_name_lenght",
        "product_name_length"
    )
    .withColumnRenamed(
        "product_description_lenght",
        "product_description_length"
    )
)

print("Products cleaned successfully.")
print("Number of Silver rows:", silver_products.count())

display(silver_products.limit(10))


# In[27]:


# Save the cleaned dat as a Silver Delta table
(
    silver_products.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.products")
)

print("silver.products created successfully.")


# In[28]:


# Read the saved Silver table
saved_products = spark.table("silver.products")

bronze_count = bronze_products.count()
silver_count = saved_products.count()

# Count duplicate product IDs
duplicate_count = (
    saved_products
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Count products with the "unknown" category
unknown_categories = (
    saved_products
    .filter(F.col("product_category_name") == "unknown")
    .count()
)

# Count products with missing physical information
missing_physical_data = (
    saved_products
    .filter(
        F.col("product_weight_g").isNull() |
        F.col("product_length_cm").isNull() |
        F.col("product_height_cm").isNull() |
        F.col("product_width_cm").isNull()
    )
    .count()
)

# Count products with zero weight
zero_weight_products = (
    saved_products
    .filter(F.col("product_weight_g") == 0)
    .count()
)

print("Bronze row count:", bronze_count)
print("Silver row count:", silver_count)
print("Removed rows:", bronze_count - silver_count)
print("Duplicate product IDs:", duplicate_count)
print("Products with unknown category:", unknown_categories)
print("Products with missing physical data:", missing_physical_data)
print("Products with zero weight:", zero_weight_products)

print("Silver Products schema:")
saved_products.printSchema()

display(saved_products.limit(10))


# In[29]:


# Read the Product Category Translation table from the Bronze layer
bronze_category_translation = spark.table(
 "bronze.product_category_translation"
)

print("Bronze category translation loaded successfully.")
print("Number of Bronze rows:", bronze_category_translation.count())

display(bronze_category_translation.limit(10))


# In[30]:


# Clean and standardize the Product Category Translation data
silver_category_translation = (
    bronze_category_translation

    # Remove unnecessary spaces and standardize the Portuguese category
    .withColumn(
        "product_category_name",
        F.lower(F.trim(F.col("product_category_name")))
    )

    # Remove unnecessary spaces and standardize the English category
    .withColumn(
        "product_category_name_english",
        F.lower(F.trim(F.col("product_category_name_english")))
    )

    # Remove rows without a valid Portuguese category
    .filter(
        F.col("product_category_name").isNotNull() &
        (F.length(F.col("product_category_name")) > 0)
    )

    # Remove rows without a valid English translation
    .filter(
        F.col("product_category_name_english").isNotNull() &
        (F.length(F.col("product_category_name_english")) > 0)
    )

    # Remove duplicate category records
    .dropDuplicates(["product_category_name"])
)

print("Category translation data cleaned successfully.")
print(
    "Number of Silver rows:",
    silver_category_translation.count()
)

display(silver_category_translation.limit(10))


# In[31]:


# Save the cleaned data as a Silver Delta table
(
    silver_category_translation.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.product_category_translation")
)

print("silver.product_category_translation created successfully.")


# In[32]:


# Read the saved Silver table
saved_category_translation = spark.table(
    "silver.product_category_translation"
)

bronze_count = bronze_category_translation.count()
silver_count = saved_category_translation.count()

# Count duplicate Portuguese categories
duplicate_categories = (
    saved_category_translation
    .groupBy("product_category_name")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Count missing category names or translations
missing_values = (
    saved_category_translation
    .filter(
        F.col("product_category_name").isNull() |
        F.col("product_category_name_english").isNull()
    )
    .count()
)

print("Bronze row count:", bronze_count)
print("Silver row count:", silver_count)
print("Removed rows:", bronze_count - silver_count)
print("Duplicate categories:", duplicate_categories)
print("Rows with missing values:", missing_values)

print("Silver Category Translation schema:")
saved_category_translation.printSchema()

display(
    saved_category_translation
    .select(
        "product_category_name",
        "product_category_name_english"
    )
    .orderBy("product_category_name")
)


# In[33]:


# Read the Sellers table from the Bronze layer
bronze_sellers = spark.table("bronze.sellers")

print("Bronze sellers loaded successfully.")
print("Number of Bronze rows:", bronze_sellers.count())

display(bronze_sellers.limit(10))


# In[34]:


# Clean and standardize the Sellers data
silver_sellers = (
    bronze_sellers

    # Remove unnecessary spaces from the seller ID
    .withColumn(
        "seller_id",
        F.trim(F.col("seller_id"))
    )

    # Standardize the seller city
    .withColumn(
        "seller_city",
        F.lower(F.trim(F.col("seller_city")))
    )

    # Standardize the seller state
    .withColumn(
        "seller_state",
        F.upper(F.trim(F.col("seller_state")))
    )

    # Convert the ZIP code prefix to integer
    .withColumn(
        "seller_zip_code_prefix",
        F.col("seller_zip_code_prefix").cast("integer")
    )

    # Remove rows without a valid seller ID
    .filter(
        F.col("seller_id").isNotNull() &
        (F.length(F.col("seller_id")) > 0)
    )

    # Remove rows without a valid city
    .filter(
        F.col("seller_city").isNotNull() &
        (F.length(F.col("seller_city")) > 0)
    )

    # Keep only valid two-letter state codes
    .filter(
        F.col("seller_state").isNotNull() &
        (F.length(F.col("seller_state")) == 2)
    )

    # Remove rows without a valid ZIP code prefix
    .filter(
        F.col("seller_zip_code_prefix").isNotNull()
    )

    # Remove duplicate sellers
    .dropDuplicates(["seller_id"])
)

print("Sellers cleaned successfully.")
print("Number of Silver rows:", silver_sellers.count())

display(silver_sellers.limit(10))


# In[35]:


# Save the cleaned data as a Silver Delta table
(
    silver_sellers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.sellers")
)

print("silver.sellers created successfully.")


# In[36]:


# Read the saved Silver table
saved_sellers = spark.table("silver.sellers")

bronze_count = bronze_sellers.count()
silver_count = saved_sellers.count()

# Count duplicate seller IDs
duplicate_sellers = (
    saved_sellers
    .groupBy("seller_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Count rows with missing required values
missing_required_values = (
    saved_sellers
    .filter(
        F.col("seller_id").isNull() |
        F.col("seller_zip_code_prefix").isNull() |
        F.col("seller_city").isNull() |
        F.col("seller_state").isNull()
    )
    .count()
)

# Count invalid state codes
invalid_state_codes = (
    saved_sellers
    .filter(F.length(F.col("seller_state")) != 2)
    .count()
)

print("Bronze row count:", bronze_count)
print("Silver row count:", silver_count)
print("Removed rows:", bronze_count - silver_count)
print("Duplicate seller IDs:", duplicate_sellers)
print("Rows with missing required values:", missing_required_values)
print("Invalid state codes:", invalid_state_codes)

print("Silver Sellers schema:")
saved_sellers.printSchema()

print("Sellers by state:")
saved_sellers.groupBy("seller_state").count().orderBy(
    F.desc("count")
).show(30)

display(saved_sellers.limit(10))


# In[37]:


# Read the Geolocation table from the Bronze layer
bronze_geolocation = spark.table("bronze.geolocation")

print("Bronze geolocation data loaded successfully.")
print("Number of Bronze rows:", bronze_geolocation.count())

display(bronze_geolocation.limit(10))


# In[38]:


# Clean and standardize the Geolocation data
silver_geolocation = (
    bronze_geolocation

    # Convert the ZIP code prefix to integer
    .withColumn(
        "geolocation_zip_code_prefix",
        F.col("geolocation_zip_code_prefix").cast("integer")
    )

    # Convert latitude and longitude to double
    .withColumn(
        "geolocation_lat",
        F.col("geolocation_lat").cast("double")
    )
    .withColumn(
        "geolocation_lng",
        F.col("geolocation_lng").cast("double")
    )

    # Standardize the city name
    .withColumn(
        "geolocation_city",
        F.lower(F.trim(F.col("geolocation_city")))
    )

    # Standardize the state code
    .withColumn(
        "geolocation_state",
        F.upper(F.trim(F.col("geolocation_state")))
    )

    # Remove rows without a valid ZIP code
    .filter(
        F.col("geolocation_zip_code_prefix").isNotNull()
    )

    # Keep ZIP code prefixes within the expected range
    .filter(
        F.col("geolocation_zip_code_prefix").between(1000, 99999)
    )

    # Remove rows without valid coordinates
    .filter(
        F.col("geolocation_lat").isNotNull() &
        F.col("geolocation_lng").isNotNull()
    )

    # Keep coordinates within the globally valid ranges
    .filter(
        F.col("geolocation_lat").between(-90, 90) &
        F.col("geolocation_lng").between(-180, 180)
    )

    # Remove rows without a valid city
    .filter(
        F.col("geolocation_city").isNotNull() &
        (F.length(F.col("geolocation_city")) > 0)
    )

    # Keep only two-letter state codes
    .filter(
        F.col("geolocation_state").isNotNull() &
        (F.length(F.col("geolocation_state")) == 2)
    )

    # Remove completely duplicated geographical records
    .dropDuplicates([
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state"
    ])
)

print("Geolocation data cleaned successfully.")
print("Number of Silver rows:", silver_geolocation.count())

display(silver_geolocation.limit(10))


# In[39]:


# Save the cleaned data as a Silver Delta table
(
    silver_geolocation.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.geolocation")
)

print("silver.geolocation created successfully.")


# In[40]:


# Read the saved Silver table
saved_geolocation = spark.table("silver.geolocation")

bronze_count = bronze_geolocation.count()
silver_count = saved_geolocation.count()

# Count completely duplicated geographical records
duplicate_locations = (
    saved_geolocation
    .groupBy(
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Count distinct ZIP code prefixes
distinct_zip_codes = (
    saved_geolocation
    .select("geolocation_zip_code_prefix")
    .distinct()
    .count()
)

# Count rows with missing required values
missing_required_values = (
    saved_geolocation
    .filter(
        F.col("geolocation_zip_code_prefix").isNull() |
        F.col("geolocation_lat").isNull() |
        F.col("geolocation_lng").isNull() |
        F.col("geolocation_city").isNull() |
        F.col("geolocation_state").isNull()
    )
    .count()
)

# Count invalid state codes
invalid_state_codes = (
    saved_geolocation
    .filter(F.length(F.col("geolocation_state")) != 2)
    .count()
)

print("Bronze row count:", bronze_count)
print("Silver row count:", silver_count)
print("Removed duplicate rows:", bronze_count - silver_count)
print("Duplicate geographical records:", duplicate_locations)
print("Distinct ZIP code prefixes:", distinct_zip_codes)
print("Rows with missing required values:", missing_required_values)
print("Invalid state codes:", invalid_state_codes)

print("Silver Geolocation schema:")
saved_geolocation.printSchema()

print("Geolocation records by state:")
(
    saved_geolocation
    .groupBy("geolocation_state")
    .count()
    .orderBy(F.desc("count"))
    .show(30)
)

display(saved_geolocation.limit(10))


# In[41]:


# Import Spark functions
from pyspark.sql import functions as F

# List of Silver tables
silver_tables = [
    "silver.customers",
    "silver.orders",
    "silver.order_items",
    "silver.order_payments",
    "silver.order_reviews",
    "silver.products",
    "silver.product_category_translation",
    "silver.sellers",
    "silver.geolocation"
]

print("SILVER LAYER ROW COUNTS")
print("-" * 50)

# Count the rows from every Silver table
for table_name in silver_tables:
    row_count = spark.table(table_name).count()
    print(table_name, ":", row_count)


# In[42]:


# Load the Silver tables
customers = spark.table("silver.customers")
orders = spark.table("silver.orders")
order_items = spark.table("silver.order_items")
order_payments = spark.table("silver.order_payments")
order_reviews = spark.table("silver.order_reviews")
products = spark.table("silver.products")
sellers = spark.table("silver.sellers")

# Orders without a matching customer
orders_without_customer = (
    orders
    .join(
        customers.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
    .count()
)

# Order items without a matching order
items_without_order = (
    order_items
    .join(
        orders.select("order_id"),
        on="order_id",
        how="left_anti"
    )
    .count()
)

# Order items without a matching product
items_without_product = (
    order_items
    .join(
        products.select("product_id"),
        on="product_id",
        how="left_anti"
    )
    .count()
)

# Order items without a matching seller
items_without_seller = (
    order_items
    .join(
        sellers.select("seller_id"),
        on="seller_id",
        how="left_anti"
    )
    .count()
)

# Payments without a matching order
payments_without_order = (
    order_payments
    .join(
        orders.select("order_id"),
        on="order_id",
        how="left_anti"
    )
    .count()
)

# Reviews without a matching order
reviews_without_order = (
    order_reviews
    .join(
        orders.select("order_id"),
        on="order_id",
        how="left_anti"
    )
    .count()
)

print("REFERENTIAL INTEGRITY CHECKS")
print("-" * 50)
print("Orders without customer:", orders_without_customer)
print("Order items without order:", items_without_order)
print("Order items without product:", items_without_product)
print("Order items without seller:", items_without_seller)
print("Payments without order:", payments_without_order)
print("Reviews without order:", reviews_without_order)


# In[43]:


# Load the translation table
category_translation = spark.table(
    "silver.product_category_translation"
)

# Find known categories without an English translation
products_without_translation = (
    products
    .filter(F.col("product_category_name") != "unknown")
    .join(
        category_translation.select("product_category_name"),
        on="product_category_name",
        how="left_anti"
    )
)

products_without_translation_count = (
    products_without_translation.count()
)

categories_without_translation_count = (
    products_without_translation
    .select("product_category_name")
    .distinct()
    .count()
)

unknown_category_count = (
    products
    .filter(F.col("product_category_name") == "unknown")
    .count()
)

print("CATEGORY CHECKS")
print("-" * 50)
print("Products with unknown category:", unknown_category_count)
print(
    "Products without English translation:",
    products_without_translation_count
)
print(
    "Categories without English translation:",
    categories_without_translation_count
)

print("Categories without translation:")

(
    products_without_translation
    .select("product_category_name")
    .distinct()
    .orderBy("product_category_name")
    .show(truncate=False)
)


# In[44]:


# Load the Geolocation table
geolocation = spark.table("silver.geolocation")

# Create a list of distinct ZIP codes
geolocation_zip_codes = (
    geolocation
    .select("geolocation_zip_code_prefix")
    .distinct()
)

# Find customers without a matching geographical ZIP code
customers_without_geolocation = (
    customers
    .join(
        geolocation_zip_codes,
        customers.customer_zip_code_prefix ==
        geolocation_zip_codes.geolocation_zip_code_prefix,
        "left_anti"
    )
    .count()
)

# Find sellers without a matching geographical ZIP code
sellers_without_geolocation = (
    sellers
    .join(
        geolocation_zip_codes,
        sellers.seller_zip_code_prefix ==
        geolocation_zip_codes.geolocation_zip_code_prefix,
        "left_anti"
    )
    .count()
)

print("GEOLOCATION COVERAGE CHECKS")
print("-" * 50)
print(
    "Customers without geolocation:",
    customers_without_geolocation
)
print(
    "Sellers without geolocation:",
    sellers_without_geolocation
)

