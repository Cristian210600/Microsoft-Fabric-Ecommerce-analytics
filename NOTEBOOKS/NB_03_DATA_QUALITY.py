#!/usr/bin/env python
# coding: utf-8

# ## NB_03_DATA_QUALITY
# 
# null

# In[66]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql("CREATE SCHEMA IF NOT EXISTS audit")
print("Schema audit a fost creata cu succes.")


# In[67]:


bronze_tables = spark.sql("SHOW TABLES IN Bronze")
print(f"Tabele Bronze gasite: {bronze_tables.count()}")
display(bronze_tables.orderBy("tableName"))


# In[68]:


spark.sql("""
    CREATE TABLE IF NOT EXISTS audit.data_quality_results (
        run_id STRING,
        batch_id STRING,
        table_name STRING,
        rule_id STRING,
        rule_name STRING,
        column_name STRING,
        severity STRING,
        rows_checked BIGINT,
        failed_rows BIGINT,
        failed_percentage DOUBLE,
        status STRING,
        execution_timestamp TIMESTAMP
    )
    USING DELTA
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS audit.rejected_records (
        run_id STRING,
        batch_id STRING,
        source_table STRING,
        rule_id STRING,
        severity STRING,
        record_key STRING,
        rejection_reason STRING,
        record_payload STRING,
        rejected_timestamp TIMESTAMP
    )
    USING DELTA
""")

print("Tabelele de audit au fost create cu succes.")


# In[69]:


audit_tables = spark.sql("SHOW TABLES IN audit")

print(f"Tabele Audit găsite: {audit_tables.count()}")
display(audit_tables.orderBy("tableName"))


# In[70]:


import uuid
from datetime import datetime
from pyspark.sql import functions as F

# Generam un ID unic pentru aceasta rulare
RUN_ID = str(uuid.uuid4())

# Salvam momentul rularii
EXECUTION_TIMESTAMP = datetime.now()

# Citim tabelul customers din Bronze
customers_df = spark.table("bronze.customers")

# Numarul total de randuri
rows_checked = customers_df.count()

# Luam batch_id-ul din tabel
batch_id = (
    customers_df
        .filter(F.col("_batch_id").isNotNull())
        .select("_batch_id")
        .first()["_batch_id"]
)

print("RUN_ID:", RUN_ID)
print("BATCH_ID:", batch_id)
print("Rânduri verificate:", rows_checked)


# In[71]:


# Regula 1:
# customer_id nu trebuie să fie NULL sau gol

missing_customer_id = (
    customers_df
        .filter(
            F.col("customer_id").isNull() |
            (F.trim(F.col("customer_id")) == "")
        )
        .count()
)


# Regula 2:
# customer_id trebuie să fie unic

duplicate_customer_id = (
    customers_df
        .groupBy("customer_id")
        .count()
        .filter(
            F.col("customer_id").isNotNull() &
            (F.col("count") > 1)
        )
        .count()
)


# Regula 3:
# customer_unique_id nu trebuie să fie NULL sau gol

missing_customer_unique_id = (
    customers_df
        .filter(
            F.col("customer_unique_id").isNull() |
            (F.trim(F.col("customer_unique_id")) == "")
        )
        .count()
)


# Regula 4:
# customer_state trebuie să conțină două litere mari

invalid_customer_state = (
    customers_df
        .filter(
            F.col("customer_state").isNull() |
            (~F.col("customer_state").rlike("^[A-Z]{2}$"))
        )
        .count()
)


print("Customer ID lipsă:", missing_customer_id)
print("Customer ID duplicat:", duplicate_customer_id)
print("Customer unique ID lipsă:", missing_customer_unique_id)
print("State invalid:", invalid_customer_state)


# In[72]:


def calculate_status(failed_rows, severity):

    if failed_rows == 0:
        return "PASS"

    if severity == "WARNING":
        return "WARNING"

    return "FAIL"


# Pregatim rezultatele celor patru reguli

quality_results = [
    (
        RUN_ID,
        batch_id,
        "bronze.customers",
        "DQ_CUSTOMERS_001",
        "Customer ID must not be null",
        "customer_id",
        "ERROR",
        rows_checked,
        missing_customer_id,
        (missing_customer_id / rows_checked) * 100,
        calculate_status(missing_customer_id, "ERROR"),
        EXECUTION_TIMESTAMP
    ),
    (
        RUN_ID,
        batch_id,
        "bronze.customers",
        "DQ_CUSTOMERS_002",
        "Customer ID must be unique",
        "customer_id",
        "ERROR",
        rows_checked,
        duplicate_customer_id,
        (duplicate_customer_id / rows_checked) * 100,
        calculate_status(duplicate_customer_id, "ERROR"),
        EXECUTION_TIMESTAMP
    ),
    (
        RUN_ID,
        batch_id,
        "bronze.customers",
        "DQ_CUSTOMERS_003",
        "Customer unique ID must not be null",
        "customer_unique_id",
        "ERROR",
        rows_checked,
        missing_customer_unique_id,
        (missing_customer_unique_id / rows_checked) * 100,
        calculate_status(missing_customer_unique_id, "ERROR"),
        EXECUTION_TIMESTAMP
    ),
    (
        RUN_ID,
        batch_id,
        "bronze.customers",
        "DQ_CUSTOMERS_004",
        "Customer state must have two uppercase letters",
        "customer_state",
        "WARNING",
        rows_checked,
        invalid_customer_state,
        (invalid_customer_state / rows_checked) * 100,
        calculate_status(invalid_customer_state, "WARNING"),
        EXECUTION_TIMESTAMP
    )
]


# In[73]:


results_df = spark.createDataFrame(
    quality_results,
    """
    run_id STRING,
    batch_id STRING,
    table_name STRING,
    rule_id STRING,
    rule_name STRING,
    column_name STRING,
    severity STRING,
    rows_checked LONG,
    failed_rows LONG,
    failed_percentage DOUBLE,
    status STRING,
    execution_timestamp TIMESTAMP
    """
)

display(
    results_df.select(
        "rule_id",
        "rule_name",
        "severity",
        "rows_checked",
        "failed_rows",
        "failed_percentage",
        "status"
    ).orderBy("rule_id")
)


# In[74]:


(
    results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.data_quality_results")
)

print("Rezultatele au fost salvate în audit.data_quality_results")


# In[75]:


import uuid
from datetime import datetime
from pyspark.sql import functions as F

# ID unic pentru verificarea tabelului orders
RUN_ID = str(uuid.uuid4())

# Momentul verificarii
EXECUTION_TIMESTAMP = datetime.now()

# Citim tabelul
orders_df = spark.table("bronze.orders")

# Numărul total de rânduri
rows_checked = orders_df.count()

# Luam batch_id-ul
batch_id = (
    orders_df
        .filter(F.col("_batch_id").isNotNull())
        .select("_batch_id")
        .first()["_batch_id"]
)

# Transforma coloanele de tip data
orders_prepared_df = (
    orders_df
        .withColumn(
            "_purchase_date",
            F.to_timestamp("order_purchase_timestamp")
        )
        .withColumn(
            "_approved_date",
            F.to_timestamp("order_approved_at")
        )
        .withColumn(
            "_delivered_date",
            F.to_timestamp("order_delivered_customer_date")
        )
)

print("RUN_ID:", RUN_ID)
print("BATCH_ID:", batch_id)
print("Rânduri verificate:", rows_checked)


# In[76]:


# Rule 1:
# The order ID must not be null or empty

missing_order_id = (
    orders_df
        .filter(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == "")
        )
        .count()
)


# Rule 2:
# The order ID must be unique

duplicate_order_ids_df = (
    orders_df
        .groupBy("order_id")
        .count()
        .filter(
            F.col("order_id").isNotNull() &
            (F.col("count") > 1)
        )
)

duplicate_order_id = (
    orders_df
        .join(
            duplicate_order_ids_df.select("order_id"),
            on="order_id",
            how="inner"
        )
        .count()
)


# Rule 3:
# The customer ID must exist in the Customers table

valid_customers_df = (
    spark.table("bronze.customers")
        .select("customer_id")
        .distinct()
)

orders_without_customer_df = (
    orders_df
        .join(
            valid_customers_df,
            on="customer_id",
            how="left_anti"
        )
)

missing_customer = orders_without_customer_df.count()


# Rule 4:
# The order status must contain an allowed value

allowed_statuses = [
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "unavailable",
    "canceled"
]

invalid_order_status = (
    orders_df
        .filter(
            F.col("order_status").isNull() |
            (~F.col("order_status").isin(allowed_statuses))
        )
        .count()
)


# Rule 5:
# The approval date cannot be earlier than the purchase date

approval_before_purchase = (
    orders_prepared_df
        .filter(
            F.col("_approved_date").isNotNull() &
            F.col("_purchase_date").isNotNull() &
            (F.col("_approved_date") < F.col("_purchase_date"))
        )
        .count()
)


# Rule 6:
# The delivery date cannot be earlier than the purchase date

delivery_before_purchase = (
    orders_prepared_df
        .filter(
            F.col("_delivered_date").isNotNull() &
            F.col("_purchase_date").isNotNull() &
            (F.col("_delivered_date") < F.col("_purchase_date"))
        )
        .count()
)


# Rule 7:
# A delivered order must have a delivery date

delivered_without_date = (
    orders_prepared_df
        .filter(
            (F.col("order_status") == "delivered") &
            F.col("_delivered_date").isNull()
        )
        .count()
)


print("Missing Order IDs:", missing_order_id)
print("Rows with duplicate Order IDs:", duplicate_order_id)
print("Orders without a valid customer:", missing_customer)
print("Orders with an invalid status:", invalid_order_status)
print("Approvals before the purchase date:", approval_before_purchase)
print("Deliveries before the purchase date:", delivery_before_purchase)
print("Delivered orders without a delivery date:", delivered_without_date)
        


# In[77]:


# Create an empty list for the data quality results
orders_quality_results = []


# Create a simple function that adds one rule result
def add_order_result(
    rule_id,
    rule_name,
    column_name,
    severity,
    failed_rows
):

    # Calculate the percentage of failed rows
    if rows_checked > 0:
        failed_percentage = (
            failed_rows / rows_checked
        ) * 100
    else:
        failed_percentage = 0.0

    # Determine the rule status
    if failed_rows == 0:
        status = "PASS"
    elif severity == "WARNING":
        status = "WARNING"
    else:
        status = "FAIL"

    # Add the result to the list
    orders_quality_results.append(
        (
            RUN_ID,
            batch_id,
            "bronze.orders",
            rule_id,
            rule_name,
            column_name,
            severity,
            rows_checked,
            failed_rows,
            failed_percentage,
            status,
            EXECUTION_TIMESTAMP
        )
    )


# Add the seven Orders rules

add_order_result(
    "DQ_ORDERS_001",
    "Order ID must not be null",
    "order_id",
    "ERROR",
    missing_order_id
)

add_order_result(
    "DQ_ORDERS_002",
    "Order ID must be unique",
    "order_id",
    "ERROR",
    duplicate_order_id
)

add_order_result(
    "DQ_ORDERS_003",
    "Customer ID must exist in the Customers table",
    "customer_id",
    "ERROR",
    missing_customer
)

add_order_result(
    "DQ_ORDERS_004",
    "Order status must be valid",
    "order_status",
    "ERROR",
    invalid_order_status
)

add_order_result(
    "DQ_ORDERS_005",
    "Approval date cannot be before the purchase date",
    "order_approved_at",
    "ERROR",
    approval_before_purchase
)

add_order_result(
    "DQ_ORDERS_006",
    "Delivery date cannot be before the purchase date",
    "order_delivered_customer_date",
    "ERROR",
    delivery_before_purchase
)

add_order_result(
    "DQ_ORDERS_007",
    "Delivered orders must have a delivery date",
    "order_delivered_customer_date",
    "ERROR",
    delivered_without_date
)

print("Rules prepared:", len(orders_quality_results))






# In[78]:


orders_results_df = spark.createDataFrame(
    orders_quality_results,
    """
    run_id STRING,
    batch_id STRING,
    table_name STRING,
    rule_id STRING,
    rule_name STRING,
    column_name STRING,
    severity STRING,
    rows_checked LONG,
    failed_rows LONG,
    failed_percentage DOUBLE,
    status STRING,
    execution_timestamp TIMESTAMP
    """
)

display(
    orders_results_df
        .select(
            "rule_id",
            "rule_name",
            "severity",
            "rows_checked",
            "failed_rows",
            "failed_percentage",
            "status"
        )
        .orderBy("rule_id")
)


# In[79]:


# Save the Orders data quality results in the audit table

(
    orders_results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.data_quality_results")
)

print("The data quality results for bronze.orders were saved successfully.")


# In[80]:


# Select delivered orders without a delivery date

rejected_orders_df = (
    orders_df
        .filter(
            (F.col("order_status") == "delivered") &
            F.to_timestamp(
                F.col("order_delivered_customer_date")
            ).isNull()
        )
        .select(
            F.lit(RUN_ID).alias("run_id"),
            F.lit(batch_id).alias("batch_id"),
            F.lit("bronze.orders").alias("source_table"),
            F.lit("DQ_ORDERS_007").alias("rule_id"),
            F.lit("ERROR").alias("severity"),
            F.col("order_id").alias("record_key"),
            F.lit(
                "Delivered order does not have a delivery date"
            ).alias("rejection_reason"),
            F.to_json(
                F.struct(
                    *[F.col(column) for column in orders_df.columns]
                )
            ).alias("record_payload"),
            F.current_timestamp().alias("rejected_timestamp")
        )
)

print("Rejected records found:", rejected_orders_df.count())

display(
    rejected_orders_df.select(
        "record_key",
        "rejection_reason"
    )
)


# In[81]:


# Save the rejected Orders records in the quarantine table

(
    rejected_orders_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.rejected_records")
)

print("The rejected Orders records were saved successfully.")


# In[82]:


import uuid
from datetime import datetime
from pyspark.sql import functions as F

# Generate a unique ID for ths data quality run
ITEMS_RUN_ID = str(uuid.uuid4())

# Save the execution timestamp
ITEMS_EXECUTION_TIMESTAMP = datetime.now()

# Read the Bronze Order Items table
order_items_df = spark.table("bronze.order_items")

# Count the total number of rows
items_rows_checked = order_items_df.count()

# Get the batch ID
items_batch_id = (
    order_items_df
        .filter(F.col("_batch_id").isNotNull())
        .select("_batch_id")
        .first()["_batch_id"]
)
# Convert numeric and date columns
order_items_prepared_df = (
    order_items_df
        .withColumn(
            "_shipping_limit_timestamp",
            F.to_timestamp("shipping_limit_date")
        )
        .withColumn(
            "_price_value",
            F.col("price").cast("double")
        )
        .withColumn(
            "_freight_value",
            F.col("freight_value").cast("double")
        )
)
print("Run ID:", ITEMS_RUN_ID)
print("Batch ID:", items_batch_id)
print("Rows checked:", items_rows_checked)


# In[83]:


# Rule 1:
# The order ID must not be null or empty

missing_item_order_id = (
    order_items_df
        .filter(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == "")
        )
        .count()
)


# Rule 2:
# The order item ID must be a positive integer

invalid_order_item_id = (
    order_items_df
        .filter(
            F.col("order_item_id").isNull() |
            (~F.trim(F.col("order_item_id")).rlike("^[1-9][0-9]*$"))
        )
        .count()
)


# Rule 3:
# The combination of order_id and order_item_id must be unique

duplicate_item_keys_df = (
    order_items_df
        .groupBy("order_id", "order_item_id")
        .count()
        .filter(F.col("count") > 1)
        .select("order_id", "order_item_id")
)

duplicate_item_key = (
    order_items_df
        .join(
            duplicate_item_keys_df,
            on=["order_id", "order_item_id"],
            how="inner"
        )
        .count()
)


# Rule 4:
# The order ID must exist in the Orders table

valid_orders_df = (
    spark.table("bronze.orders")
        .select("order_id")
        .distinct()
)

items_without_order = (
    order_items_df
        .join(
            valid_orders_df,
            on="order_id",
            how="left_anti"
        )
        .count()
)


# Rule 5:
# The product ID must exist in the Products table

valid_products_df = (
    spark.table("bronze.products")
        .select("product_id")
        .distinct()
)

items_without_product = (
    order_items_df
        .join(
            valid_products_df,
            on="product_id",
            how="left_anti"
        )
        .count()
)


# Rule 6:
valid_sellers_df = (
    spark.table("bronze.sellers")
        .select("seller_id")
        .distinct()
)

items_without_seller = (
    order_items_df
        .join(
            valid_sellers_df,
            on="seller_id",
            how="left_anti"
        )
        .count()
)

print("Missing Order IDs:", missing_item_order_id)
print("Invalid Order Item IDs:", invalid_order_item_id)
print("Rows with duplicate item keys:", duplicate_item_key)
print("Items without a valid order:", items_without_order)
print("Items without a valid product:", items_without_product)
print("Items without a valid seller:", items_without_seller)


# In[84]:


# Rule 7:
invalid_shipping_limit_date = (
    order_items_prepared_df
        .filter(
            F.col("shipping_limit_date").isNull() |
            (F.trim(F.col("shipping_limit_date")) == "") |
            F.col("_shipping_limit_timestamp").isNull()
        )
        .count()
)


# Rule 8:
# The price must be greater than zero

invalid_price = (
    order_items_prepared_df
        .filter(
            F.col("_price_value").isNull() |
            (F.col("_price_value") <= 0)
        )
        .count()
)


# Rule 9:
invalid_freight_value = (
    order_items_prepared_df
        .filter(
            F.col("_freight_value").isNull() |
            (F.col("_freight_value") < 0)
        )
        .count()
)


# Rule 10:
# The shipping limit cannot be before the purchase date

order_purchase_dates_df = (
    spark.table("bronze.orders")
        .select(
            "order_id",
            F.to_timestamp(
                "order_purchase_timestamp"
            ).alias("_purchase_timestamp")
        )
)

items_with_purchase_date_df = (
    order_items_prepared_df
        .join(
            order_purchase_dates_df,
            on="order_id",
            how="left"
        )
)

shipping_before_purchase = (
    items_with_purchase_date_df
        .filter(
            F.col("_shipping_limit_timestamp").isNotNull() &
            F.col("_purchase_timestamp").isNotNull() &
            (
                F.col("_shipping_limit_timestamp") <
                F.col("_purchase_timestamp")
            )
        )
        .count()
)

print("Invalid shipping limit dates:", invalid_shipping_limit_date)
print("Invalid prices:", invalid_price)
print("Invalid freight values:", invalid_freight_value)
print("Shipping limits before purchase:", shipping_before_purchase)


# In[85]:


# Crete an empty list for the results
items_quality_results = []


# Create a function that adds one rule result
def add_item_result(
    rule_id,
    rule_name,
    column_name,
    severity,
    failed_rows
):

    # Calculate the failed percentage
    if items_rows_checked > 0:
        failed_percentage = (
            failed_rows / items_rows_checked
        ) * 100
    else:
        failed_percentage = 0.0

    # Determine the rule status
    if failed_rows == 0:
        status = "PASS"
    elif severity == "WARNING":
        status = "WARNING"
    else:
        status = "FAIL"

    # Add the result to the list
    items_quality_results.append(
        (
            ITEMS_RUN_ID,
            items_batch_id,
            "bronze.order_items",
            rule_id,
            rule_name,
            column_name,
            severity,
            items_rows_checked,
            failed_rows,
            failed_percentage,
            status,
            ITEMS_EXECUTION_TIMESTAMP
        )
    )


# Add the Order Items rules

add_item_result(
    "DQ_ORDER_ITEMS_001",
    "Order ID must not be null",
    "order_id",
    "ERROR",
    missing_item_order_id
)

add_item_result(
    "DQ_ORDER_ITEMS_002",
    "Order item ID must be a positive integer",
    "order_item_id",
    "ERROR",
    invalid_order_item_id
)

add_item_result(
    "DQ_ORDER_ITEMS_003",
    "Order and item ID combination must be unique",
    "order_id, order_item_id",
    "ERROR",
    duplicate_item_key
)

add_item_result(
    "DQ_ORDER_ITEMS_004",
    "Order ID must exist in the Orders table",
    "order_id",
    "ERROR",
    items_without_order
)

add_item_result(
    "DQ_ORDER_ITEMS_005",
    "Product ID must exist in the Products table",
    "product_id",
    "ERROR",
    items_without_product
)

add_item_result(
    "DQ_ORDER_ITEMS_006",
    "Seller ID must exist in the Sellers table",
    "seller_id",
    "ERROR",
    items_without_seller
)

add_item_result(
    "DQ_ORDER_ITEMS_007",
    "Shipping limit date must be valid",
    "shipping_limit_date",
    "ERROR",
    invalid_shipping_limit_date
)

add_item_result(
    "DQ_ORDER_ITEMS_008",
    "Price must be greater than zero",
    "price",
    "ERROR",
    invalid_price
)

add_item_result(
    "DQ_ORDER_ITEMS_009",
    "Freight value cannot be negative",
    "freight_value",
    "ERROR",
    invalid_freight_value
)
add_item_result(
    "DQ_ORDER_ITEMS_010",
    "Shipping limit cannot be before purchase",
    "shipping_limit_date",
    "ERROR",
    shipping_before_purchase
)
print("Rules prepared:", len(items_quality_results))


# In[86]:


# Convert the results into a Spark DataFrame

items_results_df = spark.createDataFrame(
    items_quality_results,
    """
    run_id STRING,
    batch_id STRING,
    table_name STRING,
    rule_id STRING,
    rule_name STRING,
    column_name STRING,
    severity STRING,
    rows_checked LONG,
    failed_rows LONG,
    failed_percentage DOUBLE,
    status STRING,
    execution_timestamp TIMESTAMP
    """
)

# Display the data quality results

display(
    items_results_df
        .select(
            "rule_id",
            "rule_name",
            "severity",
            "rows_checked",
            "failed_rows",
            "failed_percentage",
            "status"
        )
        .orderBy("rule_id")
)


# In[87]:


# Save the Order Items results in the audit table

(
    items_results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.data_quality_results")
)

print(
    "The data quality results for bronze.order_items "
    "were saved successfully."
)


# In[88]:


import uuid
from datetime import datetime
from pyspark.sql import functions as F

# Generate a unique ID for this data quality run
PAYMENTS_RUN_ID = str(uuid.uuid4())

# Save the execution timestamp
PAYMENTS_EXECUTION_TIMESTAMP = datetime.now()

# Read the Bronze Order Payments table
payments_df = spark.table("bronze.order_payments")

# Count the total number of rows
payments_rows_checked = payments_df.count()

# Get the batch ID
payments_batch_id = (
    payments_df
        .filter(F.col("_batch_id").isNotNull())
        .select("_batch_id")
        .first()["_batch_id"]
)

# Convert numeric columns
payments_prepared_df = (
    payments_df
        .withColumn(
            "_payment_sequential_value",
            F.col("payment_sequential").cast("integer")
        )
        .withColumn(
            "_payment_installments_value",
            F.col("payment_installments").cast("integer")
        )
        .withColumn(
            "_payment_value_number",
            F.col("payment_value").cast("double")
        )
)

print("Run ID:", PAYMENTS_RUN_ID)
print("Batch ID:", payments_batch_id)
print("Rows checked:", payments_rows_checked)


# In[89]:


# Rule 1:
# The order ID must not be null or empty

missing_payment_order_id = (
    payments_df
        .filter(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == "")
        )
        .count()
)


# Rule 2:
# The payment sequential number must be a positive integer

invalid_payment_sequential = (
    payments_prepared_df
        .filter(
            F.col("_payment_sequential_value").isNull() |
            (F.col("_payment_sequential_value") <= 0)
        )
        .count()
)


# Rule 3:
# The combination of order ID and payment sequential must be unique

duplicate_payment_keys_df = (
    payments_df
        .groupBy(
            "order_id",
            "payment_sequential"
        )
        .count()
        .filter(F.col("count") > 1)
        .select(
            "order_id",
            "payment_sequential"
        )
)

duplicate_payment_key = (
    payments_df
        .join(
            duplicate_payment_keys_df,
            on=[
                "order_id",
                "payment_sequential"
            ],
            how="inner"
        )
        .count()
)


# Rule 4:
# The order ID must exist in the Orders table

valid_orders_df = (
    spark.table("bronze.orders")
        .select("order_id")
        .distinct()
)

payments_without_order = (
    payments_df
        .join(
            valid_orders_df,
            on="order_id",
            how="left_anti"
        )
        .count()
)


print("Missing Order IDs:", missing_payment_order_id)
print("Invalid payment sequential numbers:", invalid_payment_sequential)
print("Rows with duplicate payment keys:", duplicate_payment_key)
print("Payments without a valid order:", payments_without_order)


# In[90]:


# Rule 5:
# The payment type must contain a recognized value
allowed_payment_types = [
    "credit_card",
    "debit_card",
    "boleto",
    "voucher"
]
invalid_payment_type = (
    payments_df
        .filter(
            F.col("payment_type").isNull() |
            (~F.col("payment_type").isin(allowed_payment_types))
        )
        .count()
)


# Rule 6:
# The number of installments cannot be negative

invalid_payment_installments = (
    payments_prepared_df
        .filter(
            F.col("_payment_installments_value").isNull() |
            (F.col("_payment_installments_value") < 0)
        )
        .count()
)


# Rule 7:
# A credit card payment must have at least one installment

credit_card_without_installments = (
    payments_prepared_df
        .filter(
            (F.col("payment_type") == "credit_card") &
            (
                F.col("_payment_installments_value").isNull() |
                (F.col("_payment_installments_value") < 1)
            )
        )
        .count()
)


# Rule 8:
# The payment value should be greater than zero

invalid_payment_value = (
    payments_prepared_df
        .filter(
            F.col("_payment_value_number").isNull() |
            (F.col("_payment_value_number") <= 0)
        )
        .count()
)

print("Unrecognized payment types:", invalid_payment_type)
print("Invalid installment values:", invalid_payment_installments)
print(
    "Credit card payments without installments:",
    credit_card_without_installments
)
print("Payments with a non-positive value:", invalid_payment_value)


# In[91]:


# Create an empty list for the quality results

payments_quality_results = []


# Create a function that adds one rule result

def add_payment_result(
    rule_id,
    rule_name,
    column_name,
    severity,
    failed_rows
):

    # Calculate the failed percentage

    if payments_rows_checked > 0:
        failed_percentage = (
            failed_rows / payments_rows_checked
        ) * 100
    else:
        failed_percentage = 0.0

    # Determine the rule status

    if failed_rows == 0:
        status = "PASS"
    elif severity == "WARNING":
        status = "WARNING"
    else:
        status = "FAIL"

    # Add the result to the list

    payments_quality_results.append(
        (
            PAYMENTS_RUN_ID,
            payments_batch_id,
            "bronze.order_payments",
            rule_id,
            rule_name,
            column_name,
            severity,
            payments_rows_checked,
            failed_rows,
            failed_percentage,
            status,
            PAYMENTS_EXECUTION_TIMESTAMP
        )
    )


# Add the eight Order Payments rules

add_payment_result(
    "DQ_PAYMENTS_001",
    "Order ID must not be null",
    "order_id",
    "ERROR",
    missing_payment_order_id
)

add_payment_result(
    "DQ_PAYMENTS_002",
    "Payment sequential must be a positive integer",
    "payment_sequential",
    "ERROR",
    invalid_payment_sequential
)

add_payment_result(
    "DQ_PAYMENTS_003",
    "Order and payment sequential combination must be unique",
    "order_id, payment_sequential",
    "ERROR",
    duplicate_payment_key
)

add_payment_result(
    "DQ_PAYMENTS_004",
    "Order ID must exist in the Orders table",
    "order_id",
    "ERROR",
    payments_without_order
)

add_payment_result(
    "DQ_PAYMENTS_005",
    "Payment type must be recognized",
    "payment_type",
    "WARNING",
    invalid_payment_type
)

add_payment_result(
    "DQ_PAYMENTS_006",
    "Payment installments cannot be negative",
    "payment_installments",
    "ERROR",
    invalid_payment_installments
)

add_payment_result(
    "DQ_PAYMENTS_007",
    "Credit card payment must have at least one installment",
    "payment_installments",
    "ERROR",
    credit_card_without_installments
)

add_payment_result(
    "DQ_PAYMENTS_008",
    "Payment value should be greater than zero",
    "payment_value",
    "WARNING",
    invalid_payment_value
)

print("Rules prepared:", len(payments_quality_results))


# In[92]:


# Convert the results into a Spark DataFrame

payments_results_df = spark.createDataFrame(
    payments_quality_results,
    """
    run_id STRING,
    batch_id STRING,
    table_name STRING,
    rule_id STRING,
    rule_name STRING,
    column_name STRING,
    severity STRING,
    rows_checked LONG,
    failed_rows LONG,
    failed_percentage DOUBLE,
    status STRING,
    execution_timestamp TIMESTAMP
    """
)


# Display the data quality results

display(
    payments_results_df
        .select(
            "rule_id",
            "rule_name",
            "severity",
            "rows_checked",
            "failed_rows",
            "failed_percentage",
            "status"
        )
        .orderBy("rule_id")
)


# In[93]:


# Save the Order Payments results in the audit table

(
    payments_results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.data_quality_results")
)

print(
    "The data quality results for bronze.order_payments "
    "were saved successfully."
)


# In[94]:


# Select credit card payments without valid installments

rejected_payments_df = (
    payments_prepared_df
        .filter(
            (F.col("payment_type") == "credit_card") &
            (
                F.col("_payment_installments_value").isNull() |
                (F.col("_payment_installments_value") < 1)
            )
        )
        .select(
            F.lit(PAYMENTS_RUN_ID).alias("run_id"),
            F.lit(payments_batch_id).alias("batch_id"),
            F.lit("bronze.order_payments").alias("source_table"),
            F.lit("DQ_PAYMENTS_007").alias("rule_id"),
            F.lit("ERROR").alias("severity"),
            F.concat_ws(
                "|",
                F.col("order_id"),
                F.col("payment_sequential")
            ).alias("record_key"),
            F.lit(
                "Credit card payment does not have "
                "a valid number of installments"
            ).alias("rejection_reason"),
            F.to_json(
                F.struct(
                    *[F.col(column) for column in payments_df.columns]
                )
            ).alias("record_payload"),
            F.current_timestamp().alias("rejected_timestamp")
        )
)

print(
    "Rejected payment records found:",
    rejected_payments_df.count()
)

display(
    rejected_payments_df.select(
        "record_key",
        "rejection_reason"
    )
)


# In[95]:


# Save the rejected payment records in the quarantine table

(
    rejected_payments_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.rejected_records")
)

print(
    "The rejected Order Payments records "
    "were saved successfully."
)


# In[96]:


import uuid
from datetime import datetime
from pyspark.sql import functions as F

# Generate a unique ID for this data quality run
REVIEWS_RUN_ID = str(uuid.uuid4())

# Save the execution timestamp
REVIEWS_EXECUTION_TIMESTAMP = datetime.now()

# Read the Bronze Order Reviews table
reviews_df = spark.table("bronze.order_reviews")

# Count the total number of rows
reviews_rows_checked = reviews_df.count()

# Get the batch ID
reviews_batch_id = (
    reviews_df
        .filter(F.col("_batch_id").isNotNull())
        .select("_batch_id")
        .first()["_batch_id"]
)

# Convert score and date columns
reviews_prepared_df = (
    reviews_df
        .withColumn(
            "_review_score_value",
            F.col("review_score").cast("integer")
        )
        .withColumn(
            "_review_creation_timestamp",
            F.to_timestamp("review_creation_date")
        )
        .withColumn(
            "_review_answer_timestamp_value",
            F.to_timestamp("review_answer_timestamp")
        )
)

print("Run ID:", REVIEWS_RUN_ID)
print("Batch ID:", reviews_batch_id)
print("Rows checked:", reviews_rows_checked)


# In[97]:


# Rule 1:
# The review ID must not be null or empty

missing_review_id = (
    reviews_df
        .filter(
            F.col("review_id").isNull() |
            (F.trim(F.col("review_id")) == "")
        )
        .count()
)


# Rule 2:
# The order ID must not be null or empty

missing_review_order_id = (
    reviews_df
        .filter(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == "")
        )
        .count()
)


# Rule 3:
# Check whether the review ID is repeated

duplicate_review_ids_df = (
    reviews_df
        .groupBy("review_id")
        .count()
        .filter(
            F.col("review_id").isNotNull() &
            (F.col("count") > 1)
        )
        .select("review_id")
)

duplicate_review_id_rows = (
    reviews_df
        .join(
            duplicate_review_ids_df,
            on="review_id",
            how="inner"
        )
        .count()
)


# Rule 4:
# The combination of review ID and order ID must be unique

duplicate_review_keys_df = (
    reviews_df
        .groupBy(
            "review_id",
            "order_id"
        )
        .count()
        .filter(F.col("count") > 1)
        .select(
            "review_id",
            "order_id"
        )
)

duplicate_review_key_rows = (
    reviews_df
        .join(
            duplicate_review_keys_df,
            on=[
                "review_id",
                "order_id"
            ],
            how="inner"
        )
        .count()
)


# Rule 5:
# The order ID must exist in the Orders table

valid_orders_df = (
    spark.table("bronze.orders")
        .select("order_id")
        .distinct()
)

reviews_without_order = (
    reviews_df
        .join(
            valid_orders_df,
            on="order_id",
            how="left_anti"
        )
        .count()
)


print("Missing Review IDs:", missing_review_id)
print("Missing Order IDs:", missing_review_order_id)
print("Rows with repeated Review IDs:", duplicate_review_id_rows)
print("Rows with duplicate review keys:", duplicate_review_key_rows)
print("Reviews without a valid order:", reviews_without_order)


# In[98]:


# Rule 6:
# The review score must be between 1 and 5

invalid_review_score = (
    reviews_prepared_df
        .filter(
            F.col("_review_score_value").isNull() |
            (F.col("_review_score_value") < 1) |
            (F.col("_review_score_value") > 5)
        )
        .count()
)


# Rule 7:
# The review creation date must be valid

invalid_review_creation_date = (
    reviews_prepared_df
        .filter(
            F.col("review_creation_date").isNull() |
            (F.trim(F.col("review_creation_date")) == "") |
            F.col("_review_creation_timestamp").isNull()
        )
        .count()
)


# Rule 8:
# The review answer timestamp must be valid

invalid_review_answer_timestamp = (
    reviews_prepared_df
        .filter(
            F.col("review_answer_timestamp").isNull() |
            (F.trim(F.col("review_answer_timestamp")) == "") |
            F.col("_review_answer_timestamp_value").isNull()
        )
        .count()
)


# Rule 9:
# The answer timestamp cannot be before the creation date

answer_before_creation = (
    reviews_prepared_df
        .filter(
            F.col("_review_creation_timestamp").isNotNull() &
            F.col("_review_answer_timestamp_value").isNotNull() &
            (
                F.col("_review_answer_timestamp_value") <
                F.col("_review_creation_timestamp")
            )
        )
        .count()
)


print("Invalid review scores:", invalid_review_score)
print("Invalid review creation dates:", invalid_review_creation_date)
print(
    "Invalid review answer timestamps:",
    invalid_review_answer_timestamp
)
print(
    "Answers before review creation:",
    answer_before_creation
)


# In[99]:


# Create an empty list for the quality results

reviews_quality_results = []


# Create a function that adds one rule result

def add_review_result(
    rule_id,
    rule_name,
    column_name,
    severity,
    failed_rows
):

    # Calculate the failed percentage

    if reviews_rows_checked > 0:
        failed_percentage = (
            failed_rows / reviews_rows_checked
        ) * 100
    else:
        failed_percentage = 0.0

    # Determine the rule status

    if failed_rows == 0:
        status = "PASS"
    elif severity == "WARNING":
        status = "WARNING"
    else:
        status = "FAIL"

    # Add the result to the list

    reviews_quality_results.append(
        (
            REVIEWS_RUN_ID,
            reviews_batch_id,
            "bronze.order_reviews",
            rule_id,
            rule_name,
            column_name,
            severity,
            reviews_rows_checked,
            failed_rows,
            failed_percentage,
            status,
            REVIEWS_EXECUTION_TIMESTAMP
        )
    )


# Add the nine Order Reviews rules

add_review_result(
    "DQ_REVIEWS_001",
    "Review ID must not be null",
    "review_id",
    "ERROR",
    missing_review_id
)

add_review_result(
    "DQ_REVIEWS_002",
    "Order ID must not be null",
    "order_id",
    "ERROR",
    missing_review_order_id
)

add_review_result(
    "DQ_REVIEWS_003",
    "Review ID should be unique",
    "review_id",
    "WARNING",
    duplicate_review_id_rows
)

add_review_result(
    "DQ_REVIEWS_004",
    "Review and order ID combination must be unique",
    "review_id, order_id",
    "ERROR",
    duplicate_review_key_rows
)

add_review_result(
    "DQ_REVIEWS_005",
    "Order ID must exist in the Orders table",
    "order_id",
    "ERROR",
    reviews_without_order
)

add_review_result(
    "DQ_REVIEWS_006",
    "Review score must be between 1 and 5",
    "review_score",
    "ERROR",
    invalid_review_score
)

add_review_result(
    "DQ_REVIEWS_007",
    "Review creation date must be valid",
    "review_creation_date",
    "ERROR",
    invalid_review_creation_date
)

add_review_result(
    "DQ_REVIEWS_008",
    "Review answer timestamp must be valid",
    "review_answer_timestamp",
    "ERROR",
    invalid_review_answer_timestamp
)

add_review_result(
    "DQ_REVIEWS_009",
    "Review answer cannot be before review creation",
    "review_answer_timestamp",
    "ERROR",
    answer_before_creation
)

print("Rules prepared:", len(reviews_quality_results))


# In[100]:


# Convert the results into a Spark DataFrame

reviews_results_df = spark.createDataFrame(
    reviews_quality_results,
    """
    run_id STRING,
    batch_id STRING,
    table_name STRING,
    rule_id STRING,
    rule_name STRING,
    column_name STRING,
    severity STRING,
    rows_checked LONG,
    failed_rows LONG,
    failed_percentage DOUBLE,
    status STRING,
    execution_timestamp TIMESTAMP
    """
)


# Display the data quality results

display(
    reviews_results_df
        .select(
            "rule_id",
            "rule_name",
            "severity",
            "rows_checked",
            "failed_rows",
            "failed_percentage",
            "status"
        )
        .orderBy("rule_id")
)


# In[101]:


# Save the Order Reviews results in the audit table

(
    reviews_results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.data_quality_results")
)

print(
    "The data quality results for bronze.order_reviews "
    "were saved successfully."
)


# In[102]:


import uuid
from datetime import datetime
from pyspark.sql import functions as F

# Generate a unique ID for this data quality run
PRODUCTS_RUN_ID = str(uuid.uuid4())

# Save the execution timestamp
PRODUCTS_EXECUTION_TIMESTAMP = datetime.now()

# Read the Bronze Products table
products_df = spark.table("bronze.products")

# Count the total number of rows
products_rows_checked = products_df.count()

# Get the batch ID
products_batch_id = (
    products_df
        .filter(F.col("_batch_id").isNotNull())
        .select("_batch_id")
        .first()["_batch_id"]
)

# Convert the product attribute columns to numeric values
products_prepared_df = (
    products_df
        .withColumn(
            "_name_length_value",
            F.col("product_name_lenght").cast("double")
        )
        .withColumn(
            "_description_length_value",
            F.col("product_description_lenght").cast("double")
        )
        .withColumn(
            "_photos_quantity_value",
            F.col("product_photos_qty").cast("double")
        )
        .withColumn(
            "_weight_value",
            F.col("product_weight_g").cast("double")
        )
        .withColumn(
            "_length_value",
            F.col("product_length_cm").cast("double")
        )
        .withColumn(
            "_height_value",
            F.col("product_height_cm").cast("double")
        )
        .withColumn(
            "_width_value",
            F.col("product_width_cm").cast("double")
        )
)

print("Run ID:", PRODUCTS_RUN_ID)
print("Batch ID:", products_batch_id)
print("Rows checked:", products_rows_checked)


# In[103]:


# Rule 1:
# The product ID must not be null or empty

missing_product_id = (
    products_df
        .filter(
            F.col("product_id").isNull() |
            (F.trim(F.col("product_id")) == "")
        )
        .count()
)


# Rule 2:
# The product ID must be unique

duplicate_product_ids_df = (
    products_df
        .groupBy("product_id")
        .count()
        .filter(
            F.col("product_id").isNotNull() &
            (F.col("count") > 1)
        )
        .select("product_id")
)

duplicate_product_id_rows = (
    products_df
        .join(
            duplicate_product_ids_df,
            on="product_id",
            how="inner"
        )
        .count()
)


# Rule 3:
# The product category should not be null or empty

missing_product_category = (
    products_df
        .filter(
            F.col("product_category_name").isNull() |
            (F.trim(F.col("product_category_name")) == "")
        )
        .count()
)


# Rule 4:
# The product category should exist in the translation table

product_categories_df = (
    products_df
        .filter(
            F.col("product_category_name").isNotNull() &
            (F.trim(F.col("product_category_name")) != "")
        )
        .select(
            F.trim("product_category_name")
                .alias("product_category_name")
        )
)

valid_translation_categories_df = (
    spark.table("bronze.product_category_translation")
        .filter(
            F.col("product_category_name").isNotNull() &
            (F.trim(F.col("product_category_name")) != "")
        )
        .select(
            F.trim("product_category_name")
                .alias("product_category_name")
        )
        .distinct()
)

products_without_translation_df = (
    product_categories_df
        .join(
            valid_translation_categories_df,
            on="product_category_name",
            how="left_anti"
        )
)

products_without_translation = (
    products_without_translation_df.count()
)


print("Missing Product IDs:", missing_product_id)
print("Rows with duplicate Product IDs:", duplicate_product_id_rows)
print("Products without a category:", missing_product_category)
print(
    "Products without a category translation:",
    products_without_translation
)

display(
    products_without_translation_df
        .groupBy("product_category_name")
        .count()
        .orderBy("product_category_name")
)


# In[104]:


# Rule 5:
# Product descriptive attributes should contain positive values

invalid_descriptive_attributes = (
    products_prepared_df
        .filter(
            F.col("_name_length_value").isNull() |
            (F.col("_name_length_value") <= 0) |
            F.col("_description_length_value").isNull() |
            (F.col("_description_length_value") <= 0) |
            F.col("_photos_quantity_value").isNull() |
            (F.col("_photos_quantity_value") <= 0)
        )
        .count()
)


# Rule 6:
# Product weight should be greater than zero

invalid_product_weight = (
    products_prepared_df
        .filter(
            F.col("_weight_value").isNull() |
            (F.col("_weight_value") <= 0)
        )
        .count()
)


# Rule 7:
# Product dimensions should be greater than zero

invalid_product_dimensions = (
    products_prepared_df
        .filter(
            F.col("_length_value").isNull() |
            (F.col("_length_value") <= 0) |
            F.col("_height_value").isNull() |
            (F.col("_height_value") <= 0) |
            F.col("_width_value").isNull() |
            (F.col("_width_value") <= 0)
        )
        .count()
)


print(
    "Products with invalid descriptive attributes:",
    invalid_descriptive_attributes
)
print("Products with an invalid weight:", invalid_product_weight)
print(
    "Products with invalid dimensions:",
    invalid_product_dimensions
)


# In[105]:


# Create an empty list for the quality results

products_quality_results = []


# Create a function that adds one rule result

def add_product_result(
    rule_id,
    rule_name,
    column_name,
    severity,
    failed_rows
):

    # Calculate the failed percentage

    if products_rows_checked > 0:
        failed_percentage = (
            failed_rows / products_rows_checked
        ) * 100
    else:
        failed_percentage = 0.0

    # Determine the rule status

    if failed_rows == 0:
        status = "PASS"
    elif severity == "WARNING":
        status = "WARNING"
    else:
        status = "FAIL"

    # Add the result to the list

    products_quality_results.append(
        (
            PRODUCTS_RUN_ID,
            products_batch_id,
            "bronze.products",
            rule_id,
            rule_name,
            column_name,
            severity,
            products_rows_checked,
            failed_rows,
            failed_percentage,
            status,
            PRODUCTS_EXECUTION_TIMESTAMP
        )
    )


# Add the seven Products rules

add_product_result(
    "DQ_PRODUCTS_001",
    "Product ID must not be null",
    "product_id",
    "ERROR",
    missing_product_id
)

add_product_result(
    "DQ_PRODUCTS_002",
    "Product ID must be unique",
    "product_id",
    "ERROR",
    duplicate_product_id_rows
)

add_product_result(
    "DQ_PRODUCTS_003",
    "Product category should not be null",
    "product_category_name",
    "WARNING",
    missing_product_category
)

add_product_result(
    "DQ_PRODUCTS_004",
    "Product category should have an English translation",
    "product_category_name",
    "WARNING",
    products_without_translation
)

add_product_result(
    "DQ_PRODUCTS_005",
    "Product descriptive attributes should be positive",
    (
        "product_name_lenght, "
        "product_description_lenght, "
        "product_photos_qty"
    ),
    "WARNING",
    invalid_descriptive_attributes
)

add_product_result(
    "DQ_PRODUCTS_006",
    "Product weight should be greater than zero",
    "product_weight_g",
    "WARNING",
    invalid_product_weight
)

add_product_result(
    "DQ_PRODUCTS_007",
    "Product dimensions should be greater than zero",
    "product_length_cm, product_height_cm, product_width_cm",
    "WARNING",
    invalid_product_dimensions
)

print("Rules prepared:", len(products_quality_results))


# In[106]:


# Convert the results into a Spark DataFrame

products_results_df = spark.createDataFrame(
    products_quality_results,
    """
    run_id STRING,
    batch_id STRING,
    table_name STRING,
    rule_id STRING,
    rule_name STRING,
    column_name STRING,
    severity STRING,
    rows_checked LONG,
    failed_rows LONG,
    failed_percentage DOUBLE,
    status STRING,
    execution_timestamp TIMESTAMP
    """
)


# Display the data quality results

display(
    products_results_df
        .select(
            "rule_id",
            "rule_name",
            "severity",
            "rows_checked",
            "failed_rows",
            "failed_percentage",
            "status"
        )
        .orderBy("rule_id")
)


# In[107]:


# Save the Products results in the audit table

(
    products_results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.data_quality_results")
)

print(
    "The data quality results for bronze.products "
    "were saved successfully."
)


# In[108]:


import uuid
from datetime import datetime
from pyspark.sql import functions as F

# Generate a unique ID for this data quality run
TRANSLATION_RUN_ID = str(uuid.uuid4())

# Save the execution timestamp
TRANSLATION_EXECUTION_TIMESTAMP = datetime.now()

# Read the Bronze Product Category Translation table
translation_df = spark.table(
    "bronze.product_category_translation"
)

# Count the total number of rows
translation_rows_checked = translation_df.count()

# Get the batch ID
translation_batch_id = (
    translation_df
        .filter(F.col("_batch_id").isNotNull())
        .select("_batch_id")
        .first()["_batch_id"]
)

print("Run ID:", TRANSLATION_RUN_ID)
print("Batch ID:", translation_batch_id)
print("Rows checked:", translation_rows_checked)


# In[109]:


# Rule 1:
# The Portuguese category name must not be null or empty

missing_portuguese_category = (
    translation_df
        .filter(
            F.col("product_category_name").isNull() |
            (
                F.trim(
                    F.col("product_category_name")
                ) == ""
            )
        )
        .count()
)


# Rule 2:
# The English category name must not be null or empty

missing_english_category = (
    translation_df
        .filter(
            F.col(
                "product_category_name_english"
            ).isNull() |
            (
                F.trim(
                    F.col(
                        "product_category_name_english"
                    )
                ) == ""
            )
        )
        .count()
)

print(
    "Missing Portuguese category names:",
    missing_portuguese_category
)

print(
    "Missing English category names:",
    missing_english_category
)


# In[110]:


# Rule 3:
# The Portuguese category name must be unique

duplicate_portuguese_categories_df = (
    translation_df
        .groupBy("product_category_name")
        .count()
        .filter(
            F.col("product_category_name").isNotNull() &
            (F.col("count") > 1)
        )
        .select("product_category_name")
)

duplicate_portuguese_category_rows = (
    translation_df
        .join(
            duplicate_portuguese_categories_df,
            on="product_category_name",
            how="inner"
        )
        .count()
)


# Rule 4:
# The English category name must be unique

duplicate_english_categories_df = (
    translation_df
        .groupBy(
            "product_category_name_english"
        )
        .count()
        .filter(
            F.col(
                "product_category_name_english"
            ).isNotNull() &
            (F.col("count") > 1)
        )
        .select(
            "product_category_name_english"
        )
)

duplicate_english_category_rows = (
    translation_df
        .join(
            duplicate_english_categories_df,
            on="product_category_name_english",
            how="inner"
        )
        .count()
)


# Rule 5:
# Category names must use lowercase letters,
# numbers and underscores

invalid_category_format = (
    translation_df
        .filter(
            (
                ~F.trim(
                    F.col("product_category_name")
                ).rlike("^[a-z0-9_]+$")
            ) |
            (
                ~F.trim(
                    F.col(
                        "product_category_name_english"
                    )
                ).rlike("^[a-z0-9_]+$")
            )
        )
        .count()
)

print(
    "Rows with duplicate Portuguese categories:",
    duplicate_portuguese_category_rows
)

print(
    "Rows with duplicate English categories:",
    duplicate_english_category_rows
)

print(
    "Rows with an invalid category format:",
    invalid_category_format
)


# In[111]:


# Create an empty list for the quality results

translation_quality_results = []


# Create a function that adds one rule result

def add_translation_result(
    rule_id,
    rule_name,
    column_name,
    severity,
    failed_rows
):

    # Calculate the failed percentage

    if translation_rows_checked > 0:
        failed_percentage = (
            failed_rows / translation_rows_checked
        ) * 100
    else:
        failed_percentage = 0.0

    # Determine the rule status

    if failed_rows == 0:
        status = "PASS"
    elif severity == "WARNING":
        status = "WARNING"
    else:
        status = "FAIL"

    # Add the result to the list

    translation_quality_results.append(
        (
            TRANSLATION_RUN_ID,
            translation_batch_id,
            "bronze.product_category_translation",
            rule_id,
            rule_name,
            column_name,
            severity,
            translation_rows_checked,
            failed_rows,
            failed_percentage,
            status,
            TRANSLATION_EXECUTION_TIMESTAMP
        )
    )


# Add the five translation rules

add_translation_result(
    "DQ_TRANSLATION_001",
    "Portuguese category name must not be null",
    "product_category_name",
    "ERROR",
    missing_portuguese_category
)

add_translation_result(
    "DQ_TRANSLATION_002",
    "English category name must not be null",
    "product_category_name_english",
    "ERROR",
    missing_english_category
)

add_translation_result(
    "DQ_TRANSLATION_003",
    "Portuguese category name must be unique",
    "product_category_name",
    "ERROR",
    duplicate_portuguese_category_rows
)

add_translation_result(
    "DQ_TRANSLATION_004",
    "English category name must be unique",
    "product_category_name_english",
    "ERROR",
    duplicate_english_category_rows
)

add_translation_result(
    "DQ_TRANSLATION_005",
    "Category names must follow the standard format",
    (
        "product_category_name, "
        "product_category_name_english"
    ),
    "WARNING",
    invalid_category_format
)

print(
    "Rules prepared:",
    len(translation_quality_results)
)


# In[112]:


# Convert the results into a Spark DataFrame

translation_results_df = spark.createDataFrame(
    translation_quality_results,
    """
    run_id STRING,
    batch_id STRING,
    table_name STRING,
    rule_id STRING,
    rule_name STRING,
    column_name STRING,
    severity STRING,
    rows_checked LONG,
    failed_rows LONG,
    failed_percentage DOUBLE,
    status STRING,
    execution_timestamp TIMESTAMP
    """
)


# Display the data quality results

display(
    translation_results_df
        .select(
            "rule_id",
            "rule_name",
            "severity",
            "rows_checked",
            "failed_rows",
            "failed_percentage",
            "status"
        )
        .orderBy("rule_id")
)


# In[113]:


# Save the Product Category Translation results
# in the audit table

(
    translation_results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "audit.data_quality_results"
        )
)

print(
    "The data quality results for "
    "bronze.product_category_translation "
    "were saved successfully."
)


# In[114]:


import uuid
from datetime import datetime
from pyspark.sql import functions as F

# Generate a unique ID for this data quality run
SELLERS_RUN_ID = str(uuid.uuid4())

# Save the execution timestamp
SELLERS_EXECUTION_TIMESTAMP = datetime.now()

# Read the Bronze Sellers table
sellers_df = spark.table("bronze.sellers")

# Count the total number of rows
sellers_rows_checked = sellers_df.count()

# Get the batch ID
sellers_batch_id = (
    sellers_df
        .filter(F.col("_batch_id").isNotNull())
        .select("_batch_id")
        .first()["_batch_id"]
)

print("Run ID:", SELLERS_RUN_ID)
print("Batch ID:", sellers_batch_id)
print("Rows checked:", sellers_rows_checked)


# In[115]:


# Rule 1:
# The seller ID must not be null or empty

missing_seller_id = (
    sellers_df
        .filter(
            F.col("seller_id").isNull() |
            (F.trim(F.col("seller_id")) == "")
        )
        .count()
)


# Rule 2:
# The seller ID must be unique

duplicate_seller_ids_df = (
    sellers_df
        .groupBy("seller_id")
        .count()
        .filter(
            F.col("seller_id").isNotNull() &
            (F.col("count") > 1)
        )
        .select("seller_id")
)

duplicate_seller_id_rows = (
    sellers_df
        .join(
            duplicate_seller_ids_df,
            on="seller_id",
            how="inner"
        )
        .count()
)


print("Missing Seller IDs:", missing_seller_id)
print(
    "Rows with duplicate Seller IDs:",
    duplicate_seller_id_rows
)


# In[116]:


# Rule 3:
# The ZIP code prefix must contain exactly five digits

invalid_seller_zip_code = (
    sellers_df
        .filter(
            F.col("seller_zip_code_prefix").isNull() |
            (
                F.trim(
                    F.col("seller_zip_code_prefix")
                ) == ""
            ) |
            (
                ~F.trim(
                    F.col("seller_zip_code_prefix")
                ).rlike("^[0-9]{5}$")
            )
        )
        .count()
)


# Rule 4:
# The seller city must not be null or empty

missing_seller_city = (
    sellers_df
        .filter(
            F.col("seller_city").isNull() |
            (F.trim(F.col("seller_city")) == "")
        )
        .count()
)


# Rule 5:
# The seller city should not contain
# an email address or website

suspicious_seller_city = (
    sellers_df
        .filter(
            F.lower(
                F.trim(F.col("seller_city"))
            ).contains("@") |
            F.lower(
                F.trim(F.col("seller_city"))
            ).contains("http") |
            F.lower(
                F.trim(F.col("seller_city"))
            ).contains("www.")
        )
        .count()
)


# Rule 6:
# The seller state must be a valid Brazilian state code

valid_brazilian_states = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF",
    "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS",
    "RO", "RR", "SC", "SP", "SE", "TO"
]

invalid_seller_state = (
    sellers_df
        .filter(
            F.col("seller_state").isNull() |
            (
                ~F.upper(
                    F.trim(F.col("seller_state"))
                ).isin(valid_brazilian_states)
            )
        )
        .count()
)


print(
    "Invalid Seller ZIP code prefixes:",
    invalid_seller_zip_code
)
print("Missing Seller cities:", missing_seller_city)
print("Suspicious Seller cities:", suspicious_seller_city)
print("Invalid Seller states:", invalid_seller_state)


# In[117]:


display(
    sellers_df
        .filter(
            F.lower(
                F.trim(F.col("seller_city"))
            ).contains("@") |
            F.lower(
                F.trim(F.col("seller_city"))
            ).contains("http") |
            F.lower(
                F.trim(F.col("seller_city"))
            ).contains("www.")
        )
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        )
)


# In[118]:


# Create an empty list for the quality results

sellers_quality_results = []


# Create a function that adds one rule result

def add_seller_result(
    rule_id,
    rule_name,
    column_name,
    severity,
    failed_rows
):

    # Calculate the failed percentage

    if sellers_rows_checked > 0:
        failed_percentage = (
            failed_rows / sellers_rows_checked
        ) * 100
    else:
        failed_percentage = 0.0

    # Determine the rule status

    if failed_rows == 0:
        status = "PASS"
    elif severity == "WARNING":
        status = "WARNING"
    else:
        status = "FAIL"

    # Add the result to the list

    sellers_quality_results.append(
        (
            SELLERS_RUN_ID,
            sellers_batch_id,
            "bronze.sellers",
            rule_id,
            rule_name,
            column_name,
            severity,
            sellers_rows_checked,
            failed_rows,
            failed_percentage,
            status,
            SELLERS_EXECUTION_TIMESTAMP
        )
    )


# Add the six Sellers rules

add_seller_result(
    "DQ_SELLERS_001",
    "Seller ID must not be null",
    "seller_id",
    "ERROR",
    missing_seller_id
)

add_seller_result(
    "DQ_SELLERS_002",
    "Seller ID must be unique",
    "seller_id",
    "ERROR",
    duplicate_seller_id_rows
)

add_seller_result(
    "DQ_SELLERS_003",
    "Seller ZIP code prefix must contain five digits",
    "seller_zip_code_prefix",
    "ERROR",
    invalid_seller_zip_code
)

add_seller_result(
    "DQ_SELLERS_004",
    "Seller city must not be null",
    "seller_city",
    "ERROR",
    missing_seller_city
)

add_seller_result(
    "DQ_SELLERS_005",
    "Seller city should contain a valid city name",
    "seller_city",
    "WARNING",
    suspicious_seller_city
)

add_seller_result(
    "DQ_SELLERS_006",
    "Seller state must be a valid Brazilian state code",
    "seller_state",
    "ERROR",
    invalid_seller_state
)

print("Rules prepared:", len(sellers_quality_results))


# In[119]:


# Convert the results into a Spark DataFrame

sellers_results_df = spark.createDataFrame(
    sellers_quality_results,
    """
    run_id STRING,
    batch_id STRING,
    table_name STRING,
    rule_id STRING,
    rule_name STRING,
    column_name STRING,
    severity STRING,
    rows_checked LONG,
    failed_rows LONG,
    failed_percentage DOUBLE,
    status STRING,
    execution_timestamp TIMESTAMP
    """
)


# Display the data quality results

display(
    sellers_results_df
        .select(
            "rule_id",
            "rule_name",
            "severity",
            "rows_checked",
            "failed_rows",
            "failed_percentage",
            "status"
        )
        .orderBy("rule_id")
)


# In[120]:


# Save the Sellers results in the audit table

(
    sellers_results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.data_quality_results")
)

print(
    "The data quality results for bronze.sellers "
    "were saved successfully."
)


# In[121]:


import uuid
from datetime import datetime
from pyspark.sql import functions as F

# Generate a unique ID for this data quality run
GEOLOCATION_RUN_ID = str(uuid.uuid4())

# Save the execution timestamp
GEOLOCATION_EXECUTION_TIMESTAMP = datetime.now()

# Read the Bronze Geolocation table
geolocation_df = spark.table("bronze.geolocation")

# Count the total number of rows
geolocation_rows_checked = geolocation_df.count()

# Get the batch ID
geolocation_batch_id = (
    geolocation_df
        .filter(F.col("_batch_id").isNotNull())
        .select("_batch_id")
        .first()["_batch_id"]
)

# Prepare standardized values for validation
geolocation_prepared_df = (
    geolocation_df
        .withColumn(
            "_zip_code_value",
            F.trim(
                F.col("geolocation_zip_code_prefix")
                    .cast("string")
            )
        )
        .withColumn(
            "_latitude_value",
            F.col("geolocation_lat").cast("double")
        )
        .withColumn(
            "_longitude_value",
            F.col("geolocation_lng").cast("double")
        )
        .withColumn(
            "_city_value",
            F.trim(F.col("geolocation_city"))
        )
        .withColumn(
            "_state_value",
            F.upper(
                F.trim(F.col("geolocation_state"))
            )
        )
)

print("Run ID:", GEOLOCATION_RUN_ID)
print("Batch ID:", geolocation_batch_id)
print("Rows checked:", geolocation_rows_checked)


# In[122]:


# Rule 1:
# The ZIP code prefix must contain exactly five digits

invalid_geolocation_zip_code = (
    geolocation_prepared_df
        .filter(
            F.col("_zip_code_value").isNull() |
            (F.col("_zip_code_value") == "") |
            (
                ~F.col("_zip_code_value")
                    .rlike("^[0-9]{5}$")
            )
        )
        .count()
)


# Rule 2:
# The city must not be null or empty

missing_geolocation_city = (
    geolocation_prepared_df
        .filter(
            F.col("_city_value").isNull() |
            (F.col("_city_value") == "")
        )
        .count()
)


# Rule 3:
# The state must be a valid Brazilian state code

valid_brazilian_states = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF",
    "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS",
    "RO", "RR", "SC", "SP", "SE", "TO"
]

invalid_geolocation_state = (
    geolocation_prepared_df
        .filter(
            F.col("_state_value").isNull() |
            (
                ~F.col("_state_value")
                    .isin(valid_brazilian_states)
            )
        )
        .count()
)

print(
    "Invalid Geolocation ZIP code prefixes:",
    invalid_geolocation_zip_code
)
print(
    "Missing Geolocation cities:",
    missing_geolocation_city
)
print(
    "Invalid Geolocation states:",
    invalid_geolocation_state
)


# In[123]:


# Rule 4:
# Latitude and longitude must contain valid numeric values

invalid_coordinate_values = (
    geolocation_prepared_df
        .filter(
            F.col("_latitude_value").isNull() |
            F.col("_longitude_value").isNull() |
            (F.col("_latitude_value") < -90) |
            (F.col("_latitude_value") > 90) |
            (F.col("_longitude_value") < -180) |
            (F.col("_longitude_value") > 180)
        )
        .count()
)


# Rule 5:
# Coordinates should be inside the broad boundaries of Brazil

coordinates_outside_brazil_df = (
    geolocation_prepared_df
        .filter(
            F.col("_latitude_value").isNotNull() &
            F.col("_longitude_value").isNotNull() &
            (
                (F.col("_latitude_value") < -35) |
                (F.col("_latitude_value") > 6) |
                (F.col("_longitude_value") < -75) |
                (F.col("_longitude_value") > -32)
            )
        )
)

coordinates_outside_brazil = (
    coordinates_outside_brazil_df.count()
)

print(
    "Invalid coordinate values:",
    invalid_coordinate_values
)
print(
    "Coordinates outside Brazil:",
    coordinates_outside_brazil
)


# In[124]:


display(
    coordinates_outside_brazil_df
        .select(
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state"
        )
        .orderBy("geolocation_zip_code_prefix")
)


# In[125]:


# Rule 6:
# Exact business records should be unique

geolocation_business_columns = [
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state"
]

duplicate_geolocation_groups_df = (
    geolocation_df
        .groupBy(*geolocation_business_columns)
        .count()
        .filter(F.col("count") > 1)
)

duplicate_geolocation_rows = (
    duplicate_geolocation_groups_df
        .agg(
            F.sum("count")
                .cast("long")
                .alias("duplicate_rows")
        )
        .first()["duplicate_rows"]
)

if duplicate_geolocation_rows is None:
    duplicate_geolocation_rows = 0


# Rule 7:
# A ZIP code prefix should not belong to multiple states

ambiguous_zip_codes_df = (
    geolocation_prepared_df
        .groupBy("_zip_code_value")
        .agg(
            F.countDistinct("_state_value")
                .alias("state_count")
        )
        .filter(F.col("state_count") > 1)
        .select("_zip_code_value")
)

ambiguous_zip_code_rows = (
    geolocation_prepared_df
        .join(
            ambiguous_zip_codes_df,
            on="_zip_code_value",
            how="inner"
        )
        .count()
)

print(
    "Rows involved in exact duplicates:",
    duplicate_geolocation_rows
)
print(
    "Rows with ZIP codes assigned to multiple states:",
    ambiguous_zip_code_rows
)


# In[126]:


# Create an empty list for the quality results

geolocation_quality_results = []


# Create a function that adds one rule result

def add_geolocation_result(
    rule_id,
    rule_name,
    column_name,
    severity,
    failed_rows
):

    # Calculate the failed percentage

    if geolocation_rows_checked > 0:
        failed_percentage = (
            failed_rows / geolocation_rows_checked
        ) * 100
    else:
        failed_percentage = 0.0

    # Determine the rule status

    if failed_rows == 0:
        status = "PASS"
    elif severity == "WARNING":
        status = "WARNING"
    else:
        status = "FAIL"

    # Add the result to the list

    geolocation_quality_results.append(
        (
            GEOLOCATION_RUN_ID,
            geolocation_batch_id,
            "bronze.geolocation",
            rule_id,
            rule_name,
            column_name,
            severity,
            geolocation_rows_checked,
            failed_rows,
            failed_percentage,
            status,
            GEOLOCATION_EXECUTION_TIMESTAMP
        )
    )


# Add the seven Geolocation rules

add_geolocation_result(
    "DQ_GEOLOCATION_001",
    "ZIP code prefix must contain five digits",
    "geolocation_zip_code_prefix",
    "ERROR",
    invalid_geolocation_zip_code
)

add_geolocation_result(
    "DQ_GEOLOCATION_002",
    "Geolocation city must not be null",
    "geolocation_city",
    "ERROR",
    missing_geolocation_city
)

add_geolocation_result(
    "DQ_GEOLOCATION_003",
    "Geolocation state must be a valid Brazilian state",
    "geolocation_state",
    "ERROR",
    invalid_geolocation_state
)

add_geolocation_result(
    "DQ_GEOLOCATION_004",
    "Latitude and longitude must be valid numeric values",
    "geolocation_lat, geolocation_lng",
    "ERROR",
    invalid_coordinate_values
)

add_geolocation_result(
    "DQ_GEOLOCATION_005",
    "Coordinates must be located inside Brazil",
    "geolocation_lat, geolocation_lng",
    "ERROR",
    coordinates_outside_brazil
)

add_geolocation_result(
    "DQ_GEOLOCATION_006",
    "Exact geolocation records should be unique",
    (
        "geolocation_zip_code_prefix, "
        "geolocation_lat, geolocation_lng, "
        "geolocation_city, geolocation_state"
    ),
    "WARNING",
    duplicate_geolocation_rows
)

add_geolocation_result(
    "DQ_GEOLOCATION_007",
    "ZIP code prefix should belong to one state",
    (
        "geolocation_zip_code_prefix, "
        "geolocation_state"
    ),
    "WARNING",
    ambiguous_zip_code_rows
)

print(
    "Rules prepared:",
    len(geolocation_quality_results)
)


# In[127]:


geolocation_results_df = spark.createDataFrame(
    geolocation_quality_results,
    """
    run_id STRING,
    batch_id STRING,
    table_name STRING,
    rule_id STRING,
    rule_name STRING,
    column_name STRING,
    severity STRING,
    rows_checked LONG,
    failed_rows LONG,
    failed_percentage DOUBLE,
    status STRING,
    execution_timestamp TIMESTAMP
    """
)

display(
    geolocation_results_df
        .select(
            "rule_id",
            "rule_name",
            "severity",
            "rows_checked",
            "failed_rows",
            "failed_percentage",
            "status"
        )
        .orderBy("rule_id")
)


# In[128]:


(
    geolocation_results_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.data_quality_results")
)

print(
    "The data quality results for bronze.geolocation "
    "were saved successfully."
)


# In[129]:


rejected_geolocation_df = (
    coordinates_outside_brazil_df
        .select(
            F.lit(GEOLOCATION_RUN_ID).alias("run_id"),
            F.lit(geolocation_batch_id).alias("batch_id"),
            F.lit(
                "bronze.geolocation"
            ).alias("source_table"),
            F.lit(
                "DQ_GEOLOCATION_005"
            ).alias("rule_id"),
            F.lit("ERROR").alias("severity"),
            F.concat_ws(
                "|",
                F.col("geolocation_zip_code_prefix"),
                F.col("geolocation_lat"),
                F.col("geolocation_lng")
            ).alias("record_key"),
            F.lit(
                "Coordinates are outside the expected "
                "geographical boundaries of Brazil"
            ).alias("rejection_reason"),
            F.to_json(
                F.struct(
                    *[
                        F.col(column)
                        for column in geolocation_df.columns
                    ]
                )
            ).alias("record_payload"),
            F.current_timestamp().alias(
                "rejected_timestamp"
            )
        )
)

print(
    "Rejected Geolocation records found:",
    rejected_geolocation_df.count()
)

display(
    rejected_geolocation_df.select(
        "record_key",
        "rejection_reason"
    )
)


# In[130]:


(
    rejected_geolocation_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("audit.rejected_records")
)

print(
    "The rejected Geolocation records "
    "were saved successfully."
)


# In[5]:


# Display all tables available in the Bronze schema
bronze_tables = spark.sql("SHOW TABLES IN bronze")

display(bronze_tables)


# In[6]:


# Display the complete Bronze table names
spark.sql("SHOW TABLES IN bronze") \
    .select("tableName") \
    .show(truncate=False)


# In[8]:


from pyspark.sql import functions as F


# Read the existing Data Quality results table
dq_results = spark.table("audit.data_quality_results")


# Display general information
print("Number of Data Quality results:", dq_results.count())

print("Data Quality table structure:")
dq_results.printSchema()


# Display the most recent validation results
display(
    dq_results.orderBy(
        F.desc("execution_timestamp")
    )
)

