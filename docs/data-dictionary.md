# Data dictionary

This document describes the source datasets and the tables created by the notebooks. Data types for Bronze are mostly inferred as strings by the CSV reader. Silver and Gold apply explicit analytical types.

## Raw source inventory

### Product category translation

Grain: one row per Portuguese product category. Source rows: 71.

| Column | Meaning |
| --- | --- |
| `product_category_name` | Product category in Portuguese; business key |
| `product_category_name_english` | English category translation |

### Sellers

Grain: one row per seller. Source rows: 3,095.

| Column | Meaning |
| --- | --- |
| `seller_id` | Seller business key |
| `seller_zip_code_prefix` | First five digits of the seller postal code |
| `seller_city` | Seller city |
| `seller_state` | Two-letter Brazilian state code |

### Products

Grain: one row per product. Source rows: 32,951.

| Column | Meaning |
| --- | --- |
| `product_id` | Product business key |
| `product_category_name` | Portuguese product category |
| `product_name_lenght` | Source spelling for product-name length |
| `product_description_lenght` | Source spelling for description length |
| `product_photos_qty` | Number of product photographs |
| `product_weight_g` | Weight in grams |
| `product_length_cm` | Length in centimetres |
| `product_height_cm` | Height in centimetres |
| `product_width_cm` | Width in centimetres |

There are 610 source products without category and descriptive attributes. Two products have missing physical measurements.

### Order payments

Grain: one payment record inside an order. Source rows: 103,886. Business key: `order_id` + `payment_sequential`.

| Column | Meaning |
| --- | --- |
| `order_id` | Related order |
| `payment_sequential` | Sequence of the payment record inside the order |
| `payment_type` | Credit card, debit card, boleto, voucher or undefined |
| `payment_installments` | Number of instalments |
| `payment_value` | Value of this payment record |

### Customers

Grain: one customer record used by an order. Source rows: 99,441.

| Column | Meaning |
| --- | --- |
| `customer_id` | Order-level customer key; primary key in the model |
| `customer_unique_id` | Identifier that can link repeat purchases by the same customer |
| `customer_zip_code_prefix` | First five digits of the customer postal code |
| `customer_city` | Customer city |
| `customer_state` | Two-letter Brazilian state code |

There are 96,096 distinct `customer_unique_id` values. This difference is why repeat-customer calculations use `customer_unique_id`, while the order relationship uses `customer_id`.

### Order reviews

Grain: one review-order combination. Source rows: 99,224. Business key: `review_id` + `order_id`.

| Column | Meaning |
| --- | --- |
| `review_id` | Review identifier; not unique by itself in the source |
| `order_id` | Related order |
| `review_score` | Score from 1 to 5 |
| `review_comment_title` | Optional title |
| `review_comment_message` | Optional written feedback |
| `review_creation_date` | Date on which the review was created |
| `review_answer_timestamp` | Timestamp of the answer/submission |

The source contains 98,410 distinct review IDs. The compound key remains unique. Most comment titles and many messages are null, which is expected for this dataset and is not treated as a reason to remove a review.

### Order items

Grain: one item line inside an order. Source rows: 112,650. Business key: `order_id` + `order_item_id`.

| Column | Meaning |
| --- | --- |
| `order_id` | Related order |
| `order_item_id` | Item sequence inside the order |
| `product_id` | Related product |
| `seller_id` | Related seller |
| `shipping_limit_date` | Seller shipping deadline |
| `price` | Product price excluding freight |
| `freight_value` | Freight charged for the item |

### Orders

Grain: one row per order. Source rows: 99,441.

| Column | Meaning |
| --- | --- |
| `order_id` | Order business key |
| `customer_id` | Related order-level customer record |
| `order_status` | Created, approved, processing, invoiced, shipped, delivered, unavailable or canceled |
| `order_purchase_timestamp` | Purchase timestamp |
| `order_approved_at` | Approval timestamp |
| `order_delivered_carrier_date` | Timestamp when the carrier received the order |
| `order_delivered_customer_date` | Customer delivery timestamp |
| `order_estimated_delivery_date` | Estimated delivery timestamp |

The source contains 96,478 delivered orders. Missing approval or delivery timestamps are preserved because they can be valid for orders that were canceled, unavailable or still in another status.

### Geolocation

Grain: one geographic observation. Source rows: 1,000,163.

| Column | Meaning |
| --- | --- |
| `geolocation_zip_code_prefix` | Postal-code prefix |
| `geolocation_lat` | Latitude |
| `geolocation_lng` | Longitude |
| `geolocation_city` | City |
| `geolocation_state` | Brazilian state code |

The raw source has 19,015 distinct postal-code prefixes, 27 states and 261,831 exact duplicate rows.

## Bronze audit columns

Every Bronze table is expected to contain the source columns plus these fields:

| Column | Type | Meaning |
| --- | --- | --- |
| `_ingestion_timestamp` | timestamp | Row ingestion timestamp |
| `_ingestion_date` | date | Calendar date of ingestion |
| `_source_file` | string | Input file name |
| `_source_dataset` | string | Logical source dataset/folder |
| `_batch_id` | string | UTC batch ID formatted as `yyyyMMddTHHmmssZ` |

## Silver tables

| Table | Key | Main transformations |
| --- | --- | --- |
| `silver.customers` | `customer_id` | Trim IDs, normalise city/state, format ZIP as five-character text, deduplicate |
| `silver.orders` | `order_id` | Trim IDs/status, lowercase status, cast five timestamps, deduplicate |
| `silver.order_items` | `order_id` + `order_item_id` | Cast item ID, timestamp and decimal values; remove invalid IDs and negative values |
| `silver.order_payments` | `order_id` + `payment_sequential` | Normalise payment type; cast sequence, instalments and decimal value |
| `silver.order_reviews` | `review_id` + `order_id` | Trim comments, replace empty text with null, cast score/date/timestamp, validate score |
| `silver.products` | `product_id` | Normalise category, use `unknown`, cast metrics, correct two source column names |
| `silver.product_category_translation` | `product_category_name` | Lowercase and trim both category fields, remove missing and duplicate categories |
| `silver.sellers` | `seller_id` | Normalise city/state, cast ZIP prefix, validate required values, deduplicate |
| `silver.geolocation` | Five geographic fields | Cast ZIP/coordinates, normalise city/state, validate ranges, remove exact duplicates |

Silver retains the Bronze audit fields unless a transformation explicitly selects a smaller set. In this notebook most transformations start from the complete Bronze DataFrame and use `withColumn`, so audit lineage remains available in Silver.

## Gold table summary

| Table | Grain | Key | Columns created by Gold notebook |
| --- | --- | --- | ---: |
| `gold.dim_date` | Calendar day | `date_key` | 14 |
| `gold.dim_customer` | Customer record | `customer_id` | 6 |
| `gold.dim_product` | Product | `product_id` | 14 |
| `gold.dim_seller` | Seller | `seller_id` | 5 |
| `gold.fact_orders` | Order | `order_id` | 27 |
| `gold.fact_order_items` | Order item | `order_id` + `order_item_id` | 11 |
| `gold.fact_order_payments` | Payment record | `order_id` + `payment_sequential` | 10 |
| `gold.fact_order_reviews` | Review-order record | `review_id` + `order_id` | 16 |

## `gold.dim_date`

The calendar starts on 1 January of the minimum purchase year and ends on 31 December of the maximum estimated-delivery year.

| Column | Type | Meaning |
| --- | --- | --- |
| `date_key` | int | Surrogate-style key in `yyyyMMdd` format |
| `date` | date | Calendar date |
| `year` | int | Calendar year |
| `quarter_number` | int | Quarter from 1 to 4 |
| `quarter_name` | string | `Q1` to `Q4` |
| `year_quarter` | string | Example: `2018-Q3` |
| `month_number` | int | Month from 1 to 12 |
| `month_name` | string | Full month name |
| `year_month` | string | Sortable label such as `2018-07` |
| `week_of_year` | int | Week number |
| `day_of_month` | int | Day from 1 to 31 |
| `day_of_week_number` | int | Monday = 1, Sunday = 7 |
| `day_name` | string | Full weekday name |
| `is_weekend` | boolean | True for Saturday and Sunday |

## `gold.dim_customer`

| Column | Type | Meaning |
| --- | --- | --- |
| `customer_id` | string | Primary key used by `fact_orders` |
| `customer_unique_id` | string | Identifier used for distinct/repeat customer analysis |
| `customer_zip_code_prefix` | int | Postal-code prefix |
| `customer_city` | string | City in title case |
| `customer_state` | string | Uppercase state code |
| `customer_region` | string | North, Northeast, Central-West, Southeast, South or Unknown |

## `gold.dim_product`

| Column | Type | Meaning |
| --- | --- | --- |
| `product_id` | string | Primary key |
| `product_category_portuguese` | string | Reporting label based on the Portuguese category |
| `product_category_english` | string | English label or an untranslated marker |
| `category_translation_status` | string | Translated, Untranslated or Missing category |
| `product_name_length` | int | Length of product name |
| `product_description_length` | int | Length of product description |
| `product_photos_qty` | int | Number of product images |
| `product_weight_g` | double | Weight in grams |
| `product_weight_kg` | double | Weight converted to kilograms |
| `product_length_cm` | double | Length in centimetres |
| `product_height_cm` | double | Height in centimetres |
| `product_width_cm` | double | Width in centimetres |
| `product_volume_cm3` | double | Length × height × width |
| `has_complete_measurements` | boolean | True when all physical fields are available |

## `gold.dim_seller`

| Column | Type | Meaning |
| --- | --- | --- |
| `seller_id` | string | Primary key |
| `seller_zip_code_prefix` | int | Postal-code prefix |
| `seller_city` | string | City in title case |
| `seller_state` | string | Uppercase state code |
| `seller_region` | string | Brazilian geographic region |

## `gold.fact_orders`

| Column | Type | Meaning |
| --- | --- | --- |
| `order_id` | string | Primary key |
| `customer_id` | string | Foreign key to `dim_customer` |
| `order_status` | string | Degenerate order-status dimension |
| `purchase_date_key` | int | Purchase date role |
| `approved_date_key` | int | Approval date role |
| `carrier_date_key` | int | Carrier handover date role |
| `delivered_date_key` | int | Customer delivery date role |
| `estimated_delivery_date_key` | int | Estimated delivery date role |
| `order_purchase_timestamp` | timestamp | Original purchase timestamp |
| `order_approved_at` | timestamp | Original approval timestamp |
| `order_delivered_carrier_date` | timestamp | Original carrier timestamp |
| `order_delivered_customer_date` | timestamp | Original customer delivery timestamp |
| `order_estimated_delivery_date` | timestamp | Original estimated delivery timestamp |
| `approval_time_hours` | double | Hours from purchase to approval |
| `purchase_to_carrier_days` | int | Calendar days from purchase to carrier |
| `shipping_days` | int | Calendar days from carrier to customer |
| `delivery_days` | int | Calendar days from purchase to customer |
| `estimated_delivery_days` | int | Expected days from purchase to estimated delivery |
| `delivery_variance_days` | int | Actual delivery date minus estimated date; positive means late |
| `delivery_delay_days` | int | Late days with negative values replaced by zero |
| `delivery_performance_status` | string | Late, On time or Not evaluated |
| `is_delivered_status` | boolean | Order status is delivered |
| `is_canceled` | boolean | Order status is canceled |
| `has_delivery_timestamp` | boolean | Customer delivery timestamp is present |
| `is_late_delivery` | boolean | Actual date is after estimated date; null when not delivered |
| `has_timeline_anomaly` | boolean | At least one timestamp sequence is impossible |
| `order_count` | int | Constant 1 for additive counting |

## `gold.fact_order_items`

| Column | Type | Meaning |
| --- | --- | --- |
| `order_id` | string | Parent order key |
| `order_item_id` | int | Item sequence; part of the primary key |
| `product_id` | string | Foreign key to `dim_product` |
| `seller_id` | string | Foreign key to `dim_seller` |
| `shipping_limit_date_key` | int | Shipping-limit date role; null when outside the calendar range |
| `shipping_limit_timestamp` | timestamp | Original shipping deadline |
| `item_price` | decimal(12,2) | Product price excluding freight |
| `freight_value` | decimal(12,2) | Freight value |
| `item_total_value` | decimal(12,2) | Item price plus freight |
| `is_shipping_limit_date_out_of_range` | boolean | Date is outside the accepted calendar range |
| `item_count` | int | Constant 1 for additive counting |

## `gold.fact_order_payments`

| Column | Type | Meaning |
| --- | --- | --- |
| `order_id` | string | Parent order key |
| `payment_sequential` | int | Payment sequence; part of the primary key |
| `payment_type` | string | Normalised payment method |
| `payment_installments` | int | Number of instalments |
| `payment_value` | decimal(12,2) | Payment value |
| `is_installment_payment` | boolean | More than one instalment |
| `has_invalid_installment_count` | boolean | Instalment count below one |
| `is_undefined_payment_type` | boolean | Payment method is `not_defined` |
| `is_zero_value_payment` | boolean | Payment value is zero |
| `payment_count` | int | Constant 1 for additive counting |

## `gold.fact_order_reviews`

| Column | Type | Meaning |
| --- | --- | --- |
| `review_id` | string | Review identifier; part of the compound key |
| `order_id` | string | Parent order key; part of the compound key |
| `review_creation_date_key` | int | Review creation date role |
| `review_answer_date_key` | int | Review answer date role |
| `review_creation_date` | date | Original creation date |
| `review_answer_timestamp` | timestamp | Original answer timestamp |
| `review_score` | int | Score from 1 to 5 |
| `satisfaction_category` | string | Negative, Neutral, Positive or Invalid |
| `review_comment_title` | string | Optional comment title |
| `review_comment_message` | string | Optional comment message |
| `has_comment_title` | boolean | Comment title is present |
| `has_comment_message` | boolean | Comment message is present |
| `has_written_feedback` | boolean | Title or message is present |
| `review_response_days` | int | Calendar days from creation to answer |
| `is_response_over_30_days` | boolean | Response took more than 30 days |
| `review_count` | int | Constant 1 for additive counting |

## Security helper tables

### `gold.dim_seller_state`

One row per distinct, non-null `seller_state` from `gold.dim_seller`.

### `gold.security_user_region`

| Column | Meaning |
| --- | --- |
| `user_email` | Login used by the dynamic RLS rule |
| `seller_state` | State that the user is allowed to view |

Use placeholders or a private configuration source in a public repository. Do not commit real personal or company email addresses.

## Audit tables

| Table | Purpose |
| --- | --- |
| `audit.data_quality_results` | Append-only history of the 63 source-quality rules |
| `audit.rejected_records` | Quarantined row payloads for selected critical rules |
| `audit.gold_table_summary` | Row and column count for every core Gold table |
| `audit.gold_key_validation` | Null and duplicate key validation |
| `audit.gold_relationship_validation` | Unmatched non-date foreign keys |
| `audit.gold_date_relationship_validation` | Unmatched and null date keys |
| `audit.silver_gold_row_validation` | Row-count reconciliation between aligned Silver and Gold tables |

