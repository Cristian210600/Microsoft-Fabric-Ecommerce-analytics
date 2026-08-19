# Data quality framework

## Purpose

I added a separate data-quality notebook because I did not want cleaning to silently remove every problem. The Bronze tables are checked before Silver is created, and every rule result is stored with its execution metadata.

The framework answers four questions:

1. Which table and rule were checked?
2. How many rows were checked and how many failed?
3. Was the problem a warning or an error?
4. Which execution and ingestion batch produced the result?

## Status logic

| Failed rows | Severity | Result status |
| ---: | --- | --- |
| 0 | Any | `PASS` |
| Greater than 0 | `WARNING` | `WARNING` |
| Greater than 0 | `ERROR` | `FAIL` |

Warning rules record a real quality concern but do not classify the result as a failed error. Examples include missing product descriptions, reused review IDs and exact duplicate geolocation observations.

![Data-quality results in the Fabric notebook](../images/data-quality/data-quality-results.png)

*This output is the practical result of the rule framework. Every row represents one evaluated rule and records the table, field, severity, checked rows, failed rows, percentage, status and execution timestamp. Warning rows are intentionally kept visible instead of being treated as notebook errors.*

At the moment of the screenshot, `audit.data_quality_results` contained 315 rows. This is larger than the 63-rule inventory because the table is append-only and keeps previous executions.

## Rule inventory

The notebook contains 63 rules.

### Customers — 4 rules

| Rule ID | Severity | Check |
| --- | --- | --- |
| `DQ_CUSTOMERS_001` | ERROR | `customer_id` is not null or empty |
| `DQ_CUSTOMERS_002` | ERROR | `customer_id` is unique |
| `DQ_CUSTOMERS_003` | ERROR | `customer_unique_id` is not null or empty |
| `DQ_CUSTOMERS_004` | WARNING | `customer_state` has two uppercase letters |

### Orders — 7 rules

| Rule ID | Severity | Check |
| --- | --- | --- |
| `DQ_ORDERS_001` | ERROR | `order_id` is not null or empty |
| `DQ_ORDERS_002` | ERROR | `order_id` is unique |
| `DQ_ORDERS_003` | ERROR | `customer_id` exists in Bronze Customers |
| `DQ_ORDERS_004` | ERROR | Status is one of created, approved, invoiced, processing, shipped, delivered, unavailable or canceled |
| `DQ_ORDERS_005` | ERROR | Approval is not earlier than purchase |
| `DQ_ORDERS_006` | ERROR | Customer delivery is not earlier than purchase |
| `DQ_ORDERS_007` | ERROR | A delivered order has a customer delivery date |

### Order items — 10 rules

| Rule ID | Severity | Check |
| --- | --- | --- |
| `DQ_ORDER_ITEMS_001` | ERROR | `order_id` is not null or empty |
| `DQ_ORDER_ITEMS_002` | ERROR | `order_item_id` is a positive integer |
| `DQ_ORDER_ITEMS_003` | ERROR | `order_id` + `order_item_id` is unique |
| `DQ_ORDER_ITEMS_004` | ERROR | `order_id` exists in Orders |
| `DQ_ORDER_ITEMS_005` | ERROR | `product_id` exists in Products |
| `DQ_ORDER_ITEMS_006` | ERROR | `seller_id` exists in Sellers |
| `DQ_ORDER_ITEMS_007` | ERROR | Shipping-limit date can be parsed |
| `DQ_ORDER_ITEMS_008` | ERROR | Price is greater than zero |
| `DQ_ORDER_ITEMS_009` | ERROR | Freight value is not negative |
| `DQ_ORDER_ITEMS_010` | ERROR | Shipping limit is not before purchase |

### Order payments — 8 rules

| Rule ID | Severity | Check |
| --- | --- | --- |
| `DQ_PAYMENTS_001` | ERROR | `order_id` is not null or empty |
| `DQ_PAYMENTS_002` | ERROR | `payment_sequential` is a positive integer |
| `DQ_PAYMENTS_003` | ERROR | `order_id` + `payment_sequential` is unique |
| `DQ_PAYMENTS_004` | ERROR | `order_id` exists in Orders |
| `DQ_PAYMENTS_005` | WARNING | Payment type is recognised |
| `DQ_PAYMENTS_006` | ERROR | Instalment count is not negative |
| `DQ_PAYMENTS_007` | ERROR | Credit-card payment has at least one instalment |
| `DQ_PAYMENTS_008` | WARNING | Payment value is greater than zero |

### Order reviews — 9 rules

| Rule ID | Severity | Check |
| --- | --- | --- |
| `DQ_REVIEWS_001` | ERROR | `review_id` is not null or empty |
| `DQ_REVIEWS_002` | ERROR | `order_id` is not null or empty |
| `DQ_REVIEWS_003` | WARNING | `review_id` is unique by itself |
| `DQ_REVIEWS_004` | ERROR | `review_id` + `order_id` is unique |
| `DQ_REVIEWS_005` | ERROR | `order_id` exists in Orders |
| `DQ_REVIEWS_006` | ERROR | Review score is between 1 and 5 |
| `DQ_REVIEWS_007` | ERROR | Review creation date is valid |
| `DQ_REVIEWS_008` | ERROR | Review answer timestamp is valid |
| `DQ_REVIEWS_009` | ERROR | Answer timestamp is not before creation |

The source reuses some review IDs. That is why rule 003 is a warning and the compound key in rule 004 is the real uniqueness rule.

### Products — 7 rules

| Rule ID | Severity | Check |
| --- | --- | --- |
| `DQ_PRODUCTS_001` | ERROR | `product_id` is not null or empty |
| `DQ_PRODUCTS_002` | ERROR | `product_id` is unique |
| `DQ_PRODUCTS_003` | WARNING | Product category is present |
| `DQ_PRODUCTS_004` | WARNING | Product category has an English translation |
| `DQ_PRODUCTS_005` | WARNING | Name length, description length and photo count are positive |
| `DQ_PRODUCTS_006` | WARNING | Product weight is greater than zero |
| `DQ_PRODUCTS_007` | WARNING | Product dimensions are greater than zero |

### Category translation — 5 rules

| Rule ID | Severity | Check |
| --- | --- | --- |
| `DQ_TRANSLATION_001` | ERROR | Portuguese category is present |
| `DQ_TRANSLATION_002` | ERROR | English category is present |
| `DQ_TRANSLATION_003` | ERROR | Portuguese category is unique |
| `DQ_TRANSLATION_004` | ERROR | English category is unique |
| `DQ_TRANSLATION_005` | WARNING | Category names use the expected lowercase/number/underscore format |

### Sellers — 6 rules

| Rule ID | Severity | Check |
| --- | --- | --- |
| `DQ_SELLERS_001` | ERROR | `seller_id` is not null or empty |
| `DQ_SELLERS_002` | ERROR | `seller_id` is unique |
| `DQ_SELLERS_003` | ERROR | ZIP-code prefix contains five digits |
| `DQ_SELLERS_004` | ERROR | Seller city is present |
| `DQ_SELLERS_005` | WARNING | Seller city looks like a city rather than an email address or website |
| `DQ_SELLERS_006` | ERROR | Seller state is a valid Brazilian state code |

### Geolocation — 7 rules

| Rule ID | Severity | Check |
| --- | --- | --- |
| `DQ_GEOLOCATION_001` | ERROR | ZIP-code prefix contains five digits |
| `DQ_GEOLOCATION_002` | ERROR | City is present |
| `DQ_GEOLOCATION_003` | ERROR | State is a valid Brazilian state |
| `DQ_GEOLOCATION_004` | ERROR | Latitude and longitude are numeric |
| `DQ_GEOLOCATION_005` | ERROR | Coordinates are inside broad Brazilian boundaries |
| `DQ_GEOLOCATION_006` | WARNING | Exact geographic business records are unique |
| `DQ_GEOLOCATION_007` | WARNING | One ZIP-code prefix does not belong to several states |

## `audit.data_quality_results`

The table is append-only so earlier executions remain available.

| Column | Type | Meaning |
| --- | --- | --- |
| `run_id` | string | UUID generated for the table-level quality run |
| `batch_id` | string | Bronze ingestion batch |
| `table_name` | string | Checked Bronze table |
| `rule_id` | string | Stable rule identifier |
| `rule_name` | string | Human-readable rule description |
| `column_name` | string | Field or fields checked |
| `severity` | string | `ERROR` or `WARNING` |
| `rows_checked` | bigint | Total rows in the checked table |
| `failed_rows` | bigint | Number of affected rows |
| `failed_percentage` | double | Failed rows divided by checked rows × 100 |
| `status` | string | `PASS`, `WARNING` or `FAIL` |
| `execution_timestamp` | timestamp | Table-level quality execution time |

The notebook generates separate run IDs and timestamps for different tables. The Power BI measures therefore calculate the latest execution per table instead of assuming that all tables share one global run ID.

## `audit.rejected_records`

| Column | Type | Meaning |
| --- | --- | --- |
| `run_id` | string | Quality execution that rejected the row |
| `batch_id` | string | Source batch |
| `source_table` | string | Bronze source table |
| `rule_id` | string | Rule that caused quarantine |
| `severity` | string | Current quarantined rules use `ERROR` |
| `record_key` | string | Simple or concatenated business key |
| `rejection_reason` | string | Readable explanation |
| `record_payload` | string | Complete source record serialised as JSON |
| `rejected_timestamp` | timestamp | Time when the row was stored |

The current quarantine cases are:

| Rule | Key | Reason |
| --- | --- | --- |
| `DQ_ORDERS_007` | `order_id` | Delivered order has no delivery date |
| `DQ_PAYMENTS_007` | `order_id|payment_sequential` | Credit-card payment has no valid instalment count |
| `DQ_GEOLOCATION_005` | `zip|latitude|longitude` | Coordinates are outside expected Brazilian boundaries |

## Gold validation tables

The Gold notebook adds another validation layer after the dimensional model is built.

![Audit and model-validation tables](../images/architecture/audit-validation-tables.png)

*The Audit schema contains seven tables in the current implementation. `data_quality_results` and `rejected_records` belong to the Bronze quality stage, while the other five tables validate the Gold dimensional model.*

### `audit.gold_table_summary`

Stores table name, row count, column count and validation timestamp for the eight core Gold tables.

### `audit.gold_key_validation`

For every dimension and fact table, stores:

- key columns;
- row count;
- distinct key count;
- duplicate-key rows;
- missing-key rows;
- `PASS` or `FAIL`.

### `audit.gold_relationship_validation`

Checks these six non-date relationships:

- Orders to Customer;
- Order Items to Orders;
- Order Items to Product;
- Order Items to Seller;
- Payments to Orders;
- Reviews to Orders.

### `audit.gold_date_relationship_validation`

Checks eight date roles against `dim_date`: five order dates, shipping limit, review creation and review answer. Null date keys are counted separately. A relationship passes when all non-null keys exist in the date dimension.

### `audit.silver_gold_row_validation`

Compares each aligned Silver table with its Gold dimension or fact table. The expected result is no row-count difference. `dim_date` is excluded because it is generated rather than copied from a Silver table.

## How I would make the quality gate stronger

The current notebook records failures but does not stop the master pipeline. My next version would add:

1. a shared pipeline execution ID for all table checks;
2. accepted thresholds per rule in a configuration table;
3. a final error count for the current execution;
4. notebook failure when a blocking threshold is exceeded;
5. notifications for new failures;
6. idempotent handling so rerunning one batch does not duplicate audit history.
