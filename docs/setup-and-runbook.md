# Setup and runbook

This runbook is written for rebuilding the project in another Microsoft Fabric workspace. The exported IDs belong to the original workspace, so importing files is only the first step; connections and references must also be configured.

## Prerequisites

- Access to a Microsoft Fabric workspace with capacity available.
- Permission to create a Lakehouse, pipelines, notebooks and a semantic model.
- The nine Olist CSV files.
- The four exported notebook files.
- The two exported pipeline JSON files.
- The DAX-measure backup.
- Power BI Desktop if the report is maintained in PBIX format.

## 1. Create the Lakehouse

Create a Lakehouse such as `LH_ECOMMERCE`. The pipeline exports refer to an environment-specific Lakehouse identifier, so reconnect every dataset setting to the new Lakehouse.

Create or prepare these paths:

```text
Files/
├── config/
│   └── ingestion_config.json
├── source/
│   └── olist/
└── landing/
    └── olist/
        ├── category_translation/
        ├── customers/
        ├── geolocation/
        ├── order_items/
        ├── orders/
        ├── payments/
        ├── products/
        ├── reviews/
        └── sellers/
```

The `source` folder name is only an example. The real source location is controlled by `source_folder` in the ingestion configuration.

## 2. Add the ingestion configuration

Create `Files/config/ingestion_config.json`. One configuration row is required for each CSV.

Example:

```json
[
  {
    "source_file": "olist_customers_dataset.csv",
    "source_folder": "source/olist",
    "destination_folder": "landing/olist/customers"
  },
  {
    "source_file": "olist_orders_dataset.csv",
    "source_folder": "source/olist",
    "destination_folder": "landing/olist/orders"
  }
]
```

Add the other seven files with destination folders that match the Bronze notebook.

## 3. Import and attach the notebooks

Import the notebooks in execution order:

1. `NB_02_LOAD_BRONZE`
2. `NB_03_DATA_QUALITY`
3. `NB_04_SILVER_TRANSFORMATION`
4. `NB_05_GOLD_DIMENSIONAL_MODEL`

Attach each notebook to the new Lakehouse and verify that unqualified `Files/...` paths resolve correctly.

### Public-repository change required

Replace the hardcoded user email in the Gold notebook. A public example can use:

```python
security_data = [
    ("analyst@example.com", "SP")
]
```

For a reusable solution, read this mapping from a private table or deployment parameter instead of keeping it in notebook source code.

## 4. Review the Bronze loader before a clean run

The current exported notebook validates all nine Bronze tables, but the generic `MISSING_DATASETS` dictionary does not include Sellers. It also contains Reviews even though Reviews are loaded separately with multiline CSV options.

For a reproducible clean deployment:

- add a Sellers load entry or a dedicated Sellers cell;
- keep the special multiline Review load;
- remove Reviews from the later generic overwrite loop, or apply the same multiline/quote/escape options in the generic loader;
- run Bronze once from an empty schema and confirm that all nine tables are created.

The intended generic mapping is conceptually:

```python
DATASETS = {
    "category_translation": "product_category_translation",
    "geolocation": "geolocation",
    "order_items": "order_items",
    "orders": "orders",
    "payments": "order_payments",
    "products": "products",
    "sellers": "sellers"
}
```

Customers and Reviews can remain dedicated loads because Customers establishes the batch and Reviews needs multiline parsing.

## 5. Import the ingestion pipeline

Import `PL_01_METADATA_INGESTION.json`, then open every activity and reconnect:

- the Lookup Lakehouse;
- the source dataset Lakehouse;
- the sink dataset Lakehouse.

Confirm that `LKP_INGESTION_CONFIG` returns an array and that `FE_INGEST_FILES` uses the Lookup output.

Run the ingestion pipeline by itself. Check that all nine files appear in the expected landing folders.

## 6. Import the master pipeline

Import `PL_00_MASTER_ECOMMERCE.json` and replace all original references:

- invoked pipeline ID;
- notebook IDs;
- workspace IDs;
- connection IDs;
- semantic-model ID.

Do not assume that an imported pipeline automatically points to items with the same names.

## 7. First manual notebook run

Before using the master pipeline, run the notebooks manually in order. This isolates setup problems from orchestration problems.

### Bronze checks

Expected source-aligned row counts before Silver cleaning:

| Bronze table | Expected rows |
| --- | ---: |
| `bronze.product_category_translation` | 71 |
| `bronze.customers` | 99,441 |
| `bronze.geolocation` | 1,000,163 |
| `bronze.order_items` | 112,650 |
| `bronze.orders` | 99,441 |
| `bronze.order_payments` | 103,886 |
| `bronze.products` | 32,951 |
| `bronze.order_reviews` | 99,224 |
| `bronze.sellers` | 3,095 |

Review CSV data must be counted as logical CSV rows, not physical text lines, because comment fields can contain line breaks.

### Audit checks

After the data-quality notebook:

```sql
SELECT table_name, COUNT(*) AS rules
FROM audit.data_quality_results
GROUP BY table_name
ORDER BY table_name;
```

For one complete table-level execution, expected rule counts are:

| Table | Rules |
| --- | ---: |
| Customers | 4 |
| Orders | 7 |
| Order Items | 10 |
| Order Payments | 8 |
| Order Reviews | 9 |
| Products | 7 |
| Category Translation | 5 |
| Sellers | 6 |
| Geolocation | 7 |

### Silver checks

Confirm that all nine Silver tables exist. Then review the referential-integrity outputs printed at the end of the notebook:

- Orders without Customer;
- Items without Order;
- Items without Product;
- Items without Seller;
- Payments without Order;
- Reviews without Order;
- Products without translation;
- Customers/Sellers without geolocation coverage.

Not every coverage count must be zero to use the data, but every non-zero result should be understood and documented.

### Gold checks

The final Gold notebook prints an overall validation result and saves five validation tables. Review failures with:

```sql
SELECT *
FROM audit.gold_key_validation
WHERE validation_status = 'FAIL';
```

```sql
SELECT *
FROM audit.gold_relationship_validation
WHERE validation_status = 'FAIL';
```

```sql
SELECT *
FROM audit.gold_date_relationship_validation
WHERE validation_status = 'FAIL';
```

```sql
SELECT *
FROM audit.silver_gold_row_validation
WHERE validation_status = 'FAIL';
```

Null date keys are counted separately and can be expected for orders without approval or delivery timestamps. Non-null unmatched date keys should be zero.

## 8. Build or reconnect the semantic model

Add the core Gold tables and `audit.data_quality_results` to the semantic model. Add the security helper tables when dynamic RLS is used.

Create the relationships described in [Semantic model and DAX](semantic-model-and-dax.md). Recommended defaults:

- one-to-many cardinality;
- single-direction filtering;
- active Purchase Date relationship to Orders;
- inactive Order date-role relationships activated with `USERELATIONSHIP`;
- hidden technical keys and helper columns in Report view.

## 9. Restore the DAX measures

The Excel backup is a source-code inventory, not an automatic semantic-model import. Recreate or paste the expressions into the intended home tables, then:

- apply currency, percentage, whole-number and date formats;
- set sort-by columns in `dim_date` where needed;
- hide technical helper columns;
- place measures into display folders;
- validate time intelligence under a filtered year/month context.

Before final publishing, correct the issues listed in [Lessons and improvements](lessons-and-improvements.md).

## 10. Configure RLS

Create a role with a filter similar to:

```DAX
security_user_region[user_email] = USERPRINCIPALNAME()
```

Verify that the relationship path propagates the allowed seller states. Use “View as” or an equivalent test to confirm that an unmapped user does not accidentally see all data.

## 11. Run the master pipeline

After every component succeeds by itself, run `PL_00_MASTER_ECOMMERCE`.

Check:

1. Every activity is green.
2. The landing files were copied.
3. Bronze has one intended batch.
4. Latest DQ checks are visible.
5. Silver and Gold row counts are reasonable.
6. Gold validation tables have no unexpected failures.
7. The semantic model refresh completed.
8. Report cards and visuals show the current refresh.

## Troubleshooting

### Delta schema mismatch

If an existing table has a different schema, confirm that overwrite is intended and use `overwriteSchema = true`. For append-only audit tables, do not silently overwrite history; align the DataFrame schema with the existing Delta table or make an explicit controlled schema migration.

### Missing table or view

Check the schema name, Lakehouse attachment and execution order. `NB_03_DATA_QUALITY` assumes that all Bronze tables already exist.

### Unresolved column

Print the DataFrame columns before using the field. The Gold notebook expects corrected Silver names such as `product_name_length` and the exact date key `estimated_delivery_date_key`.

### Review row count is too high

Confirm multiline CSV parsing with quote and escape set to `"`. Physical line count is not a safe validation for the Reviews source.

### Latest DQ cards show inconsistent values

The quality notebook uses separate table-level execution timestamps. Use the latest execution per table, or redesign the notebook to create one global execution ID shared by all 63 checks.

