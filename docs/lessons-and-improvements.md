# Lessons learned and improvements

## What went well

The biggest achievement for me was connecting all the stages instead of building isolated exercises. The pipelines, notebooks, Delta tables, semantic model and DAX measures are part of one refresh path.

I also made a few modelling decisions that I would keep:

- separate fact tables for orders, items, payments and reviews;
- explicit compound keys for lower-grain facts;
- a generated Date dimension with several order-date roles;
- audit metadata in Bronze;
- append-only history for data-quality outcomes;
- warnings for known source behaviour, such as reused review IDs;
- Gold validation for keys, relationships, dates and row reconciliation;
- pre-calculated row-level delivery and review classifications.

## What was difficult

The most difficult part was maintaining consistency. A field renamed in Silver has to use the corrected name in Gold. A date key created in Gold has to exist in `dim_date` and use the same name in DAX. A customer count also changes meaning depending on whether I count `customer_id` or `customer_unique_id`.

I had to fix issues such as unresolved column names and Delta schema mismatch during development. These problems made me understand why schemas, naming conventions and small validation steps are important.

## Improvements before publishing

### 1. Make the Bronze loader reproducible from an empty workspace

The exported Bronze notebook validates nine tables, but Sellers is not included in the generic load mapping. This means a clean run can depend on `bronze.sellers` already existing.

Reviews are loaded once with multiline CSV settings and later included in a generic loader that does not use those settings. Because the later write uses overwrite mode, it can replace the correctly parsed table.

I would fix this by:

- adding Sellers to the loader;
- keeping Customers and Reviews as dedicated loads;
- removing Reviews from the generic mapping;
- defining one dataset configuration instead of having several partial loading blocks;
- running an automated clean-workspace test.

### 2. Use one execution ID for all data-quality rules

The notebook creates separate run IDs and execution timestamps for different tables. The latest DQ measures compensate by finding the latest timestamp for every table, but a single pipeline execution ID would be easier to understand.

I would create one `DQ_EXECUTION_ID` at the beginning and pass it to every table check. A separate `table_run_id` could still be kept if needed.

### 3. Make quality failures stop the pipeline when necessary

The master pipeline waits for the quality notebook to succeed technically. A rule can return `FAIL` without causing the notebook activity to fail.

I would create blocking and non-blocking thresholds, write a final execution summary and raise an exception when blocking errors are above the threshold.

### 4. Correct two suspicious DAX expressions

The DAX backup should be reviewed before it is treated as the final source of truth.

`DQ Latest Run Rows` assigns a `rule_id` to a variable named `LatestRunId`, then compares that value with `run_id`. The variable should select `run_id` instead:

```DAX
DQ Latest Run Rows =
VAR LatestRunId =
    MAXX(
        TOPN(
            1,
            ALL(data_quality_results),
            data_quality_results[execution_timestamp], DESC
        ),
        data_quality_results[run_id]
    )
RETURN
    CALCULATE(
        COUNTROWS(data_quality_results),
        data_quality_results[run_id] = LatestRunId
    )
```

`Repeat_customer_rate` uses `9` as the alternate result of `DIVIDE`. A zero-customer context would therefore return 9 rather than 0 or blank. A safer version is:

```DAX
Repeat Customer Rate =
DIVIDE([repeat_customers], [Total Customers], 0)
```

### 5. Decide the exact definition of average order value

The current `average_orders_value` measure uses product revenue divided by orders:

```DAX
DIVIDE([product_revenue], [total_orders], 0)
```

This is valid if the intended definition is average product revenue per order. If the report label suggests money actually paid, it may be more appropriate to use `total_payments / total_orders`. I would choose one business definition and document it directly in the measure description.

### 6. Reorganise and standardise measures

Some business measures are stored under `data_quality_results` and `dim_customer`, and names mix title case, snake case and capitalisation. There are also duplicate or near-duplicate measures such as `Negative reviews` and `Negative_reviews`, and `Item_sold` versus `total_items`.

I would:

- create a dedicated Measures table;
- use display folders;
- standardise report-facing names;
- remove validation-only measures such as `Test Customer ID` from the published report;
- hide technical helper measures;
- add descriptions and format strings.

### 7. Remove personal and environment-specific values

The exported Gold notebook contains a real user email for the RLS mapping. The pipeline JSON files contain the original workspace and artifact identifiers.

Before publishing:

- replace the email with `analyst@example.com` or read security mapping from a private source;
- review every notebook output for personal information;
- explain that pipeline IDs require rebinding;
- optionally replace original environment IDs with placeholders in a public template copy.

### 8. Remove duplicated development cells

Some exported notebooks contain repeated imports, displays and validation blocks left from interactive development. For example, the Review Silver table is saved and validated twice.

I would keep the original export as evidence if needed, but publish a cleaned notebook version with:

- one clear section per table;
- reusable helper functions;
- fewer repeated `count()` actions;
- a short parameters/configuration section at the top;
- final execution summaries instead of many intermediate displays.

### 9. Improve performance

The notebooks call `.count()` many times. In Spark, each count can start another job. This is acceptable for learning and a small static dataset, but it is not efficient.

Possible improvements:

- cache a DataFrame when it is counted and reused several times;
- combine several quality metrics into one aggregation;
- avoid repeated reads of the same Delta table;
- use partitioning only where it is supported by actual query patterns;
- use parallel file ingestion after confirming that it is safe;
- move from overwrite to incremental `MERGE` for changing data.

### 10. Make date ranges fully dynamic

`dim_date` is generated dynamically from Orders, but the Order Items code checks the shipping-limit date against hardcoded boundaries from 2016 to 2018. I would compare directly with the generated calendar limits so the model continues to work when newer data is added.

### 11. Add automated deployment and tests

The project currently depends on manual Fabric item import and rebinding. A later version could include:

- environment parameters;
- deployment pipeline or source-control integration;
- automated notebook checks;
- schema contracts;
- a small reproducible test dataset;
- CI validation for Markdown links and Python syntax.

## What I would do next

My practical next version would focus on a small number of improvements rather than rewriting everything:

1. Clean and parameterise the Bronze notebook.
2. Add one shared execution ID and a blocking quality gate.
3. Correct and organise the DAX measures.
4. Export the semantic model definition.
5. Add final report, Model view and pipeline screenshots.
6. Test the complete project from a new Fabric workspace.

That would make the repository easier for another person to reproduce and would show a clearer difference between the first learning version and a more production-oriented version.

