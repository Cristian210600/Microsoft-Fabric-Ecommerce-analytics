# Image guide

The first implementation screenshots are already stored in subfolders so the README and technical documents stay easy to maintain.

```text
images/
├── architecture/
│   ├── audit-validation-tables.png
│   ├── bronze-layer-tables.png
│   ├── gold-layer-tables.png
│   ├── lakehouse-medallion-schemas.png
│   ├── silver-layer-tables.png
│   └── workspace-lineage.png
├── data-quality/
│   └── data-quality-results.png
├── pipeline/
│   ├── master-orchestration-pipeline.png
│   ├── metadata-driven-ingestion.png
│   └── successful-end-to-end-run.png
├── semantic-model/
│   ├── dynamic-rls-role.png
│   ├── model-overview.png
│   └── relationships.png
└── report/
    ├── 01-executive-overview.png
    ├── 02-sales-performance.png
    ├── 03-customer-analytics.png
    ├── 04-product-and-category.png
    ├── 05-seller-performance.png
    ├── 06-delivery-and-logistics.png
    ├── 07-customer-satisfaction.png
    ├── 08-order-details.png
    └── product-category-tooltip.png
```

## Screenshots already included

- `architecture/lakehouse-medallion-schemas.png` shows the Bronze, Silver, Gold and Audit schemas.
- `architecture/bronze-layer-tables.png` shows the nine Bronze Delta tables.
- `architecture/silver-layer-tables.png` shows the nine cleaned Silver tables.
- `architecture/gold-layer-tables.png` shows the dimensional model and security tables.
- `architecture/audit-validation-tables.png` shows the seven quality and model-validation tables.
- `architecture/workspace-lineage.png` shows item-level dependencies across the workspace.
- `data-quality/data-quality-results.png` shows rule-level PASS and WARNING results.
- `pipeline/master-orchestration-pipeline.png` shows the complete refresh sequence.
- `pipeline/metadata-driven-ingestion.png` shows the Lookup and ForEach design.
- `pipeline/successful-end-to-end-run.png` shows a successful execution of all six master activities.
- `semantic-model/model-overview.png` shows the implemented constellation model.
- `semantic-model/relationships.png` shows active fact relationships and inactive order-date roles.
- `semantic-model/dynamic-rls-role.png` shows the `Regional_Seller_Access` rule definition.
- `report/01-executive-overview.png` through `report/08-order-details.png` follow the real report-page order.
- `report/product-category-tooltip.png` shows the tooltip used on the Product & Category page.

## Screenshots still recommended

- `semantic-model/rls-test.png` showing the result of `Test as role` rather than only the role definition.
- a higher-resolution replacement for `report/01-executive-overview.png`, if available.

Before publishing, I check every screenshot and hide or crop tenant IDs, email addresses and other personal information. A future replacement for `workspace-lineage.png` could also be captured at a slightly higher zoom so the item names are easier to read on GitHub.
