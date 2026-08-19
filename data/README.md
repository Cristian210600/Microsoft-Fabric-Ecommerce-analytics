# Data

This project was built from nine Olist e-commerce CSV files. The complete raw files are not stored in this documentation folder.

| File | Logical rows |
| --- | ---: |
| `product_category_name_translation.csv` | 71 |
| `olist_sellers_dataset.csv` | 3,095 |
| `olist_products_dataset.csv` | 32,951 |
| `olist_order_payments_dataset.csv` | 103,886 |
| `olist_customers_dataset.csv` | 99,441 |
| `olist_order_reviews_dataset.csv` | 99,224 |
| `olist_order_items_dataset.csv` | 112,650 |
| `olist_orders_dataset.csv` | 99,441 |
| `olist_geolocation_dataset.csv` | 1,000,163 |

Total: 1,550,922 source rows.

The Review file can contain line breaks inside quoted comment fields. Use a CSV parser with multiline, quote and escape support; do not treat physical text-line count as the number of review records.

Before adding the full data to a public repository, check the source terms and decide whether a download reference or small sample is more appropriate.

