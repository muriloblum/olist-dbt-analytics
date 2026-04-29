# Volis — C-Level Analytics Dashboard

A local analytics platform built for Volis, a Brazilian e-commerce logistics company.
This project transforms raw transactional data from the Olist dataset into a clean, tested data warehouse — and serves it through a modern executive dashboard built with Reflex.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Tech Stack](#2-tech-stack)
3. [How to Run Locally](#3-how-to-run-locally)
4. [Functional Requirements Document (FRD)](#4-functional-requirements-document-frd)
5. [Data Architecture](#5-data-architecture)
6. [Layer 1 — Raw Data (Seeds)](#6-layer-1--raw-data-seeds)
7. [Layer 2 — Staging](#7-layer-2--staging)
8. [Data Quality & Testing](#8-data-quality--testing)
9. [Known Data Issue — Review ID Duplication](#9-known-data-issue--review-id-duplication)
10. [Layer 3 — Marts (Wide Tables)](#10-layer-3--marts-wide-tables)
11. [Dashboard — Reflex App](#11-dashboard--reflex-app)
12. [Next Steps](#12-next-steps)

---

## 1. Project Overview

Volis needed a single place where C-level executives could monitor the health of the business — revenue, logistics performance, customer satisfaction, and marketplace quality — without depending on spreadsheets or ad-hoc data pulls.

This project delivers exactly that: a fully automated data pipeline that goes from raw CSV exports to a live, interactive dashboard, with every transformation documented and tested.

**Who is this for?**

| Stakeholder | What they see |
|---|---|
| CFO | Revenue trends, order volume, average order value |
| COO | Delivery performance, delays, on-time rates |
| Head of Customer Experience | Review scores, satisfaction trends by region |
| Head of Marketplace | Seller rankings, revenue, and delivery quality |

---

## 2. Tech Stack

| Tool | Role |
|---|---|
| [DuckDB](https://duckdb.org/) | Local analytical database — fast, columnar, zero-config |
| [dbt-core](https://docs.getdbt.com/) | Data transformation, testing, and documentation |
| [Reflex](https://reflex.dev/) | Python-based full-stack framework for the dashboard UI |
| [Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) | Source data — 9 CSV files covering orders, customers, sellers, and more |

---

## 3. How to Run Locally

### Prerequisites

- Python 3.11+
- Node.js 18+ (required by Reflex)

### Installation

```bash
# Clone the repository
git clone https://github.com/your-username/volis.git
cd volis

# Install dependencies
pip install dbt-duckdb reflex

# Verify dbt installation
dbt --version
```

### Build the Data Warehouse

```bash
cd dbt_volis_project

# Load raw CSVs into DuckDB
dbt seed

# Run all transformations (staging + marts)
dbt run

# Validate data quality
dbt test
```

Expected output:
```
Done. PASS=11 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=11   # dbt run
Done. PASS=20 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=20   # dbt test
```

### Launch the Dashboard

```bash
cd ../reflex_app
reflex run
```

Open your browser at `http://localhost:3000`

---

## 4. Functional Requirements Document (FRD)

### Goal

Provide Volis executives with a single, always-up-to-date view of business performance — enabling faster, data-driven decisions without relying on manual reports.

### Key Metrics

| Metric | Stakeholder | Why it matters |
|---|---|---|
| Total Revenue (GMV) | CFO | Top-line health indicator |
| Monthly Revenue Trend | CFO | Growth trajectory and seasonality |
| Average Order Value (AOV) | CFO | Monetization efficiency per transaction |
| On-Time Delivery Rate | COO | Core operational KPI |
| Average Delivery Days | COO | Customer experience and logistics efficiency |
| Average Delay (days) | COO | Identifies systemic carrier or seller issues |
| Average Review Score | Head of CX | Overall satisfaction benchmark |
| CSAT Rate (scores 4–5) | Head of CX | Percentage of happy customers |
| Review Volume by Month | Head of CX | Feedback coverage and engagement |
| Top Sellers by Revenue | Head of Marketplace | Partner performance ranking |
| Seller On-Time Rate | Head of Marketplace | Identifies underperforming partners |
| Seller Avg Review Score | Head of Marketplace | Quality signal per seller |

### What Decisions Does This Enable?

- **CFO**: Detect revenue drops or spikes early. Identify which months drive growth.
- **COO**: Pinpoint logistics bottlenecks. Prioritize regions or carriers causing delays.
- **Head of CX**: Track satisfaction over time. Correlate low scores with delivery delays.
- **Head of Marketplace**: Reward top sellers. Flag partners with poor delivery or review performance for review.

### Data Sources

See [Section 6](#6-layer-1--raw-data-seeds) for the full source inventory.

---

## 5. Data Architecture

The project follows a **three-layer medallion architecture**, which is the industry standard for clean, maintainable data pipelines.

```
┌─────────────────────────────────────────────────────────────┐
│                        RAW (Seeds)                          │
│         9 CSV files loaded directly into DuckDB             │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                       STAGING                               │
│   9 views — cleaning, type casting, deduplication           │
│   Grain controlled here. Fan-out issues resolved here.      │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                        MARTS                                │
│   2 wide tables — mart_orders_wide, mart_sellers_wide       │
│   Pre-aggregated and ready for the Reflex dashboard         │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    REFLEX DASHBOARD                         │
│   Python reads mart tables directly via DuckDB connection   │
└─────────────────────────────────────────────────────────────┘
```

**Why this structure?**

Each layer has a single, clear responsibility. If a business rule changes (e.g., how revenue is calculated), you change it in one place — the mart — and it propagates automatically. Staging never contains business logic. Marts never repeat cleaning logic that staging already handled.

---

## 6. Layer 1 — Raw Data (Seeds)

dbt Seeds are the mechanism for loading static CSV files directly into DuckDB as database tables. All 9 Olist CSV files were placed in the `seeds/` folder and loaded with `dbt seed`.

| Seed Table | Description | Rows (approx.) |
|---|---|---|
| `olist_orders_dataset` | All orders placed on the platform | 99,441 |
| `olist_order_items_dataset` | Individual items within each order | 112,650 |
| `olist_order_payments_dataset` | Payment records per order | 103,886 |
| `olist_order_reviews_dataset` | Customer reviews submitted after delivery | 99,224 |
| `olist_customers_dataset` | Customer profiles and location | 99,441 |
| `olist_sellers_dataset` | Seller profiles and location | 3,095 |
| `olist_products_dataset` | Product catalog with categories | 32,951 |
| `olist_geolocation_dataset` | ZIP code to lat/lng mapping | 1,000,163 |
| `product_category_name_translation` | Portuguese to English category names | 71 |

No transformations happen at this layer. These are the raw, unmodified exports from the production database.

---

## 7. Layer 2 — Staging

Staging models are views that sit directly on top of the raw seed tables. Their only job is to make the data clean, consistently typed, and correctly named before any business logic is applied.

**Rules enforced in every staging model:**
- Column names are standardized (no raw source abbreviations)
- All date columns are explicitly cast to `timestamp`
- All numeric columns are explicitly cast to `numeric`
- Grain (the unique key) is identified and controlled
- Source-level data quality issues are handled here, not in marts

### Staging Models

#### `stg_olist_orders`
Casts all 5 date columns from text to timestamp. These dates power every delivery metric in the dashboard.

| Column | Type | Notes |
|---|---|---|
| `order_id` | text | Primary key |
| `customer_id` | text | Foreign key to customers |
| `order_status` | text | delivered, canceled, shipped, etc. |
| `order_purchase_at` | timestamp | When the customer placed the order |
| `order_approved_at` | timestamp | When payment was confirmed |
| `order_delivered_carrier_at` | timestamp | When handed to the carrier |
| `order_delivered_customer_at` | timestamp | When received by the customer |
| `order_estimated_delivery_at` | timestamp | Platform's promised delivery date |

#### `stg_olist_order_items`
Each order can have multiple items (multiple rows per `order_id`). Grain is `(order_id, order_item_id)`. This fan-out is intentional and expected — it is resolved in the mart layer via pre-aggregation.

#### `stg_olist_order_payments`
Each order can have multiple payment records (e.g., credit card + voucher). Grain is `(order_id, payment_sequential)`. Same fan-out handling applies.

#### `stg_olist_order_reviews`
Reviews have a known data quality issue documented in [Section 9](#9-known-data-issue--review-id-duplication). Grain is `(review_id, order_id)`. The mart handles deduplication to ensure one score per order.

#### `stg_olist_customers`
Straightforward rename and type standardization. One row per `customer_id`.

#### `stg_olist_sellers`
One row per `seller_id`. City and state renamed for clarity.

#### `stg_olist_geolocation`
The most complex staging model. The raw table has **17,972 ZIP codes with multiple lat/lng coordinates** — meaning the same postal code appears dozens of times with slightly different coordinates. Joining on ZIP code without deduplication would multiply every row it touches (fan-out).

**Solution:** `ROW_NUMBER()` partitioned by `zip_code_prefix` keeps exactly one coordinate set per ZIP code. The first row is selected, which is deterministic and consistent across runs.

```sql
, deduplicated as (
    select
        *
        , row_number() over (
            partition by geolocation_zip_code_prefix
            order by geolocation_zip_code_prefix
        ) as rn
    from source
)
select ... from deduplicated where rn = 1
```

#### `stg_olist_products`
Corrects two typos in the source column names (`product_name_lenght` → `product_name_length`, `product_description_lenght` → `product_description_length`).

#### `stg_olist_product_category_translation`
Simple passthrough. Maps Portuguese category names to English for display in the dashboard.

---

## 8. Data Quality & Testing

All tests are defined in YAML files alongside the models (`_staging.yml`, `_marts.yml`, `_sources.yml`) and run with `dbt test`.

### Tests Implemented

**Source tests** (validate raw data before any transformation):
- `order_id` is unique and not null in `olist_orders_dataset`
- `customer_id` is unique and not null in `olist_customers_dataset`
- `seller_id` is unique and not null in `olist_sellers_dataset`
- `product_id` is unique and not null in `olist_products_dataset`

**Staging tests** (validate the output of each staging model):
- `order_id` is unique and not null in `stg_olist_orders`
- `customer_id` is unique and not null in `stg_olist_customers`
- `seller_id` is unique and not null in `stg_olist_sellers`
- `zip_code_prefix` is unique and not null in `stg_olist_geolocation` *(validates deduplication worked)*

**Mart tests** (validate the final tables consumed by the dashboard):
- `order_id` is unique and not null in `mart_orders_wide`
- `seller_id` is unique and not null in `mart_sellers_wide`

### Granularity Verification (SQL Analysis)

Before writing any model, a manual SQL analysis was run directly on the raw seeds to map the grain of every table and identify fan-out risks:

```sql
SELECT 'olist_orders', COUNT(*)
FROM (SELECT order_id FROM olist_orders_dataset
      GROUP BY order_id HAVING COUNT(*) > 1) t
-- Result: 0 duplicates — grain is order_id

SELECT 'olist_order_reviews', COUNT(*)
FROM (SELECT review_id FROM olist_order_reviews_dataset
      GROUP BY review_id HAVING COUNT(*) > 1) t
-- Result: 789 duplicate review_ids — see Section 9

SELECT 'olist_geolocation', COUNT(*)
FROM (SELECT geolocation_zip_code_prefix FROM olist_geolocation_dataset
      GROUP BY geolocation_zip_code_prefix HAVING COUNT(*) > 1) t
-- Result: 17,972 duplicate ZIP codes — handled in stg_olist_geolocation
```

| Table | Expected Grain | Duplicates Found | Resolution |
|---|---|---|---|
| `olist_orders` | `order_id` | 0 | None needed |
| `olist_order_items` | `(order_id, order_item_id)` | 0 | Pre-aggregated in mart |
| `olist_order_payments` | `(order_id, payment_sequential)` | 0 | Pre-aggregated in mart |
| `olist_order_reviews` | `review_id` | 789 | See Section 9 |
| `olist_customers` | `customer_id` | 0 | None needed |
| `olist_products` | `product_id` | 0 | None needed |
| `olist_sellers` | `seller_id` | 0 | None needed |
| `olist_geolocation` | `zip_code_prefix` | 17,972 | Deduped in staging |

---

## 9. Known Data Issue — Review ID Duplication

### What was found

During the granularity analysis, **789 `review_id` values** appeared more than once in the reviews table — each linked to a different `order_id`.

This means the same review (same `review_id`, same comment text, same score) was associated with multiple different orders.

### A concrete example

The review below appears twice in the dataset:

| review_id | order_id | review_score | review_comment_message |
|---|---|---|---|
| `00130cbe...` | `04a28263...` | 1 | "O cartucho original HP 60XL não é reconhecido pela impressora..." |
| `00130cbe...` | `dfcdfc43...` | 1 | "O cartucho original HP 60XL não é reconhecido pela impressora..." |

The customer is clearly describing a **specific product** (an HP 60XL ink cartridge) that was not recognized by their printer. This is a product-specific complaint — yet the same review text is being attributed to two separate orders.

### Why this likely happened

This pattern suggests a data pipeline issue on Olist's side, possibly related to how reviews are matched to orders when a customer has multiple active orders at the same time. The review system may have linked a single review submission to all open orders for that customer instead of just the relevant one.

### How it was handled

Since the correct grain for reviews is **one score per order** (not one score per `review_id`), the mart layer uses `MAX(review_score)` grouped by `order_id`. This ensures every order carries at most one review score, regardless of duplicates in the source.

The staging model deliberately does **not** deduplicate reviews — it preserves the raw grain `(review_id, order_id)` and leaves the business decision of "which score counts" to the mart, where business context is available.

### Impact on metrics

Metrics derived from review scores (`avg_review_score`, `on_time_pct` correlations) are calculated on the deduplicated view. The 789 duplicate records do not inflate or distort any KPI on the dashboard.

---

## 10. Layer 3 — Marts (Wide Tables)

Marts are the final, analytics-ready tables that the Reflex dashboard reads directly. The design principle is **wide tables**: all the columns a stakeholder could need, pre-joined and pre-aggregated, so the Python layer only needs lightweight grouping — not complex SQL.

Fan-out is resolved inside each mart using **pre-aggregated CTEs** — intermediate steps that collapse multi-row relationships (items, payments) into a single row per order before the final join.

### `mart_orders_wide`

**Grain:** one row per order. Feeds the CFO, COO, and Head of CX.

| Column | Category | Description |
|---|---|---|
| `order_id` | Identifier | Unique order key |
| `customer_id` | Identifier | Links to customer |
| `purchased_at` | Date | When the order was placed |
| `order_month` | Date | Month truncated — used for time-series grouping in the dashboard |
| `delivered_at` | Date | When the customer received the order |
| `estimated_delivery_at` | Date | Platform's promised delivery date |
| `order_status` | Status | Current state of the order |
| `delivery_days` | Logistics | Total days from purchase to delivery. NULL for undelivered orders. |
| `delay_days` | Logistics | Days beyond the estimated date. 0 if early. NULL if undelivered. |
| `is_on_time` | Logistics | TRUE if delivered on or before estimate. NULL if undelivered — not FALSE, to avoid misclassifying in-transit orders as late. |
| `order_revenue` | Financial | Sum of item prices in the order |
| `order_freight` | Financial | Sum of freight costs in the order |
| `total_payment` | Financial | Total amount actually charged (may differ from item prices due to discounts/vouchers) |
| `items_count` | Financial | Number of items in the order |
| `recognized_revenue` | Financial | Same as `total_payment`, but forced to 0 for canceled and unavailable orders. This is the CFO's revenue figure. |
| `customer_city` | Geography | Customer's city |
| `customer_state` | Geography | Customer's state — used for regional heatmaps |
| `review_score` | CX | Score from 1–5. NULL if no review was submitted. |

**How fan-out is prevented:**

```
order_items_agg CTE  → aggregates N items into 1 row per order_id
payments_agg CTE     → aggregates N payments into 1 row per order_id
reviews CTE          → max(review_score) per order_id
                                  ↓
                     All joins in the final SELECT are 1:1
                     No row multiplication. No duplicated revenue.
```

### `mart_sellers_wide`

**Grain:** one row per seller. Feeds the Head of Marketplace.

| Column | Category | Description |
|---|---|---|
| `seller_id` | Identifier | Unique seller key |
| `seller_city` | Geography | Seller's city |
| `seller_state` | Geography | Seller's state |
| `total_orders` | Volume | Number of delivered orders fulfilled by this seller |
| `total_items_sold` | Volume | Total items shipped across all orders |
| `total_revenue` | Financial | Sum of item prices for all delivered orders |
| `avg_revenue_per_item` | Financial | Weighted average: total revenue ÷ total items. Reflects actual pricing mix, not a simple average. |
| `avg_delivery_days` | Logistics | Mean days from purchase to delivery across all orders |
| `avg_delay_days` | Logistics | Mean delay days. 0 for on-time orders, never negative. |
| `on_time_pct` | Logistics | Percentage of orders delivered on or before the estimated date |
| `avg_review_score` | CX | Mean review score across all delivered orders |
| `total_reviews` | CX | Number of reviews received. Context for interpreting the average — a 4.9 with 2 reviews is less meaningful than 4.9 with 500. |

**Why this mart references `mart_orders_wide`:**

Rather than recomputing `delivery_days`, `delay_days`, `is_on_time`, and `review_score` from scratch, `mart_sellers_wide` joins directly against `mart_orders_wide`. This means business logic (e.g., how delay is defined, which statuses count as "delivered") is defined in exactly one place. If that logic changes, both marts update automatically on the next `dbt run`.

---

## 11. Dashboard — Reflex App

The Reflex app connects to the DuckDB file produced by dbt and reads the two mart tables directly.

```python
import duckdb

DB_PATH = "db/volis.duckdb"

def query(sql: str):
    return duckdb.connect(DB_PATH, read_only=True).execute(sql).df()

df_orders  = query("SELECT * FROM mart_orders_wide")
df_sellers = query("SELECT * FROM mart_sellers_wide")
```

All KPI calculations (monthly revenue, on-time rate, CSAT percentage, etc.) are performed in Python/pandas on top of these DataFrames. The mart tables are intentionally wide so the Python layer stays simple and readable.

---

## 12. Next Steps

Given an additional 10 hours, the following improvements would be prioritized:

**Data Quality**
- Add `accepted_values` tests for `order_status` and `review_score` to catch unexpected values from future data loads
- Add `relationships` tests between mart tables and staging to enforce referential integrity
- Investigate the 789 duplicated `review_id` records further — determine if the correct order match can be identified from timestamps or product data

**Data Modeling**
- Add a `mart_customers_wide` table to enable cohort analysis and repeat purchase tracking for the CFO
- Enrich `mart_orders_wide` with product category (via `stg_olist_products` + `stg_olist_product_category_translation`) to enable category-level revenue breakdowns

**Dashboard**
- Add date range filters so executives can slice KPIs by custom time windows
- Add state-level map visualization for delivery performance and revenue distribution
- Implement seller drill-down: click a seller on the leaderboard to see their full order history

**Infrastructure**
- Replace `dbt seed` with a proper ingestion layer (e.g., a Python script that loads CSVs on a schedule) so the pipeline can run automatically when new data arrives
- Add a `dbt docs generate` step to the repo so the full data lineage is always browsable via `dbt docs serve`