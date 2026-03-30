# Sales & Campaign Performance Analytics

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![SQL](https://img.shields.io/badge/SQL-PostgreSQL-336791?logo=postgresql)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen)

## Overview

End-to-end campaign analytics pipeline covering 50 campaigns across 5 channels, 5 campaign types, and 5 regions. Built a full ETL pipeline from 4 raw sources, engineered 8+ KPIs, and developed rule-based logic to automatically flag underperforming segments.

---

## Key Results

| Metric | Value |
|---|---|
| Campaigns Analyzed | 50 across 10+ variants |
| Data Sources Ingested | 4 (campaigns, ads, conversions, segments) |
| KPIs Tracked | 8+ (ROAS, CTR, CVR, CPA, CPC, CPM, ROI, AOV) |
| Underperformers Flagged | 10 campaigns (auto-detected) |
| Validation Checks | 6 automated checks — all passing |

---

## Project Structure

```
sales-campaign-analytics/
├── data/
│   ├── generate_data.py            # Generates 4 raw source files
│   ├── source1_campaigns.csv       # Campaign master (50 rows)
│   ├── source2_ad_performance.csv  # Daily ad metrics (1,837 rows)
│   ├── source3_conversions.csv     # Daily conversions (1,837 rows)
│   ├── source4_audience_segments.csv # Segment targeting (140 rows)
│   └── campaign_master.csv         # ETL output — analysis-ready
├── etl/
│   └── etl_pipeline.py             # Full Extract → Transform → Load pipeline
├── notebooks/
│   └── campaign_analysis.ipynb     # EDA, KPI analysis, visualizations
├── sql/
│   └── campaign_analysis.sql       # 8 production SQL queries
├── visuals/
│   ├── channel_performance.png
│   ├── campaign_type_analysis.png
│   ├── underperformer_flags.png
│   ├── spend_vs_revenue.png
│   ├── roas_heatmap.png
│   └── monthly_trend.png
├── requirements.txt
└── README.md
```

---

## ETL Pipeline

The pipeline (`etl/etl_pipeline.py`) runs in 4 stages:

**Extract** — loads all 4 source CSVs with shape validation

**Transform** — cleans each source independently, then:
- Aggregates daily ad + conversion data to campaign level
- Computes 8 KPIs: CTR, CVR, CPC, CPA, ROAS, ROI%, CPM, revenue per click
- Applies rule-based flagging: campaigns scoring 2+ of 3 risk flags (low CTR / low CVR / ROAS < 1.5) are marked underperforming
- Assigns performance tiers: Poor / Below Average / Good / Excellent

**Validate** — 6 automated checks ensure data integrity before load

**Load** — outputs `campaign_master.csv` (50 rows × 30 columns)

---

## Analysis Breakdown

### 1. Channel Performance
Compared CTR, CVR, and ROAS across all 5 channels. Identified which channels consistently deliver above the 1.5x ROAS threshold.

### 2. Campaign Type Efficiency
Conversion and Retargeting campaigns show highest ROAS. Awareness campaigns — while useful for top-of-funnel — show the highest CPA and lowest CVR.

### 3. Underperformer Detection
Rule-based flagging logic automatically surfaces campaigns with low CTR + low CVR + low ROAS. 10 campaigns flagged with 2+ risk signals, representing recoverable budget waste.

### 4. Spend vs Revenue Scatter
Visualizes all 50 campaigns against break-even (ROAS=1) and threshold (ROAS=1.5) lines. Campaigns below the break-even line are prime candidates for pausing.

### 5. ROAS Heatmap
Channel × Campaign Type matrix reveals which combinations consistently underperform — actionable for future budget allocation.

### 6. Monthly Trend
Tracks spend, revenue, and ROAS month-over-month with LAG-based change detection in SQL.

---

## SQL Highlights

- Channel performance ranking with `RANK() OVER (ORDER BY ...)`
- Underperformer detection using `PERCENTILE_CONT` thresholds
- Budget utilization with running totals via `SUM() OVER (PARTITION BY channel)`
- Month-over-month trend using `LAG()` window function
- ROAS matrix using conditional `AVG(CASE WHEN channel=... THEN roas END)`

---

## How to Run

```bash
git clone https://github.com/Monishka17/sales-campaign-analytics.git
cd sales-campaign-analytics

pip install -r requirements.txt

# Step 1: Generate raw source data
python data/generate_data.py

# Step 2: Run ETL pipeline
python etl/etl_pipeline.py

# Step 3: Open notebook
jupyter notebook notebooks/campaign_analysis.ipynb
```

---

## Tech Stack

- **Python** — pandas, numpy, matplotlib, seaborn
- **SQL** — PostgreSQL (window functions, CTEs, PERCENTILE_CONT, NTILE)
- **Power BI** — ROAS KPI cards, channel comparison bar charts, underperformer table, monthly trend line
