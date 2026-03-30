"""
ETL Pipeline — Sales & Campaign Performance Analytics
Extracts from 4 sources, cleans, transforms, and outputs
a single analysis-ready master dataset.
"""

import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings("ignore")

DATA_DIR   = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# ══════════════════════════════════════════════════════════════
# EXTRACT
# ══════════════════════════════════════════════════════════════

def extract():
    print("[ EXTRACT ] Loading 4 source files...")
    campaigns = pd.read_csv(os.path.join(DATA_DIR, "source1_campaigns.csv"))
    ads       = pd.read_csv(os.path.join(DATA_DIR, "source2_ad_performance.csv"))
    conv      = pd.read_csv(os.path.join(DATA_DIR, "source3_conversions.csv"))
    segments  = pd.read_csv(os.path.join(DATA_DIR, "source4_audience_segments.csv"))
    print(f"    Campaigns      : {campaigns.shape}")
    print(f"    Ad Performance : {ads.shape}")
    print(f"    Conversions    : {conv.shape}")
    print(f"    Segments       : {segments.shape}")
    return campaigns, ads, conv, segments


# ══════════════════════════════════════════════════════════════
# TRANSFORM
# ══════════════════════════════════════════════════════════════

def clean_campaigns(df):
    print("\n[ TRANSFORM ] Cleaning campaigns...")
    df = df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"]   = pd.to_datetime(df["end_date"])
    df["duration_days"] = (df["end_date"] - df["start_date"]).dt.days
    # Validate budget
    df = df[df["budget_usd"] > 0]
    assert df["campaign_id"].is_unique, "Duplicate campaign IDs found!"
    print(f"    Campaigns after clean: {len(df)}")
    return df


def clean_ads(df):
    print("[ TRANSFORM ] Cleaning ad performance...")
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["impressions"] > 0]
    df = df[df["spend_usd"]   > 0]
    df["ctr"] = (df["clicks"] / df["impressions"]).round(4)
    # Remove outlier CTR > 50%
    df = df[df["ctr"] <= 0.5]
    print(f"    Ad rows after clean: {len(df)}")
    return df


def clean_conversions(df):
    print("[ TRANSFORM ] Cleaning conversions...")
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["revenue_usd"]      = df["revenue_usd"].clip(lower=0)
    df["avg_order_value"]  = df["avg_order_value"].fillna(0)
    print(f"    Conversion rows after clean: {len(df)}")
    return df


def aggregate_daily(ads, conv):
    """Aggregate ad + conversion data to campaign level."""
    print("[ TRANSFORM ] Aggregating to campaign level...")

    ads_agg = ads.groupby("campaign_id").agg(
        total_impressions = ("impressions", "sum"),
        total_clicks      = ("clicks",      "sum"),
        total_spend       = ("spend_usd",   "sum"),
        avg_daily_ctr     = ("ctr",         "mean"),
        active_days       = ("date",        "nunique"),
    ).reset_index()

    conv_agg = conv.groupby("campaign_id").agg(
        total_conversions = ("conversions",    "sum"),
        total_revenue     = ("revenue_usd",    "sum"),
        avg_order_value   = ("avg_order_value","mean"),
    ).reset_index()

    merged = ads_agg.merge(conv_agg, on="campaign_id", how="left")
    merged["total_conversions"] = merged["total_conversions"].fillna(0)
    merged["total_revenue"]     = merged["total_revenue"].fillna(0)
    return merged


def compute_kpis(df):
    """Compute all marketing KPIs."""
    print("[ TRANSFORM ] Computing KPIs...")
    df = df.copy()
    df["ctr"]          = (df["total_clicks"] / df["total_impressions"].replace(0, np.nan)).round(4)
    df["cvr"]          = (df["total_conversions"] / df["total_clicks"].replace(0, np.nan)).round(4)
    df["cpc"]          = (df["total_spend"] / df["total_clicks"].replace(0, np.nan)).round(2)
    df["cpa"]          = (df["total_spend"] / df["total_conversions"].replace(0, np.nan)).round(2)
    df["roas"]         = (df["total_revenue"] / df["total_spend"].replace(0, np.nan)).round(2)
    df["roi_pct"]      = ((df["total_revenue"] - df["total_spend"]) / df["total_spend"].replace(0, np.nan) * 100).round(1)
    df["cpm"]          = (df["total_spend"] / df["total_impressions"].replace(0, np.nan) * 1000).round(2)
    df["revenue_per_click"] = (df["total_revenue"] / df["total_clicks"].replace(0, np.nan)).round(2)
    return df


def flag_underperformers(df):
    """
    Rule-based flagging logic for underperforming campaign segments.
    Flags campaigns that fall below threshold on CTR or conversion rate.
    """
    print("[ TRANSFORM ] Flagging underperformers...")
    df = df.copy()

    ctr_threshold = df["ctr"].quantile(0.25)   # bottom 25% CTR
    cvr_threshold = df["cvr"].quantile(0.25)   # bottom 25% CVR
    roas_threshold = 1.5                        # minimum acceptable ROAS

    df["flag_low_ctr"]  = df["ctr"]  < ctr_threshold
    df["flag_low_cvr"]  = df["cvr"]  < cvr_threshold
    df["flag_low_roas"] = df["roas"] < roas_threshold

    df["underperforming_flags"] = (
        df["flag_low_ctr"].astype(int) +
        df["flag_low_cvr"].astype(int) +
        df["flag_low_roas"].astype(int)
    )

    df["performance_tier"] = pd.cut(
        df["roas"].fillna(0),
        bins=[-np.inf, 0.5, 1.5, 3.0, np.inf],
        labels=["Poor", "Below Average", "Good", "Excellent"]
    )

    flagged = df["underperforming_flags"].ge(2).sum()
    print(f"    Campaigns flagged as underperforming (2+ flags): {flagged}")
    return df


def add_validation_checks(df):
    """Automated validation checks — ensures data reliability."""
    print("[ VALIDATE ] Running validation checks...")
    checks = {
        "No negative spend":       (df["total_spend"] >= 0).all(),
        "No negative revenue":     (df["total_revenue"] >= 0).all(),
        "CTR between 0 and 1":     df["ctr"].dropna().between(0, 1).all(),
        "CVR between 0 and 1":     df["cvr"].dropna().between(0, 1).all(),
        "No duplicate campaigns":  ~df["campaign_id"].duplicated().any(),
        "ROAS is numeric":         df["roas"].dtype in [np.float64, np.float32],
    }
    for check, result in checks.items():
        status = "PASS" if result else "FAIL"
        print(f"    [{status}] {check}")
    assert all(checks.values()), "Validation failed — check logs above."
    return df


# ══════════════════════════════════════════════════════════════
# LOAD
# ══════════════════════════════════════════════════════════════

def load(df):
    out_path = os.path.join(OUTPUT_DIR, "campaign_master.csv")
    df.to_csv(out_path, index=False)
    print(f"\n[ LOAD ] Master dataset saved → {out_path}")
    print(f"    Shape  : {df.shape}")
    print(f"    Columns: {list(df.columns)}")
    return df


# ══════════════════════════════════════════════════════════════
# PIPELINE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════

def run_pipeline():
    print("=" * 55)
    print("  ETL PIPELINE — Sales & Campaign Analytics")
    print("=" * 55)

    campaigns, ads, conv, segments = extract()

    campaigns = clean_campaigns(campaigns)
    ads       = clean_ads(ads)
    conv      = clean_conversions(conv)

    agg  = aggregate_daily(ads, conv)
    master = campaigns.merge(agg, on="campaign_id", how="left")
    master = compute_kpis(master)
    master = flag_underperformers(master)
    master = add_validation_checks(master)

    master = load(master)
    print("\nPipeline completed successfully.")
    return master


if __name__ == "__main__":
    df = run_pipeline()
