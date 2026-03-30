import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(99)
random.seed(99)

# ── Source 1: Campaigns master table ──────────────────────────
campaigns = []
channels = ["Email", "Paid Search", "Social Media", "Display", "Referral"]
campaign_types = ["Awareness", "Retargeting", "Conversion", "Loyalty", "Upsell"]
regions = ["North", "South", "East", "West", "Central"]

for i in range(1, 51):
    channel = random.choice(channels)
    ctype   = random.choice(campaign_types)
    region  = random.choice(regions)
    start   = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 300))
    end     = start + timedelta(days=random.randint(14, 60))
    budget  = random.choice([5000, 10000, 15000, 20000, 25000, 30000])
    campaigns.append({
        "campaign_id":   f"CAMP_{str(i).zfill(3)}",
        "campaign_name": f"{region} {channel} {ctype} #{i}",
        "channel":       channel,
        "campaign_type": ctype,
        "region":        region,
        "start_date":    start.date(),
        "end_date":      end.date(),
        "budget_usd":    budget,
    })

df_campaigns = pd.DataFrame(campaigns)
df_campaigns.to_csv("source1_campaigns.csv", index=False)
print(f"Source 1 — Campaigns: {df_campaigns.shape}")

# ── Source 2: Ad performance (impressions, clicks, spend) ─────
ad_records = []
for _, row in df_campaigns.iterrows():
    days = (pd.to_datetime(row["end_date"]) - pd.to_datetime(row["start_date"])).days
    for d in range(days):
        date = pd.to_datetime(row["start_date"]) + timedelta(days=d)
        impressions = random.randint(800, 8000)
        # CTR varies by channel
        base_ctr = {"Email": 0.04, "Paid Search": 0.06, "Social Media": 0.03,
                    "Display": 0.01, "Referral": 0.05}.get(row["channel"], 0.03)
        ctr = max(0.003, np.random.normal(base_ctr, base_ctr * 0.3))
        clicks = int(impressions * ctr)
        spend  = round(random.uniform(50, row["budget_usd"] / days * 1.2), 2)
        ad_records.append({
            "campaign_id": row["campaign_id"],
            "date":        date.date(),
            "impressions": impressions,
            "clicks":      clicks,
            "spend_usd":   spend,
        })

df_ads = pd.DataFrame(ad_records)
df_ads.to_csv("source2_ad_performance.csv", index=False)
print(f"Source 2 — Ad Performance: {df_ads.shape}")

# ── Source 3: Conversions / Sales ─────────────────────────────
conv_records = []
for _, row in df_campaigns.iterrows():
    days = (pd.to_datetime(row["end_date"]) - pd.to_datetime(row["start_date"])).days
    base_cvr = {"Conversion": 0.08, "Retargeting": 0.06, "Upsell": 0.07,
                "Loyalty": 0.05, "Awareness": 0.02}.get(row["campaign_type"], 0.04)
    for d in range(days):
        date   = pd.to_datetime(row["start_date"]) + timedelta(days=d)
        clicks_day = random.randint(5, 200)
        cvr    = max(0.005, np.random.normal(base_cvr, base_cvr * 0.25))
        convs  = int(clicks_day * cvr)
        rev    = round(convs * random.uniform(40, 250), 2)
        conv_records.append({
            "campaign_id":   row["campaign_id"],
            "date":          date.date(),
            "conversions":   convs,
            "revenue_usd":   rev,
            "avg_order_value": round(rev / convs, 2) if convs > 0 else 0,
        })

df_conv = pd.DataFrame(conv_records)
df_conv.to_csv("source3_conversions.csv", index=False)
print(f"Source 3 — Conversions: {df_conv.shape}")

# ── Source 4: Audience segments ───────────────────────────────
segments = []
seg_names = ["New Visitors", "Returning Users", "High-Value", "Lapsed", "Mobile Users",
             "Desktop Users", "Cart Abandoners", "Loyalty Members"]
for i, row in df_campaigns.iterrows():
    for seg in random.sample(seg_names, k=random.randint(2, 4)):
        segments.append({
            "campaign_id":    row["campaign_id"],
            "segment":        seg,
            "audience_size":  random.randint(500, 50000),
            "targeted_spend": round(random.uniform(500, 8000), 2),
        })

df_seg = pd.DataFrame(segments)
df_seg.to_csv("source4_audience_segments.csv", index=False)
print(f"Source 4 — Audience Segments: {df_seg.shape}")
print("\nAll source files generated!")
