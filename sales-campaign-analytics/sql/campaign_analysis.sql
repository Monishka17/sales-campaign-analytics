-- ============================================================
-- Sales & Campaign Performance Analytics — SQL Queries
-- Database: PostgreSQL compatible
-- ============================================================

-- ============================================================
-- 0. TABLE SETUP
-- ============================================================

CREATE TABLE IF NOT EXISTS campaign_master (
    campaign_id           VARCHAR(20) PRIMARY KEY,
    campaign_name         VARCHAR(100),
    channel               VARCHAR(30),
    campaign_type         VARCHAR(30),
    region                VARCHAR(20),
    start_date            DATE,
    end_date              DATE,
    budget_usd            NUMERIC(12,2),
    duration_days         INT,
    total_impressions     BIGINT,
    total_clicks          BIGINT,
    total_spend           NUMERIC(12,2),
    avg_daily_ctr         NUMERIC(8,4),
    active_days           INT,
    total_conversions     INT,
    total_revenue         NUMERIC(12,2),
    avg_order_value       NUMERIC(10,2),
    ctr                   NUMERIC(8,4),
    cvr                   NUMERIC(8,4),
    cpc                   NUMERIC(10,2),
    cpa                   NUMERIC(10,2),
    roas                  NUMERIC(8,2),
    roi_pct               NUMERIC(8,1),
    cpm                   NUMERIC(10,2),
    revenue_per_click     NUMERIC(10,2),
    flag_low_ctr          BOOLEAN,
    flag_low_cvr          BOOLEAN,
    flag_low_roas         BOOLEAN,
    underperforming_flags INT,
    performance_tier      VARCHAR(20)
);


-- ============================================================
-- 1. OVERALL CAMPAIGN KPI SUMMARY
-- ============================================================

SELECT
    COUNT(*)                                            AS total_campaigns,
    ROUND(SUM(total_spend)::NUMERIC, 0)                 AS total_spend,
    ROUND(SUM(total_revenue)::NUMERIC, 0)               AS total_revenue,
    ROUND(SUM(total_revenue) / NULLIF(SUM(total_spend),0), 2) AS overall_roas,
    ROUND(AVG(ctr) * 100, 2)                            AS avg_ctr_pct,
    ROUND(AVG(cvr) * 100, 2)                            AS avg_cvr_pct,
    ROUND(AVG(cpa), 0)                                  AS avg_cpa,
    SUM(CASE WHEN underperforming_flags >= 2 THEN 1 END) AS underperforming_campaigns
FROM campaign_master;


-- ============================================================
-- 2. CHANNEL PERFORMANCE WITH RANKING
-- Using window functions
-- ============================================================

SELECT
    channel,
    COUNT(*)                                            AS campaigns,
    ROUND(SUM(total_spend), 0)                          AS total_spend,
    ROUND(SUM(total_revenue), 0)                        AS total_revenue,
    ROUND(SUM(total_revenue)/NULLIF(SUM(total_spend),0), 2) AS roas,
    ROUND(AVG(ctr)*100, 2)                              AS avg_ctr_pct,
    ROUND(AVG(cvr)*100, 2)                              AS avg_cvr_pct,
    ROUND(AVG(cpa), 0)                                  AS avg_cpa,
    RANK() OVER (ORDER BY SUM(total_revenue)/NULLIF(SUM(total_spend),0) DESC) AS roas_rank,
    RANK() OVER (ORDER BY AVG(ctr) DESC)                AS ctr_rank
FROM campaign_master
GROUP BY channel
ORDER BY roas DESC;


-- ============================================================
-- 3. UNDERPERFORMING SEGMENT DETECTION
-- Rule-based flagging with CTR and CVR thresholds
-- ============================================================

WITH thresholds AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ctr) AS ctr_p25,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cvr) AS cvr_p25
    FROM campaign_master
)
SELECT
    cm.campaign_id,
    cm.channel,
    cm.campaign_type,
    cm.region,
    ROUND(cm.ctr * 100, 2)                              AS ctr_pct,
    ROUND(t.ctr_p25 * 100, 2)                           AS ctr_threshold_pct,
    ROUND(cm.cvr * 100, 2)                              AS cvr_pct,
    ROUND(t.cvr_p25 * 100, 2)                           AS cvr_threshold_pct,
    cm.roas,
    cm.underperforming_flags,
    cm.performance_tier,
    ROUND(cm.total_spend, 0)                            AS spend_at_risk,
    CASE
        WHEN cm.underperforming_flags = 3 THEN 'PAUSE IMMEDIATELY'
        WHEN cm.underperforming_flags = 2 THEN 'REVIEW & OPTIMIZE'
        ELSE 'MONITOR'
    END                                                 AS recommended_action
FROM campaign_master cm
CROSS JOIN thresholds t
WHERE cm.underperforming_flags >= 2
ORDER BY cm.underperforming_flags DESC, cm.roas ASC;


-- ============================================================
-- 4. CAMPAIGN TYPE × CHANNEL ROAS MATRIX
-- ============================================================

SELECT
    campaign_type,
    ROUND(AVG(CASE WHEN channel='Email'        THEN roas END), 2) AS email_roas,
    ROUND(AVG(CASE WHEN channel='Paid Search'  THEN roas END), 2) AS paid_search_roas,
    ROUND(AVG(CASE WHEN channel='Social Media' THEN roas END), 2) AS social_roas,
    ROUND(AVG(CASE WHEN channel='Display'      THEN roas END), 2) AS display_roas,
    ROUND(AVG(CASE WHEN channel='Referral'     THEN roas END), 2) AS referral_roas,
    ROUND(AVG(roas), 2)                                           AS overall_avg_roas
FROM campaign_master
GROUP BY campaign_type
ORDER BY overall_avg_roas DESC;


-- ============================================================
-- 5. BUDGET UTILIZATION & EFFICIENCY
-- ============================================================

SELECT
    campaign_id,
    campaign_name,
    channel,
    budget_usd,
    ROUND(total_spend, 0)                               AS actual_spend,
    ROUND(total_spend / NULLIF(budget_usd,0) * 100, 1) AS budget_utilization_pct,
    ROUND(total_revenue, 0)                             AS revenue,
    roas,
    performance_tier,
    -- Running total spend by channel
    ROUND(SUM(total_spend) OVER (PARTITION BY channel ORDER BY roas DESC), 0) AS cumulative_spend_by_channel
FROM campaign_master
ORDER BY budget_utilization_pct DESC;


-- ============================================================
-- 6. MONTHLY PERFORMANCE TREND
-- ============================================================

SELECT
    TO_CHAR(start_date, 'YYYY-MM')                      AS month,
    COUNT(*)                                            AS campaigns_started,
    ROUND(SUM(total_spend), 0)                          AS total_spend,
    ROUND(SUM(total_revenue), 0)                        AS total_revenue,
    ROUND(SUM(total_revenue)/NULLIF(SUM(total_spend),0), 2) AS monthly_roas,
    ROUND(AVG(ctr)*100, 2)                              AS avg_ctr_pct,
    -- Month-over-month revenue change
    ROUND(SUM(total_revenue) - LAG(SUM(total_revenue))
        OVER (ORDER BY TO_CHAR(start_date, 'YYYY-MM')), 0) AS mom_revenue_delta,
    ROUND((SUM(total_revenue) - LAG(SUM(total_revenue))
        OVER (ORDER BY TO_CHAR(start_date, 'YYYY-MM')))
        / NULLIF(LAG(SUM(total_revenue))
        OVER (ORDER BY TO_CHAR(start_date, 'YYYY-MM')), 0) * 100, 1) AS mom_change_pct
FROM campaign_master
GROUP BY TO_CHAR(start_date, 'YYYY-MM')
ORDER BY month;


-- ============================================================
-- 7. TOP 10 CAMPAIGNS BY ROAS
-- ============================================================

SELECT
    campaign_id,
    channel,
    campaign_type,
    region,
    ROUND(total_spend, 0)       AS spend,
    ROUND(total_revenue, 0)     AS revenue,
    roas,
    ROUND(ctr*100, 2)           AS ctr_pct,
    ROUND(cvr*100, 2)           AS cvr_pct,
    ROUND(cpa, 0)               AS cpa,
    performance_tier,
    NTILE(4) OVER (ORDER BY roas DESC) AS roas_quartile
FROM campaign_master
ORDER BY roas DESC
LIMIT 10;


-- ============================================================
-- 8. REGION × CHANNEL SPEND DISTRIBUTION
-- ============================================================

SELECT
    region,
    channel,
    COUNT(*)                                            AS campaigns,
    ROUND(SUM(total_spend), 0)                          AS total_spend,
    ROUND(SUM(total_revenue), 0)                        AS total_revenue,
    ROUND(AVG(roas), 2)                                 AS avg_roas,
    -- Share of total spend within region
    ROUND(SUM(total_spend) / SUM(SUM(total_spend)) OVER (PARTITION BY region) * 100, 1) AS pct_of_region_spend
FROM campaign_master
GROUP BY region, channel
ORDER BY region, total_spend DESC;
