-- ============================================================
-- 后效大盘分布分析 —— 下载付费类后效（Hive 版）
-- 数据来源：ks_origin_ad_log.ad_callback_log_from_ad_log_full
-- 归因日：20260320（首日消耗 + 3日/7日累计付费）
-- 分箱策略：Bin1=(0,p5] | Bin2~9=p5~p95等距8箱 | Bin10=(p95,+∞)
-- 维度：visitor_id
-- ============================================================

WITH

pay_info AS (
    -- 首日：归因日当天，消耗 + 首日付费
    SELECT
        visitor_id,
        SUM(IF(action_type = charge_action_type AND is_for_report_engine = true, cost_total, 0)) / 1000.0 AS cost_total,
        CAST(COUNT(DISTINCT IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true, llsid, NULL)) AS DOUBLE) AS pay_num,
        SUM(IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true AND callback_purchase_amount > 0,
            callback_purchase_amount, 0))                                                                  AS pay_amount,
        SUM(IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true AND callback_purchase_amount > 0,
            callback_purchase_amount, 0))                                                                  AS pay_amount_first_day,
        0.0                                                                                                AS pay_amount_three_day,
        0.0                                                                                                AS pay_amount_seven_day
    FROM ks_origin_ad_log.ad_callback_log_from_ad_log_full
    WHERE p_date = '20260320'
      AND is_duplicate = false
      AND is_retry = false
      AND is_for_report_engine = true
      AND ocpc_action_type IN ('AD_PURCHASE','AD_ROAS','EVENT_7_DAY_PAY_TIMES','AD_SEVEN_DAY_ROAS','AD_PURCHASE_CONVERSION')
    GROUP BY visitor_id

    UNION ALL

    -- 3日/7日：后续分区，conversion_time归因到20260320
    SELECT
        visitor_id,
        0.0 AS cost_total,
        0.0 AS pay_num,
        SUM(IF(
            is_for_report_engine = true
            AND action_type = 'EVENT_PAY'
            AND callback_purchase_amount > 0
            AND from_unixtime(CAST(conversion_time / 1000 AS BIGINT), 'yyyyMMdd') = '20260320',
            callback_purchase_amount, 0
        ))  AS pay_amount,
        0.0 AS pay_amount_first_day,
        SUM(IF(
            is_for_report_engine = true
            AND action_type = 'EVENT_PAY'
            AND callback_purchase_amount > 0
            AND from_unixtime(CAST(conversion_time / 1000 AS BIGINT), 'yyyyMMdd') = '20260320'
            AND p_date <= '20260322',
            callback_purchase_amount, 0
        ))  AS pay_amount_three_day,
        SUM(IF(
            is_for_report_engine = true
            AND action_type = 'EVENT_PAY'
            AND callback_purchase_amount > 0
            AND from_unixtime(CAST(conversion_time / 1000 AS BIGINT), 'yyyyMMdd') = '20260320'
            AND p_date <= '20260326',
            callback_purchase_amount, 0
        ))  AS pay_amount_seven_day
    FROM ks_origin_ad_log.ad_callback_log_from_ad_log_full
    WHERE p_date BETWEEN '20260321' AND '20260326'
      AND is_duplicate = false
      AND is_retry = false
      AND is_for_report_engine = true
      AND action_type = 'EVENT_PAY'
      AND ocpc_action_type IN ('AD_PURCHASE','AD_ROAS','EVENT_7_DAY_PAY_TIMES','AD_SEVEN_DAY_ROAS','AD_PURCHASE_CONVERSION')
    GROUP BY visitor_id
),

pay_agg AS (
    SELECT
        visitor_id,
        SUM(cost_total)           AS cost_total,
        SUM(pay_num)              AS pay_num,
        SUM(pay_amount)           AS pay_amount,
        SUM(pay_amount_first_day) AS pay_amount_first_day,
        SUM(pay_amount_three_day) AS pay_amount_three_day,
        SUM(pay_amount_seven_day) AS pay_amount_seven_day
    FROM pay_info
    GROUP BY visitor_id
),

uv_metrics AS (
    SELECT
        1                                                                                      AS join_key,
        visitor_id,
        cost_total,
        pay_num,
        pay_amount,
        IF(pay_num > 0,    pay_amount / pay_num,                                        NULL) AS avg_pay_amount,
        IF(cost_total > 0, pay_amount_first_day / cost_total,                           NULL) AS roi_first_day,
        IF(cost_total > 0, (pay_amount_first_day + pay_amount_three_day) / cost_total,  NULL) AS roi_three_day,
        IF(cost_total > 0, (pay_amount_first_day + pay_amount_seven_day) / cost_total,  NULL) AS roi_seven_day
    FROM pay_agg
),

percentiles AS (
    SELECT
        1                                                                                      AS join_key,
        percentile_approx(IF(cost_total     > 0, cost_total,     NULL), 0.05) AS cost_p5,
        percentile_approx(IF(cost_total     > 0, cost_total,     NULL), 0.95) AS cost_p95,
        percentile_approx(IF(pay_num        > 0, pay_num,        NULL), 0.05) AS pay_num_p5,
        percentile_approx(IF(pay_num        > 0, pay_num,        NULL), 0.95) AS pay_num_p95,
        percentile_approx(IF(pay_amount     > 0, pay_amount,     NULL), 0.05) AS pay_amt_p5,
        percentile_approx(IF(pay_amount     > 0, pay_amount,     NULL), 0.95) AS pay_amt_p95,
        percentile_approx(IF(avg_pay_amount > 0, avg_pay_amount, NULL), 0.05) AS avg_pay_p5,
        percentile_approx(IF(avg_pay_amount > 0, avg_pay_amount, NULL), 0.95) AS avg_pay_p95,
        percentile_approx(IF(roi_first_day  > 0, roi_first_day,  NULL), 0.05) AS roi1_p5,
        percentile_approx(IF(roi_first_day  > 0, roi_first_day,  NULL), 0.95) AS roi1_p95,
        percentile_approx(IF(roi_three_day  > 0, roi_three_day,  NULL), 0.05) AS roi3_p5,
        percentile_approx(IF(roi_three_day  > 0, roi_three_day,  NULL), 0.95) AS roi3_p95,
        percentile_approx(IF(roi_seven_day  > 0, roi_seven_day,  NULL), 0.05) AS roi7_p5,
        percentile_approx(IF(roi_seven_day  > 0, roi_seven_day,  NULL), 0.95) AS roi7_p95
    FROM uv_metrics
),

binned AS (
    -- 1. cost_total
    SELECT 'cost_total' AS metric, visitor_id,
        CASE
            WHEN cost_total <= 0 OR cost_total IS NULL THEN NULL
            WHEN cost_total <= p.cost_p5                                        THEN CONCAT('Bin01_(0,',ROUND(p.cost_p5,2),']')
            WHEN cost_total <= p.cost_p5 + 1*(p.cost_p95-p.cost_p5)/8          THEN CONCAT('Bin02_(',ROUND(p.cost_p5,2),',',ROUND(p.cost_p5+1*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost_total <= p.cost_p5 + 2*(p.cost_p95-p.cost_p5)/8          THEN CONCAT('Bin03_(',ROUND(p.cost_p5+1*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+2*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost_total <= p.cost_p5 + 3*(p.cost_p95-p.cost_p5)/8          THEN CONCAT('Bin04_(',ROUND(p.cost_p5+2*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+3*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost_total <= p.cost_p5 + 4*(p.cost_p95-p.cost_p5)/8          THEN CONCAT('Bin05_(',ROUND(p.cost_p5+3*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+4*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost_total <= p.cost_p5 + 5*(p.cost_p95-p.cost_p5)/8          THEN CONCAT('Bin06_(',ROUND(p.cost_p5+4*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+5*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost_total <= p.cost_p5 + 6*(p.cost_p95-p.cost_p5)/8          THEN CONCAT('Bin07_(',ROUND(p.cost_p5+5*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+6*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost_total <= p.cost_p5 + 7*(p.cost_p95-p.cost_p5)/8          THEN CONCAT('Bin08_(',ROUND(p.cost_p5+6*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+7*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost_total <= p.cost_p95                                       THEN CONCAT('Bin09_(',ROUND(p.cost_p5+7*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p95,2),']')
            ELSE                                                                     CONCAT('Bin10_(',ROUND(p.cost_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 2. pay_num
    SELECT 'pay_num' AS metric, visitor_id,
        CASE
            WHEN pay_num <= 0 OR pay_num IS NULL THEN NULL
            WHEN pay_num <= p.pay_num_p5                                        THEN CONCAT('Bin01_(0,',ROUND(p.pay_num_p5,2),']')
            WHEN pay_num <= p.pay_num_p5 + 1*(p.pay_num_p95-p.pay_num_p5)/8    THEN CONCAT('Bin02_(',ROUND(p.pay_num_p5,2),',',ROUND(p.pay_num_p5+1*(p.pay_num_p95-p.pay_num_p5)/8,2),']')
            WHEN pay_num <= p.pay_num_p5 + 2*(p.pay_num_p95-p.pay_num_p5)/8    THEN CONCAT('Bin03_(',ROUND(p.pay_num_p5+1*(p.pay_num_p95-p.pay_num_p5)/8,2),',',ROUND(p.pay_num_p5+2*(p.pay_num_p95-p.pay_num_p5)/8,2),']')
            WHEN pay_num <= p.pay_num_p5 + 3*(p.pay_num_p95-p.pay_num_p5)/8    THEN CONCAT('Bin04_(',ROUND(p.pay_num_p5+2*(p.pay_num_p95-p.pay_num_p5)/8,2),',',ROUND(p.pay_num_p5+3*(p.pay_num_p95-p.pay_num_p5)/8,2),']')
            WHEN pay_num <= p.pay_num_p5 + 4*(p.pay_num_p95-p.pay_num_p5)/8    THEN CONCAT('Bin05_(',ROUND(p.pay_num_p5+3*(p.pay_num_p95-p.pay_num_p5)/8,2),',',ROUND(p.pay_num_p5+4*(p.pay_num_p95-p.pay_num_p5)/8,2),']')
            WHEN pay_num <= p.pay_num_p5 + 5*(p.pay_num_p95-p.pay_num_p5)/8    THEN CONCAT('Bin06_(',ROUND(p.pay_num_p5+4*(p.pay_num_p95-p.pay_num_p5)/8,2),',',ROUND(p.pay_num_p5+5*(p.pay_num_p95-p.pay_num_p5)/8,2),']')
            WHEN pay_num <= p.pay_num_p5 + 6*(p.pay_num_p95-p.pay_num_p5)/8    THEN CONCAT('Bin07_(',ROUND(p.pay_num_p5+5*(p.pay_num_p95-p.pay_num_p5)/8,2),',',ROUND(p.pay_num_p5+6*(p.pay_num_p95-p.pay_num_p5)/8,2),']')
            WHEN pay_num <= p.pay_num_p5 + 7*(p.pay_num_p95-p.pay_num_p5)/8    THEN CONCAT('Bin08_(',ROUND(p.pay_num_p5+6*(p.pay_num_p95-p.pay_num_p5)/8,2),',',ROUND(p.pay_num_p5+7*(p.pay_num_p95-p.pay_num_p5)/8,2),']')
            WHEN pay_num <= p.pay_num_p95                                       THEN CONCAT('Bin09_(',ROUND(p.pay_num_p5+7*(p.pay_num_p95-p.pay_num_p5)/8,2),',',ROUND(p.pay_num_p95,2),']')
            ELSE                                                                     CONCAT('Bin10_(',ROUND(p.pay_num_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 3. pay_amount
    SELECT 'pay_amount' AS metric, visitor_id,
        CASE
            WHEN pay_amount <= 0 OR pay_amount IS NULL THEN NULL
            WHEN pay_amount <= p.pay_amt_p5                                                 THEN CONCAT('Bin01_(0,',ROUND(p.pay_amt_p5,2),']')
            WHEN pay_amount <= p.pay_amt_p5 + 1*(p.pay_amt_p95-p.pay_amt_p5)/8             THEN CONCAT('Bin02_(',ROUND(p.pay_amt_p5,2),',',ROUND(p.pay_amt_p5+1*(p.pay_amt_p95-p.pay_amt_p5)/8,2),']')
            WHEN pay_amount <= p.pay_amt_p5 + 2*(p.pay_amt_p95-p.pay_amt_p5)/8             THEN CONCAT('Bin03_(',ROUND(p.pay_amt_p5+1*(p.pay_amt_p95-p.pay_amt_p5)/8,2),',',ROUND(p.pay_amt_p5+2*(p.pay_amt_p95-p.pay_amt_p5)/8,2),']')
            WHEN pay_amount <= p.pay_amt_p5 + 3*(p.pay_amt_p95-p.pay_amt_p5)/8             THEN CONCAT('Bin04_(',ROUND(p.pay_amt_p5+2*(p.pay_amt_p95-p.pay_amt_p5)/8,2),',',ROUND(p.pay_amt_p5+3*(p.pay_amt_p95-p.pay_amt_p5)/8,2),']')
            WHEN pay_amount <= p.pay_amt_p5 + 4*(p.pay_amt_p95-p.pay_amt_p5)/8             THEN CONCAT('Bin05_(',ROUND(p.pay_amt_p5+3*(p.pay_amt_p95-p.pay_amt_p5)/8,2),',',ROUND(p.pay_amt_p5+4*(p.pay_amt_p95-p.pay_amt_p5)/8,2),']')
            WHEN pay_amount <= p.pay_amt_p5 + 5*(p.pay_amt_p95-p.pay_amt_p5)/8             THEN CONCAT('Bin06_(',ROUND(p.pay_amt_p5+4*(p.pay_amt_p95-p.pay_amt_p5)/8,2),',',ROUND(p.pay_amt_p5+5*(p.pay_amt_p95-p.pay_amt_p5)/8,2),']')
            WHEN pay_amount <= p.pay_amt_p5 + 6*(p.pay_amt_p95-p.pay_amt_p5)/8             THEN CONCAT('Bin07_(',ROUND(p.pay_amt_p5+5*(p.pay_amt_p95-p.pay_amt_p5)/8,2),',',ROUND(p.pay_amt_p5+6*(p.pay_amt_p95-p.pay_amt_p5)/8,2),']')
            WHEN pay_amount <= p.pay_amt_p5 + 7*(p.pay_amt_p95-p.pay_amt_p5)/8             THEN CONCAT('Bin08_(',ROUND(p.pay_amt_p5+6*(p.pay_amt_p95-p.pay_amt_p5)/8,2),',',ROUND(p.pay_amt_p5+7*(p.pay_amt_p95-p.pay_amt_p5)/8,2),']')
            WHEN pay_amount <= p.pay_amt_p95                                                THEN CONCAT('Bin09_(',ROUND(p.pay_amt_p5+7*(p.pay_amt_p95-p.pay_amt_p5)/8,2),',',ROUND(p.pay_amt_p95,2),']')
            ELSE                                                                                 CONCAT('Bin10_(',ROUND(p.pay_amt_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 4. avg_pay_amount
    SELECT 'avg_pay_amount' AS metric, visitor_id,
        CASE
            WHEN avg_pay_amount <= 0 OR avg_pay_amount IS NULL THEN NULL
            WHEN avg_pay_amount <= p.avg_pay_p5                                                             THEN CONCAT('Bin01_(0,',ROUND(p.avg_pay_p5,2),']')
            WHEN avg_pay_amount <= p.avg_pay_p5 + 1*(p.avg_pay_p95-p.avg_pay_p5)/8                         THEN CONCAT('Bin02_(',ROUND(p.avg_pay_p5,2),',',ROUND(p.avg_pay_p5+1*(p.avg_pay_p95-p.avg_pay_p5)/8,2),']')
            WHEN avg_pay_amount <= p.avg_pay_p5 + 2*(p.avg_pay_p95-p.avg_pay_p5)/8                         THEN CONCAT('Bin03_(',ROUND(p.avg_pay_p5+1*(p.avg_pay_p95-p.avg_pay_p5)/8,2),',',ROUND(p.avg_pay_p5+2*(p.avg_pay_p95-p.avg_pay_p5)/8,2),']')
            WHEN avg_pay_amount <= p.avg_pay_p5 + 3*(p.avg_pay_p95-p.avg_pay_p5)/8                         THEN CONCAT('Bin04_(',ROUND(p.avg_pay_p5+2*(p.avg_pay_p95-p.avg_pay_p5)/8,2),',',ROUND(p.avg_pay_p5+3*(p.avg_pay_p95-p.avg_pay_p5)/8,2),']')
            WHEN avg_pay_amount <= p.avg_pay_p5 + 4*(p.avg_pay_p95-p.avg_pay_p5)/8                         THEN CONCAT('Bin05_(',ROUND(p.avg_pay_p5+3*(p.avg_pay_p95-p.avg_pay_p5)/8,2),',',ROUND(p.avg_pay_p5+4*(p.avg_pay_p95-p.avg_pay_p5)/8,2),']')
            WHEN avg_pay_amount <= p.avg_pay_p5 + 5*(p.avg_pay_p95-p.avg_pay_p5)/8                         THEN CONCAT('Bin06_(',ROUND(p.avg_pay_p5+4*(p.avg_pay_p95-p.avg_pay_p5)/8,2),',',ROUND(p.avg_pay_p5+5*(p.avg_pay_p95-p.avg_pay_p5)/8,2),']')
            WHEN avg_pay_amount <= p.avg_pay_p5 + 6*(p.avg_pay_p95-p.avg_pay_p5)/8                         THEN CONCAT('Bin07_(',ROUND(p.avg_pay_p5+5*(p.avg_pay_p95-p.avg_pay_p5)/8,2),',',ROUND(p.avg_pay_p5+6*(p.avg_pay_p95-p.avg_pay_p5)/8,2),']')
            WHEN avg_pay_amount <= p.avg_pay_p5 + 7*(p.avg_pay_p95-p.avg_pay_p5)/8                         THEN CONCAT('Bin08_(',ROUND(p.avg_pay_p5+6*(p.avg_pay_p95-p.avg_pay_p5)/8,2),',',ROUND(p.avg_pay_p5+7*(p.avg_pay_p95-p.avg_pay_p5)/8,2),']')
            WHEN avg_pay_amount <= p.avg_pay_p95                                                            THEN CONCAT('Bin09_(',ROUND(p.avg_pay_p5+7*(p.avg_pay_p95-p.avg_pay_p5)/8,2),',',ROUND(p.avg_pay_p95,2),']')
            ELSE                                                                                                 CONCAT('Bin10_(',ROUND(p.avg_pay_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 5. roi_first_day
    SELECT 'roi_first_day' AS metric, visitor_id,
        CASE
            WHEN roi_first_day <= 0 OR roi_first_day IS NULL THEN NULL
            WHEN roi_first_day <= p.roi1_p5                                             THEN CONCAT('Bin01_(0,',ROUND(p.roi1_p5,4),']')
            WHEN roi_first_day <= p.roi1_p5 + 1*(p.roi1_p95-p.roi1_p5)/8               THEN CONCAT('Bin02_(',ROUND(p.roi1_p5,4),',',ROUND(p.roi1_p5+1*(p.roi1_p95-p.roi1_p5)/8,4),']')
            WHEN roi_first_day <= p.roi1_p5 + 2*(p.roi1_p95-p.roi1_p5)/8               THEN CONCAT('Bin03_(',ROUND(p.roi1_p5+1*(p.roi1_p95-p.roi1_p5)/8,4),',',ROUND(p.roi1_p5+2*(p.roi1_p95-p.roi1_p5)/8,4),']')
            WHEN roi_first_day <= p.roi1_p5 + 3*(p.roi1_p95-p.roi1_p5)/8               THEN CONCAT('Bin04_(',ROUND(p.roi1_p5+2*(p.roi1_p95-p.roi1_p5)/8,4),',',ROUND(p.roi1_p5+3*(p.roi1_p95-p.roi1_p5)/8,4),']')
            WHEN roi_first_day <= p.roi1_p5 + 4*(p.roi1_p95-p.roi1_p5)/8               THEN CONCAT('Bin05_(',ROUND(p.roi1_p5+3*(p.roi1_p95-p.roi1_p5)/8,4),',',ROUND(p.roi1_p5+4*(p.roi1_p95-p.roi1_p5)/8,4),']')
            WHEN roi_first_day <= p.roi1_p5 + 5*(p.roi1_p95-p.roi1_p5)/8               THEN CONCAT('Bin06_(',ROUND(p.roi1_p5+4*(p.roi1_p95-p.roi1_p5)/8,4),',',ROUND(p.roi1_p5+5*(p.roi1_p95-p.roi1_p5)/8,4),']')
            WHEN roi_first_day <= p.roi1_p5 + 6*(p.roi1_p95-p.roi1_p5)/8               THEN CONCAT('Bin07_(',ROUND(p.roi1_p5+5*(p.roi1_p95-p.roi1_p5)/8,4),',',ROUND(p.roi1_p5+6*(p.roi1_p95-p.roi1_p5)/8,4),']')
            WHEN roi_first_day <= p.roi1_p5 + 7*(p.roi1_p95-p.roi1_p5)/8               THEN CONCAT('Bin08_(',ROUND(p.roi1_p5+6*(p.roi1_p95-p.roi1_p5)/8,4),',',ROUND(p.roi1_p5+7*(p.roi1_p95-p.roi1_p5)/8,4),']')
            WHEN roi_first_day <= p.roi1_p95                                            THEN CONCAT('Bin09_(',ROUND(p.roi1_p5+7*(p.roi1_p95-p.roi1_p5)/8,4),',',ROUND(p.roi1_p95,4),']')
            ELSE                                                                             CONCAT('Bin10_(',ROUND(p.roi1_p95,4),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 6. roi_three_day
    SELECT 'roi_three_day' AS metric, visitor_id,
        CASE
            WHEN roi_three_day <= 0 OR roi_three_day IS NULL THEN NULL
            WHEN roi_three_day <= p.roi3_p5                                             THEN CONCAT('Bin01_(0,',ROUND(p.roi3_p5,4),']')
            WHEN roi_three_day <= p.roi3_p5 + 1*(p.roi3_p95-p.roi3_p5)/8               THEN CONCAT('Bin02_(',ROUND(p.roi3_p5,4),',',ROUND(p.roi3_p5+1*(p.roi3_p95-p.roi3_p5)/8,4),']')
            WHEN roi_three_day <= p.roi3_p5 + 2*(p.roi3_p95-p.roi3_p5)/8               THEN CONCAT('Bin03_(',ROUND(p.roi3_p5+1*(p.roi3_p95-p.roi3_p5)/8,4),',',ROUND(p.roi3_p5+2*(p.roi3_p95-p.roi3_p5)/8,4),']')
            WHEN roi_three_day <= p.roi3_p5 + 3*(p.roi3_p95-p.roi3_p5)/8               THEN CONCAT('Bin04_(',ROUND(p.roi3_p5+2*(p.roi3_p95-p.roi3_p5)/8,4),',',ROUND(p.roi3_p5+3*(p.roi3_p95-p.roi3_p5)/8,4),']')
            WHEN roi_three_day <= p.roi3_p5 + 4*(p.roi3_p95-p.roi3_p5)/8               THEN CONCAT('Bin05_(',ROUND(p.roi3_p5+3*(p.roi3_p95-p.roi3_p5)/8,4),',',ROUND(p.roi3_p5+4*(p.roi3_p95-p.roi3_p5)/8,4),']')
            WHEN roi_three_day <= p.roi3_p5 + 5*(p.roi3_p95-p.roi3_p5)/8               THEN CONCAT('Bin06_(',ROUND(p.roi3_p5+4*(p.roi3_p95-p.roi3_p5)/8,4),',',ROUND(p.roi3_p5+5*(p.roi3_p95-p.roi3_p5)/8,4),']')
            WHEN roi_three_day <= p.roi3_p5 + 6*(p.roi3_p95-p.roi3_p5)/8               THEN CONCAT('Bin07_(',ROUND(p.roi3_p5+5*(p.roi3_p95-p.roi3_p5)/8,4),',',ROUND(p.roi3_p5+6*(p.roi3_p95-p.roi3_p5)/8,4),']')
            WHEN roi_three_day <= p.roi3_p5 + 7*(p.roi3_p95-p.roi3_p5)/8               THEN CONCAT('Bin08_(',ROUND(p.roi3_p5+6*(p.roi3_p95-p.roi3_p5)/8,4),',',ROUND(p.roi3_p5+7*(p.roi3_p95-p.roi3_p5)/8,4),']')
            WHEN roi_three_day <= p.roi3_p95                                            THEN CONCAT('Bin09_(',ROUND(p.roi3_p5+7*(p.roi3_p95-p.roi3_p5)/8,4),',',ROUND(p.roi3_p95,4),']')
            ELSE                                                                             CONCAT('Bin10_(',ROUND(p.roi3_p95,4),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 7. roi_seven_day
    SELECT 'roi_seven_day' AS metric, visitor_id,
        CASE
            WHEN roi_seven_day <= 0 OR roi_seven_day IS NULL THEN NULL
            WHEN roi_seven_day <= p.roi7_p5                                             THEN CONCAT('Bin01_(0,',ROUND(p.roi7_p5,4),']')
            WHEN roi_seven_day <= p.roi7_p5 + 1*(p.roi7_p95-p.roi7_p5)/8               THEN CONCAT('Bin02_(',ROUND(p.roi7_p5,4),',',ROUND(p.roi7_p5+1*(p.roi7_p95-p.roi7_p5)/8,4),']')
            WHEN roi_seven_day <= p.roi7_p5 + 2*(p.roi7_p95-p.roi7_p5)/8               THEN CONCAT('Bin03_(',ROUND(p.roi7_p5+1*(p.roi7_p95-p.roi7_p5)/8,4),',',ROUND(p.roi7_p5+2*(p.roi7_p95-p.roi7_p5)/8,4),']')
            WHEN roi_seven_day <= p.roi7_p5 + 3*(p.roi7_p95-p.roi7_p5)/8               THEN CONCAT('Bin04_(',ROUND(p.roi7_p5+2*(p.roi7_p95-p.roi7_p5)/8,4),',',ROUND(p.roi7_p5+3*(p.roi7_p95-p.roi7_p5)/8,4),']')
            WHEN roi_seven_day <= p.roi7_p5 + 4*(p.roi7_p95-p.roi7_p5)/8               THEN CONCAT('Bin05_(',ROUND(p.roi7_p5+3*(p.roi7_p95-p.roi7_p5)/8,4),',',ROUND(p.roi7_p5+4*(p.roi7_p95-p.roi7_p5)/8,4),']')
            WHEN roi_seven_day <= p.roi7_p5 + 5*(p.roi7_p95-p.roi7_p5)/8               THEN CONCAT('Bin06_(',ROUND(p.roi7_p5+4*(p.roi7_p95-p.roi7_p5)/8,4),',',ROUND(p.roi7_p5+5*(p.roi7_p95-p.roi7_p5)/8,4),']')
            WHEN roi_seven_day <= p.roi7_p5 + 6*(p.roi7_p95-p.roi7_p5)/8               THEN CONCAT('Bin07_(',ROUND(p.roi7_p5+5*(p.roi7_p95-p.roi7_p5)/8,4),',',ROUND(p.roi7_p5+6*(p.roi7_p95-p.roi7_p5)/8,4),']')
            WHEN roi_seven_day <= p.roi7_p5 + 7*(p.roi7_p95-p.roi7_p5)/8               THEN CONCAT('Bin08_(',ROUND(p.roi7_p5+6*(p.roi7_p95-p.roi7_p5)/8,4),',',ROUND(p.roi7_p5+7*(p.roi7_p95-p.roi7_p5)/8,4),']')
            WHEN roi_seven_day <= p.roi7_p95                                            THEN CONCAT('Bin09_(',ROUND(p.roi7_p5+7*(p.roi7_p95-p.roi7_p5)/8,4),',',ROUND(p.roi7_p95,4),']')
            ELSE                                                                             CONCAT('Bin10_(',ROUND(p.roi7_p95,4),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key
)

SELECT
    metric,
    bin_label,
    COUNT(visitor_id) AS visitor_cnt
FROM binned
WHERE bin_label IS NOT NULL
GROUP BY metric, bin_label
ORDER BY metric, bin_label
;
