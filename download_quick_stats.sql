-- ============================================================
-- 下载付费类后效大盘快速统计（均值/最值/中位数/p90/p99）
-- 归因日：20260320（首日消耗 + 3日/7日累计付费）
-- ============================================================

WITH

pay_info AS (
    SELECT
        visitor_id,
        SUM(IF(action_type = charge_action_type AND is_for_report_engine = true, cost_total, 0)) / 1000.0 AS cost_total,
        CAST(COUNT(DISTINCT IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true, llsid, NULL)) AS DOUBLE) AS pay_num,
        SUM(IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true AND callback_purchase_amount > 0,
            callback_purchase_amount, 0))                                                                  AS pay_amount,
        SUM(IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true AND callback_purchase_amount > 0,
            callback_purchase_amount, 0))                                                                  AS pay_amount_first_day,
        0.0 AS pay_amount_three_day,
        0.0 AS pay_amount_seven_day
    FROM ks_origin_ad_log.ad_callback_log_from_ad_log_full
    WHERE p_date = '20260320'
      AND is_duplicate = false
      AND is_retry = false
      AND is_for_report_engine = true
      AND ocpc_action_type IN ('AD_PURCHASE','AD_ROAS','EVENT_7_DAY_PAY_TIMES','AD_SEVEN_DAY_ROAS','AD_PURCHASE_CONVERSION')
    GROUP BY visitor_id

    UNION ALL

    SELECT
        visitor_id,
        0.0 AS cost_total,
        0.0 AS pay_num,
        SUM(IF(
            is_for_report_engine = true AND action_type = 'EVENT_PAY' AND callback_purchase_amount > 0
            AND from_unixtime(CAST(conversion_time / 1000 AS BIGINT), 'yyyyMMdd') = '20260320',
            callback_purchase_amount, 0))                                                                  AS pay_amount,
        0.0 AS pay_amount_first_day,
        SUM(IF(
            is_for_report_engine = true AND action_type = 'EVENT_PAY' AND callback_purchase_amount > 0
            AND from_unixtime(CAST(conversion_time / 1000 AS BIGINT), 'yyyyMMdd') = '20260320'
            AND p_date <= '20260322',
            callback_purchase_amount, 0))                                                                  AS pay_amount_three_day,
        SUM(IF(
            is_for_report_engine = true AND action_type = 'EVENT_PAY' AND callback_purchase_amount > 0
            AND from_unixtime(CAST(conversion_time / 1000 AS BIGINT), 'yyyyMMdd') = '20260320'
            AND p_date <= '20260326',
            callback_purchase_amount, 0))                                                                  AS pay_amount_seven_day
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
)

SELECT
    -- 覆盖UV
    COUNT(*)                                                                                        AS total_uv,
    COUNT(IF(cost_total  > 0, 1, NULL))                                                             AS uv_with_cost,
    COUNT(IF(pay_num     > 0, 1, NULL))                                                             AS uv_with_pay,
    COUNT(IF(pay_amount  > 0, 1, NULL))                                                             AS uv_with_pay_amount,

    -- cost_total 正常流量消耗（元）
    ROUND(AVG(IF(cost_total > 0, cost_total, NULL)), 4)                                             AS cost_avg,
    ROUND(MAX(cost_total), 4)                                                                       AS cost_max,
    ROUND(percentile_approx(IF(cost_total > 0, cost_total, NULL), 0.50), 4)                        AS cost_p50,
    ROUND(percentile_approx(IF(cost_total > 0, cost_total, NULL), 0.90), 4)                        AS cost_p90,
    ROUND(percentile_approx(IF(cost_total > 0, cost_total, NULL), 0.99), 4)                        AS cost_p99,

    -- pay_num 付费次数
    ROUND(AVG(IF(pay_num > 0, pay_num, NULL)), 4)                                                   AS pay_num_avg,
    ROUND(MAX(pay_num), 0)                                                                          AS pay_num_max,
    ROUND(percentile_approx(IF(pay_num > 0, pay_num, NULL), 0.50), 2)                              AS pay_num_p50,
    ROUND(percentile_approx(IF(pay_num > 0, pay_num, NULL), 0.90), 2)                              AS pay_num_p90,
    ROUND(percentile_approx(IF(pay_num > 0, pay_num, NULL), 0.99), 2)                              AS pay_num_p99,

    -- pay_amount 7日全量付费金额（原始值，单位待确认）
    ROUND(AVG(IF(pay_amount > 0, pay_amount, NULL)), 2)                                             AS pay_amount_avg,
    ROUND(MIN(IF(pay_amount > 0, pay_amount, NULL)), 2)                                             AS pay_amount_min,
    ROUND(MAX(pay_amount), 2)                                                                       AS pay_amount_max,
    ROUND(percentile_approx(IF(pay_amount > 0, pay_amount, NULL), 0.50), 2)                        AS pay_amount_p50,
    ROUND(percentile_approx(IF(pay_amount > 0, pay_amount, NULL), 0.90), 2)                        AS pay_amount_p90,
    ROUND(percentile_approx(IF(pay_amount > 0, pay_amount, NULL), 0.99), 2)                        AS pay_amount_p99,

    -- avg_pay_amount 次均付费金额
    ROUND(AVG(IF(pay_num > 0, pay_amount / pay_num, NULL)), 2)                                     AS avg_pay_amount_avg,
    ROUND(percentile_approx(IF(pay_num > 0, pay_amount / pay_num, NULL), 0.50), 2)                 AS avg_pay_amount_p50,
    ROUND(percentile_approx(IF(pay_num > 0, pay_amount / pay_num, NULL), 0.90), 2)                 AS avg_pay_amount_p90,

    -- roi_first_day 首日ROI
    ROUND(AVG(IF(cost_total > 0, pay_amount_first_day / cost_total, NULL)), 4)                     AS roi1_avg,
    ROUND(percentile_approx(IF(cost_total > 0, pay_amount_first_day / cost_total, NULL), 0.50), 4) AS roi1_p50,
    ROUND(percentile_approx(IF(cost_total > 0, pay_amount_first_day / cost_total, NULL), 0.90), 4) AS roi1_p90,

    -- roi_three_day 3日ROI
    ROUND(AVG(IF(cost_total > 0,
        (pay_amount_first_day + pay_amount_three_day) / cost_total, NULL)), 4)                     AS roi3_avg,
    ROUND(percentile_approx(IF(cost_total > 0,
        (pay_amount_first_day + pay_amount_three_day) / cost_total, NULL), 0.50), 4)               AS roi3_p50,
    ROUND(percentile_approx(IF(cost_total > 0,
        (pay_amount_first_day + pay_amount_three_day) / cost_total, NULL), 0.90), 4)               AS roi3_p90,

    -- roi_seven_day 7日ROI
    ROUND(AVG(IF(cost_total > 0,
        (pay_amount_first_day + pay_amount_seven_day) / cost_total, NULL)), 4)                     AS roi7_avg,
    ROUND(percentile_approx(IF(cost_total > 0,
        (pay_amount_first_day + pay_amount_seven_day) / cost_total, NULL), 0.50), 4)               AS roi7_p50,
    ROUND(percentile_approx(IF(cost_total > 0,
        (pay_amount_first_day + pay_amount_seven_day) / cost_total, NULL), 0.90), 4)               AS roi7_p90

FROM pay_agg
;
