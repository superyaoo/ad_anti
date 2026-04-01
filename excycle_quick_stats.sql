-- 外循环后效大盘快速统计（均值/最值/中位数/p90/p99）
-- 比分箱版本快很多，适合先摸底

WITH uv_metrics AS (
    SELECT
        visitor_id,
        SUM(CASE WHEN action_type = charge_action_type THEN cost_total ELSE 0 END) / 1000.0                 AS excycle_cost,
        COUNT(DISTINCT IF(action_type = 'EVENT_ORDER_SUBMIT', llsid, NULL))                                  AS excycle_order_cnt,
        COUNT(DISTINCT IF(action_type = 'EVENT_PAID_REFUND',  llsid, NULL))                                  AS excycle_refund_cnt,
        SUM(CASE WHEN action_type = 'EVENT_ORDER_SUBMIT' THEN callback_purchase_amount ELSE 0 END)           AS excycle_order_amt_raw
    FROM ks_ad_antispam.ks_anticheat_small_log_hi
    WHERE p_date = '20260320'
      AND ocpc_action_type IN ('EVENT_ORDER_SUBMIT','AD_CID_ROAS','CID_ROAS','CID_EVENT_ORDER_PAID')
      AND is_duplicate = false
      AND is_retry = false
      AND visitor_id > 0
      AND medium_attribute NOT IN (2, 4)
    GROUP BY visitor_id
)

SELECT
    -- 覆盖UV数
    COUNT(*)                                                                                  AS total_uv,
    COUNT(IF(excycle_cost      > 0, 1, NULL))                                                 AS uv_with_cost,
    COUNT(IF(excycle_order_cnt > 0, 1, NULL))                                                 AS uv_with_order,
    COUNT(IF(excycle_refund_cnt > 0, 1, NULL))                                                AS uv_with_refund,

    -- excycle_cost（元）
    ROUND(AVG(IF(excycle_cost > 0, excycle_cost, NULL)), 4)                                   AS cost_avg,
    ROUND(MIN(IF(excycle_cost > 0, excycle_cost, NULL)), 4)                                   AS cost_min,
    ROUND(MAX(excycle_cost), 4)                                                               AS cost_max,
    ROUND(percentile_approx(IF(excycle_cost > 0, excycle_cost, NULL), 0.50), 4)              AS cost_p50,
    ROUND(percentile_approx(IF(excycle_cost > 0, excycle_cost, NULL), 0.90), 4)              AS cost_p90,
    ROUND(percentile_approx(IF(excycle_cost > 0, excycle_cost, NULL), 0.99), 4)              AS cost_p99,

    -- excycle_order_cnt
    ROUND(AVG(IF(excycle_order_cnt > 0, excycle_order_cnt, NULL)), 4)                        AS order_cnt_avg,
    MAX(excycle_order_cnt)                                                                    AS order_cnt_max,
    ROUND(percentile_approx(IF(excycle_order_cnt > 0, CAST(excycle_order_cnt AS DOUBLE), NULL), 0.50), 2) AS order_cnt_p50,
    ROUND(percentile_approx(IF(excycle_order_cnt > 0, CAST(excycle_order_cnt AS DOUBLE), NULL), 0.90), 2) AS order_cnt_p90,
    ROUND(percentile_approx(IF(excycle_order_cnt > 0, CAST(excycle_order_cnt AS DOUBLE), NULL), 0.99), 2) AS order_cnt_p99,

    -- excycle_refund_cnt
    ROUND(AVG(IF(excycle_refund_cnt > 0, excycle_refund_cnt, NULL)), 4)                      AS refund_cnt_avg,
    MAX(excycle_refund_cnt)                                                                   AS refund_cnt_max,
    ROUND(percentile_approx(IF(excycle_refund_cnt > 0, CAST(excycle_refund_cnt AS DOUBLE), NULL), 0.50), 2) AS refund_cnt_p50,
    ROUND(percentile_approx(IF(excycle_refund_cnt > 0, CAST(excycle_refund_cnt AS DOUBLE), NULL), 0.90), 2) AS refund_cnt_p90,

    -- excycle_refund_rate（退单率，仅统计有订单的UV）
    ROUND(AVG(IF(excycle_order_cnt > 0,
        excycle_refund_cnt / excycle_order_cnt, NULL)), 4)                                    AS refund_rate_avg,
    ROUND(percentile_approx(IF(excycle_order_cnt > 0,
        excycle_refund_cnt / excycle_order_cnt, NULL), 0.50), 4)                              AS refund_rate_p50,
    ROUND(percentile_approx(IF(excycle_order_cnt > 0,
        excycle_refund_cnt / excycle_order_cnt, NULL), 0.90), 4)                              AS refund_rate_p90,

    -- excycle_order_amt_raw（先看原始值，确认单位用）
    ROUND(AVG(IF(excycle_order_amt_raw > 0, excycle_order_amt_raw, NULL)), 2)                 AS order_amt_raw_avg,
    ROUND(MIN(IF(excycle_order_amt_raw > 0, excycle_order_amt_raw, NULL)), 2)                 AS order_amt_raw_min,
    ROUND(MAX(excycle_order_amt_raw), 2)                                                      AS order_amt_raw_max,
    ROUND(percentile_approx(IF(excycle_order_amt_raw > 0, excycle_order_amt_raw, NULL), 0.50), 2) AS order_amt_raw_p50

FROM uv_metrics
;
