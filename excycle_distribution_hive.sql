-- ============================================================
-- 后效大盘分布分析 —— 外循环后效（Hive 版）
-- 数据来源：ks_ad_antispam.ks_anticheat_small_log_hi
-- ocpc_action_type：EVENT_ORDER_SUBMIT / AD_CID_ROAS / CID_ROAS / CID_EVENT_ORDER_PAID
-- 分箱策略：Bin1=(0,p5] | Bin2~9=p5~p95等距8箱 | Bin10=(p95,+∞)
-- 维度：visitor_id
-- ============================================================

WITH

uv_metrics AS (
    SELECT
        1                                                                                                    AS join_key,
        visitor_id,
        SUM(CASE WHEN action_type = charge_action_type THEN cost_total ELSE 0 END) / 1000.0                 AS excycle_cost,
        CAST(COUNT(DISTINCT IF(action_type = 'EVENT_ORDER_SUBMIT', llsid, NULL)) AS DOUBLE)                 AS excycle_order_cnt,
        CAST(COUNT(DISTINCT IF(action_type = 'EVENT_PAID_REFUND',  llsid, NULL)) AS DOUBLE)                 AS excycle_refund_cnt,
        SUM(CASE WHEN action_type = 'EVENT_ORDER_SUBMIT' THEN callback_purchase_amount ELSE 0 END) / 1000.0 AS excycle_order_amt
    FROM ks_ad_antispam.ks_anticheat_small_log_hi
    WHERE p_date = '20260320'
      AND ocpc_action_type IN ('EVENT_ORDER_SUBMIT','AD_CID_ROAS','CID_ROAS','CID_EVENT_ORDER_PAID')
      AND is_duplicate = false
      AND is_retry = false
      AND visitor_id > 0
      AND medium_attribute NOT IN (2, 4)
    GROUP BY visitor_id
),

uv_derived AS (
    SELECT
        join_key,
        visitor_id,
        excycle_cost,
        excycle_order_cnt,
        excycle_refund_cnt,
        IF(excycle_order_cnt > 0, excycle_refund_cnt / excycle_order_cnt,                 NULL) AS excycle_refund_rate,
        IF(excycle_cost      > 0, excycle_order_amt  / excycle_cost,                      NULL) AS excycle_roi
    FROM uv_metrics
),

percentiles AS (
    SELECT
        1                                                                                                    AS join_key,
        percentile_approx(IF(excycle_cost         > 0, excycle_cost,         NULL), 0.05) AS cost_p5,
        percentile_approx(IF(excycle_cost         > 0, excycle_cost,         NULL), 0.95) AS cost_p95,
        percentile_approx(IF(excycle_order_cnt    > 0, excycle_order_cnt,    NULL), 0.05) AS order_cnt_p5,
        percentile_approx(IF(excycle_order_cnt    > 0, excycle_order_cnt,    NULL), 0.95) AS order_cnt_p95,
        percentile_approx(IF(excycle_refund_cnt   > 0, excycle_refund_cnt,   NULL), 0.05) AS refund_cnt_p5,
        percentile_approx(IF(excycle_refund_cnt   > 0, excycle_refund_cnt,   NULL), 0.95) AS refund_cnt_p95,
        percentile_approx(IF(excycle_refund_rate  > 0, excycle_refund_rate,  NULL), 0.05) AS refund_rate_p5,
        percentile_approx(IF(excycle_refund_rate  > 0, excycle_refund_rate,  NULL), 0.95) AS refund_rate_p95,
        percentile_approx(IF(excycle_roi          > 0, excycle_roi,          NULL), 0.05) AS roi_p5,
        percentile_approx(IF(excycle_roi          > 0, excycle_roi,          NULL), 0.95) AS roi_p95
    FROM uv_derived
),

binned AS (
    -- 1. excycle_cost
    SELECT 'excycle_cost' AS metric, visitor_id,
        CASE
            WHEN excycle_cost <= 0 OR excycle_cost IS NULL THEN NULL
            WHEN excycle_cost <= p.cost_p5                                          THEN CONCAT('Bin01_(0,',ROUND(p.cost_p5,2),']')
            WHEN excycle_cost <= p.cost_p5 + 1*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin02_(',ROUND(p.cost_p5,2),',',ROUND(p.cost_p5+1*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN excycle_cost <= p.cost_p5 + 2*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin03_(',ROUND(p.cost_p5+1*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+2*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN excycle_cost <= p.cost_p5 + 3*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin04_(',ROUND(p.cost_p5+2*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+3*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN excycle_cost <= p.cost_p5 + 4*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin05_(',ROUND(p.cost_p5+3*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+4*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN excycle_cost <= p.cost_p5 + 5*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin06_(',ROUND(p.cost_p5+4*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+5*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN excycle_cost <= p.cost_p5 + 6*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin07_(',ROUND(p.cost_p5+5*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+6*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN excycle_cost <= p.cost_p5 + 7*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin08_(',ROUND(p.cost_p5+6*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+7*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN excycle_cost <= p.cost_p95                                         THEN CONCAT('Bin09_(',ROUND(p.cost_p5+7*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p95,2),']')
            ELSE                                                                         CONCAT('Bin10_(',ROUND(p.cost_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 2. excycle_order_cnt
    SELECT 'excycle_order_cnt' AS metric, visitor_id,
        CASE
            WHEN excycle_order_cnt <= 0 OR excycle_order_cnt IS NULL THEN NULL
            WHEN excycle_order_cnt <= p.order_cnt_p5                                                            THEN CONCAT('Bin01_(0,',ROUND(p.order_cnt_p5,2),']')
            WHEN excycle_order_cnt <= p.order_cnt_p5 + 1*(p.order_cnt_p95-p.order_cnt_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.order_cnt_p5,2),',',ROUND(p.order_cnt_p5+1*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN excycle_order_cnt <= p.order_cnt_p5 + 2*(p.order_cnt_p95-p.order_cnt_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.order_cnt_p5+1*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+2*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN excycle_order_cnt <= p.order_cnt_p5 + 3*(p.order_cnt_p95-p.order_cnt_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.order_cnt_p5+2*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+3*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN excycle_order_cnt <= p.order_cnt_p5 + 4*(p.order_cnt_p95-p.order_cnt_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.order_cnt_p5+3*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+4*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN excycle_order_cnt <= p.order_cnt_p5 + 5*(p.order_cnt_p95-p.order_cnt_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.order_cnt_p5+4*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+5*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN excycle_order_cnt <= p.order_cnt_p5 + 6*(p.order_cnt_p95-p.order_cnt_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.order_cnt_p5+5*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+6*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN excycle_order_cnt <= p.order_cnt_p5 + 7*(p.order_cnt_p95-p.order_cnt_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.order_cnt_p5+6*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+7*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN excycle_order_cnt <= p.order_cnt_p95                                                           THEN CONCAT('Bin09_(',ROUND(p.order_cnt_p5+7*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p95,2),']')
            ELSE                                                                                                     CONCAT('Bin10_(',ROUND(p.order_cnt_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 3. excycle_refund_cnt
    SELECT 'excycle_refund_cnt' AS metric, visitor_id,
        CASE
            WHEN excycle_refund_cnt <= 0 OR excycle_refund_cnt IS NULL THEN NULL
            WHEN excycle_refund_cnt <= p.refund_cnt_p5                                                              THEN CONCAT('Bin01_(0,',ROUND(p.refund_cnt_p5,2),']')
            WHEN excycle_refund_cnt <= p.refund_cnt_p5 + 1*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.refund_cnt_p5,2),',',ROUND(p.refund_cnt_p5+1*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN excycle_refund_cnt <= p.refund_cnt_p5 + 2*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.refund_cnt_p5+1*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+2*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN excycle_refund_cnt <= p.refund_cnt_p5 + 3*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.refund_cnt_p5+2*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+3*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN excycle_refund_cnt <= p.refund_cnt_p5 + 4*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.refund_cnt_p5+3*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+4*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN excycle_refund_cnt <= p.refund_cnt_p5 + 5*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.refund_cnt_p5+4*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+5*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN excycle_refund_cnt <= p.refund_cnt_p5 + 6*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.refund_cnt_p5+5*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+6*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN excycle_refund_cnt <= p.refund_cnt_p5 + 7*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.refund_cnt_p5+6*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+7*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN excycle_refund_cnt <= p.refund_cnt_p95                                                             THEN CONCAT('Bin09_(',ROUND(p.refund_cnt_p5+7*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p95,2),']')
            ELSE                                                                                                         CONCAT('Bin10_(',ROUND(p.refund_cnt_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 4. excycle_refund_rate
    SELECT 'excycle_refund_rate' AS metric, visitor_id,
        CASE
            WHEN excycle_refund_rate <= 0 OR excycle_refund_rate IS NULL THEN NULL
            WHEN excycle_refund_rate <= p.refund_rate_p5                                                                THEN CONCAT('Bin01_(0,',ROUND(p.refund_rate_p5,4),']')
            WHEN excycle_refund_rate <= p.refund_rate_p5 + 1*(p.refund_rate_p95-p.refund_rate_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.refund_rate_p5,4),',',ROUND(p.refund_rate_p5+1*(p.refund_rate_p95-p.refund_rate_p5)/8,4),']')
            WHEN excycle_refund_rate <= p.refund_rate_p5 + 2*(p.refund_rate_p95-p.refund_rate_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.refund_rate_p5+1*(p.refund_rate_p95-p.refund_rate_p5)/8,4),',',ROUND(p.refund_rate_p5+2*(p.refund_rate_p95-p.refund_rate_p5)/8,4),']')
            WHEN excycle_refund_rate <= p.refund_rate_p5 + 3*(p.refund_rate_p95-p.refund_rate_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.refund_rate_p5+2*(p.refund_rate_p95-p.refund_rate_p5)/8,4),',',ROUND(p.refund_rate_p5+3*(p.refund_rate_p95-p.refund_rate_p5)/8,4),']')
            WHEN excycle_refund_rate <= p.refund_rate_p5 + 4*(p.refund_rate_p95-p.refund_rate_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.refund_rate_p5+3*(p.refund_rate_p95-p.refund_rate_p5)/8,4),',',ROUND(p.refund_rate_p5+4*(p.refund_rate_p95-p.refund_rate_p5)/8,4),']')
            WHEN excycle_refund_rate <= p.refund_rate_p5 + 5*(p.refund_rate_p95-p.refund_rate_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.refund_rate_p5+4*(p.refund_rate_p95-p.refund_rate_p5)/8,4),',',ROUND(p.refund_rate_p5+5*(p.refund_rate_p95-p.refund_rate_p5)/8,4),']')
            WHEN excycle_refund_rate <= p.refund_rate_p5 + 6*(p.refund_rate_p95-p.refund_rate_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.refund_rate_p5+5*(p.refund_rate_p95-p.refund_rate_p5)/8,4),',',ROUND(p.refund_rate_p5+6*(p.refund_rate_p95-p.refund_rate_p5)/8,4),']')
            WHEN excycle_refund_rate <= p.refund_rate_p5 + 7*(p.refund_rate_p95-p.refund_rate_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.refund_rate_p5+6*(p.refund_rate_p95-p.refund_rate_p5)/8,4),',',ROUND(p.refund_rate_p5+7*(p.refund_rate_p95-p.refund_rate_p5)/8,4),']')
            WHEN excycle_refund_rate <= p.refund_rate_p95                                                               THEN CONCAT('Bin09_(',ROUND(p.refund_rate_p5+7*(p.refund_rate_p95-p.refund_rate_p5)/8,4),',',ROUND(p.refund_rate_p95,4),']')
            ELSE                                                                                                             CONCAT('Bin10_(',ROUND(p.refund_rate_p95,4),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 5. excycle_roi
    SELECT 'excycle_roi' AS metric, visitor_id,
        CASE
            WHEN excycle_roi <= 0 OR excycle_roi IS NULL THEN NULL
            WHEN excycle_roi <= p.roi_p5                                        THEN CONCAT('Bin01_(0,',ROUND(p.roi_p5,4),']')
            WHEN excycle_roi <= p.roi_p5 + 1*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin02_(',ROUND(p.roi_p5,4),',',ROUND(p.roi_p5+1*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN excycle_roi <= p.roi_p5 + 2*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin03_(',ROUND(p.roi_p5+1*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+2*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN excycle_roi <= p.roi_p5 + 3*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin04_(',ROUND(p.roi_p5+2*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+3*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN excycle_roi <= p.roi_p5 + 4*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin05_(',ROUND(p.roi_p5+3*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+4*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN excycle_roi <= p.roi_p5 + 5*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin06_(',ROUND(p.roi_p5+4*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+5*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN excycle_roi <= p.roi_p5 + 6*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin07_(',ROUND(p.roi_p5+5*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+6*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN excycle_roi <= p.roi_p5 + 7*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin08_(',ROUND(p.roi_p5+6*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+7*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN excycle_roi <= p.roi_p95                                       THEN CONCAT('Bin09_(',ROUND(p.roi_p5+7*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p95,4),']')
            ELSE                                                                     CONCAT('Bin10_(',ROUND(p.roi_p95,4),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key
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
