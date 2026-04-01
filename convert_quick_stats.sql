-- ============================================================
-- 转化类后效大盘快速统计（均值/最值/中位数/p90/p99）
-- 数据来源：ks_origin_ad_log.ad_callback_log_from_ad_log_full
-- ============================================================

WITH uv_metrics AS (
    SELECT
        visitor_id,
        CAST(COUNT(DISTINCT llsid) AS DOUBLE)                                                  AS impression_cnt,
        CAST(SUM(e_ad_item_click) AS DOUBLE)                                                   AS click_cnt,
        CAST(SUM(CASE WHEN is_conversion      = true THEN 1 ELSE 0 END) AS DOUBLE)             AS convert_num,
        CAST(SUM(CASE WHEN is_deep_conversion = true THEN 1 ELSE 0 END) AS DOUBLE)             AS deep_convert_num,
        SUM(CASE WHEN action_type = charge_action_type THEN cost_total ELSE 0 END) / 1000.0    AS cost,
        SUM(callback_purchase_amount)                                                           AS event_pay
    FROM ks_origin_ad_log.ad_callback_log_from_ad_log_full
    WHERE p_date = '20260320'
      AND is_duplicate = false
      AND is_retry = false
    GROUP BY visitor_id
)

SELECT
    -- 覆盖UV
    COUNT(*)                                                                                   AS total_uv,
    COUNT(IF(impression_cnt    > 0, 1, NULL))                                                  AS uv_with_impr,
    COUNT(IF(click_cnt         > 0, 1, NULL))                                                  AS uv_with_click,
    COUNT(IF(convert_num       > 0, 1, NULL))                                                  AS uv_with_convert,
    COUNT(IF(deep_convert_num  > 0, 1, NULL))                                                  AS uv_with_deep_convert,
    COUNT(IF(cost              > 0, 1, NULL))                                                  AS uv_with_cost,
    COUNT(IF(event_pay         > 0, 1, NULL))                                                  AS uv_with_pay,

    -- impression_cnt 曝光数
    ROUND(AVG(IF(impression_cnt > 0, impression_cnt, NULL)), 2)                                AS impr_avg,
    ROUND(MAX(impression_cnt), 0)                                                              AS impr_max,
    ROUND(percentile_approx(IF(impression_cnt > 0, impression_cnt, NULL), 0.50), 2)           AS impr_p50,
    ROUND(percentile_approx(IF(impression_cnt > 0, impression_cnt, NULL), 0.90), 2)           AS impr_p90,
    ROUND(percentile_approx(IF(impression_cnt > 0, impression_cnt, NULL), 0.99), 2)           AS impr_p99,

    -- click_cnt 点击数
    ROUND(AVG(IF(click_cnt > 0, click_cnt, NULL)), 2)                                         AS click_avg,
    ROUND(MAX(click_cnt), 0)                                                                   AS click_max,
    ROUND(percentile_approx(IF(click_cnt > 0, click_cnt, NULL), 0.50), 2)                     AS click_p50,
    ROUND(percentile_approx(IF(click_cnt > 0, click_cnt, NULL), 0.90), 2)                     AS click_p90,
    ROUND(percentile_approx(IF(click_cnt > 0, click_cnt, NULL), 0.99), 2)                     AS click_p99,

    -- convert_num 浅度转化数
    ROUND(AVG(IF(convert_num > 0, convert_num, NULL)), 2)                                     AS convert_avg,
    ROUND(MAX(convert_num), 0)                                                                 AS convert_max,
    ROUND(percentile_approx(IF(convert_num > 0, convert_num, NULL), 0.50), 2)                 AS convert_p50,
    ROUND(percentile_approx(IF(convert_num > 0, convert_num, NULL), 0.90), 2)                 AS convert_p90,
    ROUND(percentile_approx(IF(convert_num > 0, convert_num, NULL), 0.99), 2)                 AS convert_p99,

    -- deep_convert_num 深度转化数
    ROUND(AVG(IF(deep_convert_num > 0, deep_convert_num, NULL)), 2)                           AS deep_convert_avg,
    ROUND(MAX(deep_convert_num), 0)                                                            AS deep_convert_max,
    ROUND(percentile_approx(IF(deep_convert_num > 0, deep_convert_num, NULL), 0.50), 2)       AS deep_convert_p50,
    ROUND(percentile_approx(IF(deep_convert_num > 0, deep_convert_num, NULL), 0.90), 2)       AS deep_convert_p90,

    -- cost 消耗（元）
    ROUND(AVG(IF(cost > 0, cost, NULL)), 4)                                                    AS cost_avg,
    ROUND(MAX(cost), 4)                                                                        AS cost_max,
    ROUND(percentile_approx(IF(cost > 0, cost, NULL), 0.50), 4)                               AS cost_p50,
    ROUND(percentile_approx(IF(cost > 0, cost, NULL), 0.90), 4)                               AS cost_p90,
    ROUND(percentile_approx(IF(cost > 0, cost, NULL), 0.99), 4)                               AS cost_p99,

    -- event_pay 支付金额（原始回传值，单位待确认）
    ROUND(AVG(IF(event_pay > 0, event_pay, NULL)), 2)                                          AS event_pay_avg,
    ROUND(MIN(IF(event_pay > 0, event_pay, NULL)), 2)                                          AS event_pay_min,
    ROUND(MAX(event_pay), 2)                                                                   AS event_pay_max,
    ROUND(percentile_approx(IF(event_pay > 0, event_pay, NULL), 0.50), 2)                     AS event_pay_p50,
    ROUND(percentile_approx(IF(event_pay > 0, event_pay, NULL), 0.90), 2)                     AS event_pay_p90,

    -- shallow_cvr 浅度转化率（仅有点击的UV）
    ROUND(AVG(IF(click_cnt > 0, convert_num / click_cnt, NULL)), 4)                           AS shallow_cvr_avg,
    ROUND(percentile_approx(IF(click_cnt > 0, convert_num / click_cnt, NULL), 0.50), 4)       AS shallow_cvr_p50,
    ROUND(percentile_approx(IF(click_cnt > 0, convert_num / click_cnt, NULL), 0.90), 4)       AS shallow_cvr_p90,

    -- deep_cvr 深度转化率
    ROUND(AVG(IF(click_cnt > 0, deep_convert_num / click_cnt, NULL)), 4)                      AS deep_cvr_avg,
    ROUND(percentile_approx(IF(click_cnt > 0, deep_convert_num / click_cnt, NULL), 0.50), 4)  AS deep_cvr_p50,

    -- deep_shallow_ratio 深浅比
    ROUND(AVG(IF(convert_num > 0, deep_convert_num / convert_num, NULL)), 4)                  AS deep_shallow_avg,
    ROUND(percentile_approx(IF(convert_num > 0, deep_convert_num / convert_num, NULL), 0.50), 4) AS deep_shallow_p50,

    -- impression_cvr 曝光转化率
    ROUND(AVG(IF(impression_cnt > 0, convert_num / impression_cnt, NULL)), 4)                 AS impr_cvr_avg,
    ROUND(percentile_approx(IF(impression_cnt > 0, convert_num / impression_cnt, NULL), 0.50), 4) AS impr_cvr_p50,

    -- shallow_pay_roi 浅度转化ROI（原始值）
    ROUND(AVG(IF(convert_num > 0, event_pay / convert_num, NULL)), 4)                         AS shallow_roi_avg,
    ROUND(percentile_approx(IF(convert_num > 0, event_pay / convert_num, NULL), 0.50), 4)     AS shallow_roi_p50,
    ROUND(percentile_approx(IF(convert_num > 0, event_pay / convert_num, NULL), 0.90), 4)     AS shallow_roi_p90,

    -- deep_pay_roi 深度转化ROI（原始值）
    ROUND(AVG(IF(deep_convert_num > 0, event_pay / deep_convert_num, NULL)), 4)               AS deep_roi_avg,
    ROUND(percentile_approx(IF(deep_convert_num > 0, event_pay / deep_convert_num, NULL), 0.50), 4) AS deep_roi_p50,
    ROUND(percentile_approx(IF(deep_convert_num > 0, event_pay / deep_convert_num, NULL), 0.90), 4) AS deep_roi_p90

FROM uv_metrics
;
