-- ============================================================
-- 后效大盘分布分析 —— 转化类后效（Hive 版）
-- 数据来源：ks_origin_ad_log.ad_callback_log_from_ad_log_full
-- 分箱策略：Bin1=(0,p5] | Bin2~9=p5~p95等距8箱 | Bin10=(p95,+∞)
-- 维度：visitor_id
-- ============================================================

WITH

uv_metrics AS (
    SELECT
        1                                                                                                                     AS join_key,
        visitor_id,
        CAST(COUNT(DISTINCT llsid) AS DOUBLE)                                                                                AS impression_cnt,
        CAST(SUM(e_ad_item_click) AS DOUBLE)                                                                                 AS click_cnt,
        CAST(SUM(CASE WHEN is_conversion      = true THEN 1 ELSE 0 END) AS DOUBLE)                                          AS convert_num,
        CAST(SUM(CASE WHEN is_deep_conversion = true THEN 1 ELSE 0 END) AS DOUBLE)                                          AS deep_convert_num,
        SUM(CASE WHEN action_type = charge_action_type THEN cost_total ELSE 0 END) / 1000.0                                  AS cost,
        SUM(callback_purchase_amount)                                                                                        AS event_pay
    FROM ks_origin_ad_log.ad_callback_log_from_ad_log_full
    WHERE p_date = '20260320'
      AND is_duplicate = false
      AND is_retry = false
    GROUP BY visitor_id
),

uv_derived AS (
    SELECT
        join_key,
        visitor_id,
        impression_cnt,
        click_cnt,
        convert_num,
        deep_convert_num,
        cost,
        event_pay,
        IF(click_cnt       > 0, convert_num      / click_cnt,       NULL) AS shallow_cvr,
        IF(click_cnt       > 0, deep_convert_num / click_cnt,       NULL) AS deep_cvr,
        IF(convert_num     > 0, deep_convert_num / convert_num,     NULL) AS deep_shallow_ratio,
        IF(impression_cnt  > 0, convert_num      / impression_cnt,  NULL) AS impression_cvr,
        IF(convert_num     > 0, event_pay        / convert_num,     NULL) AS shallow_pay_roi,
        IF(deep_convert_num > 0, event_pay       / deep_convert_num, NULL) AS deep_pay_roi
    FROM uv_metrics
),

percentiles AS (
    SELECT
        1                                                                                                                     AS join_key,
        percentile_approx(IF(impression_cnt     > 0, impression_cnt,     NULL), 0.05) AS impr_p5,
        percentile_approx(IF(impression_cnt     > 0, impression_cnt,     NULL), 0.95) AS impr_p95,
        percentile_approx(IF(click_cnt          > 0, click_cnt,          NULL), 0.05) AS click_p5,
        percentile_approx(IF(click_cnt          > 0, click_cnt,          NULL), 0.95) AS click_p95,
        percentile_approx(IF(convert_num        > 0, convert_num,        NULL), 0.05) AS convert_p5,
        percentile_approx(IF(convert_num        > 0, convert_num,        NULL), 0.95) AS convert_p95,
        percentile_approx(IF(deep_convert_num   > 0, deep_convert_num,   NULL), 0.05) AS deep_convert_p5,
        percentile_approx(IF(deep_convert_num   > 0, deep_convert_num,   NULL), 0.95) AS deep_convert_p95,
        percentile_approx(IF(cost               > 0, cost,               NULL), 0.05) AS cost_p5,
        percentile_approx(IF(cost               > 0, cost,               NULL), 0.95) AS cost_p95,
        percentile_approx(IF(event_pay          > 0, event_pay,          NULL), 0.05) AS event_pay_p5,
        percentile_approx(IF(event_pay          > 0, event_pay,          NULL), 0.95) AS event_pay_p95,
        percentile_approx(IF(shallow_cvr        > 0, shallow_cvr,        NULL), 0.05) AS shallow_cvr_p5,
        percentile_approx(IF(shallow_cvr        > 0, shallow_cvr,        NULL), 0.95) AS shallow_cvr_p95,
        percentile_approx(IF(deep_cvr           > 0, deep_cvr,           NULL), 0.05) AS deep_cvr_p5,
        percentile_approx(IF(deep_cvr           > 0, deep_cvr,           NULL), 0.95) AS deep_cvr_p95,
        percentile_approx(IF(deep_shallow_ratio > 0, deep_shallow_ratio, NULL), 0.05) AS deep_shallow_p5,
        percentile_approx(IF(deep_shallow_ratio > 0, deep_shallow_ratio, NULL), 0.95) AS deep_shallow_p95,
        percentile_approx(IF(impression_cvr     > 0, impression_cvr,     NULL), 0.05) AS impr_cvr_p5,
        percentile_approx(IF(impression_cvr     > 0, impression_cvr,     NULL), 0.95) AS impr_cvr_p95,
        percentile_approx(IF(shallow_pay_roi    > 0, shallow_pay_roi,    NULL), 0.05) AS shallow_roi_p5,
        percentile_approx(IF(shallow_pay_roi    > 0, shallow_pay_roi,    NULL), 0.95) AS shallow_roi_p95,
        percentile_approx(IF(deep_pay_roi       > 0, deep_pay_roi,       NULL), 0.05) AS deep_roi_p5,
        percentile_approx(IF(deep_pay_roi       > 0, deep_pay_roi,       NULL), 0.95) AS deep_roi_p95
    FROM uv_derived
),

binned AS (
    -- 1. impression_cnt
    SELECT 'impression_cnt' AS metric, visitor_id,
        CASE
            WHEN impression_cnt <= 0 OR impression_cnt IS NULL THEN NULL
            WHEN impression_cnt <= p.impr_p5                                        THEN CONCAT('Bin01_(0,',ROUND(p.impr_p5,2),']')
            WHEN impression_cnt <= p.impr_p5 + 1*(p.impr_p95-p.impr_p5)/8          THEN CONCAT('Bin02_(',ROUND(p.impr_p5,2),',',ROUND(p.impr_p5+1*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impression_cnt <= p.impr_p5 + 2*(p.impr_p95-p.impr_p5)/8          THEN CONCAT('Bin03_(',ROUND(p.impr_p5+1*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+2*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impression_cnt <= p.impr_p5 + 3*(p.impr_p95-p.impr_p5)/8          THEN CONCAT('Bin04_(',ROUND(p.impr_p5+2*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+3*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impression_cnt <= p.impr_p5 + 4*(p.impr_p95-p.impr_p5)/8          THEN CONCAT('Bin05_(',ROUND(p.impr_p5+3*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+4*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impression_cnt <= p.impr_p5 + 5*(p.impr_p95-p.impr_p5)/8          THEN CONCAT('Bin06_(',ROUND(p.impr_p5+4*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+5*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impression_cnt <= p.impr_p5 + 6*(p.impr_p95-p.impr_p5)/8          THEN CONCAT('Bin07_(',ROUND(p.impr_p5+5*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+6*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impression_cnt <= p.impr_p5 + 7*(p.impr_p95-p.impr_p5)/8          THEN CONCAT('Bin08_(',ROUND(p.impr_p5+6*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+7*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impression_cnt <= p.impr_p95                                       THEN CONCAT('Bin09_(',ROUND(p.impr_p5+7*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p95,2),']')
            ELSE                                                                         CONCAT('Bin10_(',ROUND(p.impr_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 2. click_cnt
    SELECT 'click_cnt' AS metric, visitor_id,
        CASE
            WHEN click_cnt <= 0 OR click_cnt IS NULL THEN NULL
            WHEN click_cnt <= p.click_p5                                        THEN CONCAT('Bin01_(0,',ROUND(p.click_p5,2),']')
            WHEN click_cnt <= p.click_p5 + 1*(p.click_p95-p.click_p5)/8        THEN CONCAT('Bin02_(',ROUND(p.click_p5,2),',',ROUND(p.click_p5+1*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5 + 2*(p.click_p95-p.click_p5)/8        THEN CONCAT('Bin03_(',ROUND(p.click_p5+1*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+2*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5 + 3*(p.click_p95-p.click_p5)/8        THEN CONCAT('Bin04_(',ROUND(p.click_p5+2*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+3*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5 + 4*(p.click_p95-p.click_p5)/8        THEN CONCAT('Bin05_(',ROUND(p.click_p5+3*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+4*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5 + 5*(p.click_p95-p.click_p5)/8        THEN CONCAT('Bin06_(',ROUND(p.click_p5+4*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+5*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5 + 6*(p.click_p95-p.click_p5)/8        THEN CONCAT('Bin07_(',ROUND(p.click_p5+5*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+6*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5 + 7*(p.click_p95-p.click_p5)/8        THEN CONCAT('Bin08_(',ROUND(p.click_p5+6*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+7*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p95                                       THEN CONCAT('Bin09_(',ROUND(p.click_p5+7*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p95,2),']')
            ELSE                                                                     CONCAT('Bin10_(',ROUND(p.click_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 3. convert_num
    SELECT 'convert_num' AS metric, visitor_id,
        CASE
            WHEN convert_num <= 0 OR convert_num IS NULL THEN NULL
            WHEN convert_num <= p.convert_p5                                                    THEN CONCAT('Bin01_(0,',ROUND(p.convert_p5,2),']')
            WHEN convert_num <= p.convert_p5 + 1*(p.convert_p95-p.convert_p5)/8                THEN CONCAT('Bin02_(',ROUND(p.convert_p5,2),',',ROUND(p.convert_p5+1*(p.convert_p95-p.convert_p5)/8,2),']')
            WHEN convert_num <= p.convert_p5 + 2*(p.convert_p95-p.convert_p5)/8                THEN CONCAT('Bin03_(',ROUND(p.convert_p5+1*(p.convert_p95-p.convert_p5)/8,2),',',ROUND(p.convert_p5+2*(p.convert_p95-p.convert_p5)/8,2),']')
            WHEN convert_num <= p.convert_p5 + 3*(p.convert_p95-p.convert_p5)/8                THEN CONCAT('Bin04_(',ROUND(p.convert_p5+2*(p.convert_p95-p.convert_p5)/8,2),',',ROUND(p.convert_p5+3*(p.convert_p95-p.convert_p5)/8,2),']')
            WHEN convert_num <= p.convert_p5 + 4*(p.convert_p95-p.convert_p5)/8                THEN CONCAT('Bin05_(',ROUND(p.convert_p5+3*(p.convert_p95-p.convert_p5)/8,2),',',ROUND(p.convert_p5+4*(p.convert_p95-p.convert_p5)/8,2),']')
            WHEN convert_num <= p.convert_p5 + 5*(p.convert_p95-p.convert_p5)/8                THEN CONCAT('Bin06_(',ROUND(p.convert_p5+4*(p.convert_p95-p.convert_p5)/8,2),',',ROUND(p.convert_p5+5*(p.convert_p95-p.convert_p5)/8,2),']')
            WHEN convert_num <= p.convert_p5 + 6*(p.convert_p95-p.convert_p5)/8                THEN CONCAT('Bin07_(',ROUND(p.convert_p5+5*(p.convert_p95-p.convert_p5)/8,2),',',ROUND(p.convert_p5+6*(p.convert_p95-p.convert_p5)/8,2),']')
            WHEN convert_num <= p.convert_p5 + 7*(p.convert_p95-p.convert_p5)/8                THEN CONCAT('Bin08_(',ROUND(p.convert_p5+6*(p.convert_p95-p.convert_p5)/8,2),',',ROUND(p.convert_p5+7*(p.convert_p95-p.convert_p5)/8,2),']')
            WHEN convert_num <= p.convert_p95                                                   THEN CONCAT('Bin09_(',ROUND(p.convert_p5+7*(p.convert_p95-p.convert_p5)/8,2),',',ROUND(p.convert_p95,2),']')
            ELSE                                                                                     CONCAT('Bin10_(',ROUND(p.convert_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 4. deep_convert_num
    SELECT 'deep_convert_num' AS metric, visitor_id,
        CASE
            WHEN deep_convert_num <= 0 OR deep_convert_num IS NULL THEN NULL
            WHEN deep_convert_num <= p.deep_convert_p5                                                              THEN CONCAT('Bin01_(0,',ROUND(p.deep_convert_p5,2),']')
            WHEN deep_convert_num <= p.deep_convert_p5 + 1*(p.deep_convert_p95-p.deep_convert_p5)/8                THEN CONCAT('Bin02_(',ROUND(p.deep_convert_p5,2),',',ROUND(p.deep_convert_p5+1*(p.deep_convert_p95-p.deep_convert_p5)/8,2),']')
            WHEN deep_convert_num <= p.deep_convert_p5 + 2*(p.deep_convert_p95-p.deep_convert_p5)/8                THEN CONCAT('Bin03_(',ROUND(p.deep_convert_p5+1*(p.deep_convert_p95-p.deep_convert_p5)/8,2),',',ROUND(p.deep_convert_p5+2*(p.deep_convert_p95-p.deep_convert_p5)/8,2),']')
            WHEN deep_convert_num <= p.deep_convert_p5 + 3*(p.deep_convert_p95-p.deep_convert_p5)/8                THEN CONCAT('Bin04_(',ROUND(p.deep_convert_p5+2*(p.deep_convert_p95-p.deep_convert_p5)/8,2),',',ROUND(p.deep_convert_p5+3*(p.deep_convert_p95-p.deep_convert_p5)/8,2),']')
            WHEN deep_convert_num <= p.deep_convert_p5 + 4*(p.deep_convert_p95-p.deep_convert_p5)/8                THEN CONCAT('Bin05_(',ROUND(p.deep_convert_p5+3*(p.deep_convert_p95-p.deep_convert_p5)/8,2),',',ROUND(p.deep_convert_p5+4*(p.deep_convert_p95-p.deep_convert_p5)/8,2),']')
            WHEN deep_convert_num <= p.deep_convert_p5 + 5*(p.deep_convert_p95-p.deep_convert_p5)/8                THEN CONCAT('Bin06_(',ROUND(p.deep_convert_p5+4*(p.deep_convert_p95-p.deep_convert_p5)/8,2),',',ROUND(p.deep_convert_p5+5*(p.deep_convert_p95-p.deep_convert_p5)/8,2),']')
            WHEN deep_convert_num <= p.deep_convert_p5 + 6*(p.deep_convert_p95-p.deep_convert_p5)/8                THEN CONCAT('Bin07_(',ROUND(p.deep_convert_p5+5*(p.deep_convert_p95-p.deep_convert_p5)/8,2),',',ROUND(p.deep_convert_p5+6*(p.deep_convert_p95-p.deep_convert_p5)/8,2),']')
            WHEN deep_convert_num <= p.deep_convert_p5 + 7*(p.deep_convert_p95-p.deep_convert_p5)/8                THEN CONCAT('Bin08_(',ROUND(p.deep_convert_p5+6*(p.deep_convert_p95-p.deep_convert_p5)/8,2),',',ROUND(p.deep_convert_p5+7*(p.deep_convert_p95-p.deep_convert_p5)/8,2),']')
            WHEN deep_convert_num <= p.deep_convert_p95                                                             THEN CONCAT('Bin09_(',ROUND(p.deep_convert_p5+7*(p.deep_convert_p95-p.deep_convert_p5)/8,2),',',ROUND(p.deep_convert_p95,2),']')
            ELSE                                                                                                         CONCAT('Bin10_(',ROUND(p.deep_convert_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 5. cost
    SELECT 'cost' AS metric, visitor_id,
        CASE
            WHEN cost <= 0 OR cost IS NULL THEN NULL
            WHEN cost <= p.cost_p5                                          THEN CONCAT('Bin01_(0,',ROUND(p.cost_p5,2),']')
            WHEN cost <= p.cost_p5 + 1*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin02_(',ROUND(p.cost_p5,2),',',ROUND(p.cost_p5+1*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost <= p.cost_p5 + 2*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin03_(',ROUND(p.cost_p5+1*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+2*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost <= p.cost_p5 + 3*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin04_(',ROUND(p.cost_p5+2*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+3*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost <= p.cost_p5 + 4*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin05_(',ROUND(p.cost_p5+3*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+4*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost <= p.cost_p5 + 5*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin06_(',ROUND(p.cost_p5+4*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+5*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost <= p.cost_p5 + 6*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin07_(',ROUND(p.cost_p5+5*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+6*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost <= p.cost_p5 + 7*(p.cost_p95-p.cost_p5)/8            THEN CONCAT('Bin08_(',ROUND(p.cost_p5+6*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p5+7*(p.cost_p95-p.cost_p5)/8,2),']')
            WHEN cost <= p.cost_p95                                         THEN CONCAT('Bin09_(',ROUND(p.cost_p5+7*(p.cost_p95-p.cost_p5)/8,2),',',ROUND(p.cost_p95,2),']')
            ELSE                                                                 CONCAT('Bin10_(',ROUND(p.cost_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 6. event_pay
    SELECT 'event_pay' AS metric, visitor_id,
        CASE
            WHEN event_pay <= 0 OR event_pay IS NULL THEN NULL
            WHEN event_pay <= p.event_pay_p5                                                    THEN CONCAT('Bin01_(0,',ROUND(p.event_pay_p5,2),']')
            WHEN event_pay <= p.event_pay_p5 + 1*(p.event_pay_p95-p.event_pay_p5)/8            THEN CONCAT('Bin02_(',ROUND(p.event_pay_p5,2),',',ROUND(p.event_pay_p5+1*(p.event_pay_p95-p.event_pay_p5)/8,2),']')
            WHEN event_pay <= p.event_pay_p5 + 2*(p.event_pay_p95-p.event_pay_p5)/8            THEN CONCAT('Bin03_(',ROUND(p.event_pay_p5+1*(p.event_pay_p95-p.event_pay_p5)/8,2),',',ROUND(p.event_pay_p5+2*(p.event_pay_p95-p.event_pay_p5)/8,2),']')
            WHEN event_pay <= p.event_pay_p5 + 3*(p.event_pay_p95-p.event_pay_p5)/8            THEN CONCAT('Bin04_(',ROUND(p.event_pay_p5+2*(p.event_pay_p95-p.event_pay_p5)/8,2),',',ROUND(p.event_pay_p5+3*(p.event_pay_p95-p.event_pay_p5)/8,2),']')
            WHEN event_pay <= p.event_pay_p5 + 4*(p.event_pay_p95-p.event_pay_p5)/8            THEN CONCAT('Bin05_(',ROUND(p.event_pay_p5+3*(p.event_pay_p95-p.event_pay_p5)/8,2),',',ROUND(p.event_pay_p5+4*(p.event_pay_p95-p.event_pay_p5)/8,2),']')
            WHEN event_pay <= p.event_pay_p5 + 5*(p.event_pay_p95-p.event_pay_p5)/8            THEN CONCAT('Bin06_(',ROUND(p.event_pay_p5+4*(p.event_pay_p95-p.event_pay_p5)/8,2),',',ROUND(p.event_pay_p5+5*(p.event_pay_p95-p.event_pay_p5)/8,2),']')
            WHEN event_pay <= p.event_pay_p5 + 6*(p.event_pay_p95-p.event_pay_p5)/8            THEN CONCAT('Bin07_(',ROUND(p.event_pay_p5+5*(p.event_pay_p95-p.event_pay_p5)/8,2),',',ROUND(p.event_pay_p5+6*(p.event_pay_p95-p.event_pay_p5)/8,2),']')
            WHEN event_pay <= p.event_pay_p5 + 7*(p.event_pay_p95-p.event_pay_p5)/8            THEN CONCAT('Bin08_(',ROUND(p.event_pay_p5+6*(p.event_pay_p95-p.event_pay_p5)/8,2),',',ROUND(p.event_pay_p5+7*(p.event_pay_p95-p.event_pay_p5)/8,2),']')
            WHEN event_pay <= p.event_pay_p95                                                   THEN CONCAT('Bin09_(',ROUND(p.event_pay_p5+7*(p.event_pay_p95-p.event_pay_p5)/8,2),',',ROUND(p.event_pay_p95,2),']')
            ELSE                                                                                     CONCAT('Bin10_(',ROUND(p.event_pay_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 7. shallow_cvr
    SELECT 'shallow_cvr' AS metric, visitor_id,
        CASE
            WHEN shallow_cvr <= 0 OR shallow_cvr IS NULL THEN NULL
            WHEN shallow_cvr <= p.shallow_cvr_p5                                                                THEN CONCAT('Bin01_(0,',ROUND(p.shallow_cvr_p5,4),']')
            WHEN shallow_cvr <= p.shallow_cvr_p5 + 1*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.shallow_cvr_p5,4),',',ROUND(p.shallow_cvr_p5+1*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),']')
            WHEN shallow_cvr <= p.shallow_cvr_p5 + 2*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.shallow_cvr_p5+1*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),',',ROUND(p.shallow_cvr_p5+2*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),']')
            WHEN shallow_cvr <= p.shallow_cvr_p5 + 3*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.shallow_cvr_p5+2*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),',',ROUND(p.shallow_cvr_p5+3*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),']')
            WHEN shallow_cvr <= p.shallow_cvr_p5 + 4*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.shallow_cvr_p5+3*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),',',ROUND(p.shallow_cvr_p5+4*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),']')
            WHEN shallow_cvr <= p.shallow_cvr_p5 + 5*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.shallow_cvr_p5+4*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),',',ROUND(p.shallow_cvr_p5+5*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),']')
            WHEN shallow_cvr <= p.shallow_cvr_p5 + 6*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.shallow_cvr_p5+5*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),',',ROUND(p.shallow_cvr_p5+6*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),']')
            WHEN shallow_cvr <= p.shallow_cvr_p5 + 7*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.shallow_cvr_p5+6*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),',',ROUND(p.shallow_cvr_p5+7*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),']')
            WHEN shallow_cvr <= p.shallow_cvr_p95                                                               THEN CONCAT('Bin09_(',ROUND(p.shallow_cvr_p5+7*(p.shallow_cvr_p95-p.shallow_cvr_p5)/8,4),',',ROUND(p.shallow_cvr_p95,4),']')
            ELSE                                                                                                     CONCAT('Bin10_(',ROUND(p.shallow_cvr_p95,4),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 8. deep_cvr
    SELECT 'deep_cvr' AS metric, visitor_id,
        CASE
            WHEN deep_cvr <= 0 OR deep_cvr IS NULL THEN NULL
            WHEN deep_cvr <= p.deep_cvr_p5                                                  THEN CONCAT('Bin01_(0,',ROUND(p.deep_cvr_p5,4),']')
            WHEN deep_cvr <= p.deep_cvr_p5 + 1*(p.deep_cvr_p95-p.deep_cvr_p5)/8            THEN CONCAT('Bin02_(',ROUND(p.deep_cvr_p5,4),',',ROUND(p.deep_cvr_p5+1*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),']')
            WHEN deep_cvr <= p.deep_cvr_p5 + 2*(p.deep_cvr_p95-p.deep_cvr_p5)/8            THEN CONCAT('Bin03_(',ROUND(p.deep_cvr_p5+1*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),',',ROUND(p.deep_cvr_p5+2*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),']')
            WHEN deep_cvr <= p.deep_cvr_p5 + 3*(p.deep_cvr_p95-p.deep_cvr_p5)/8            THEN CONCAT('Bin04_(',ROUND(p.deep_cvr_p5+2*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),',',ROUND(p.deep_cvr_p5+3*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),']')
            WHEN deep_cvr <= p.deep_cvr_p5 + 4*(p.deep_cvr_p95-p.deep_cvr_p5)/8            THEN CONCAT('Bin05_(',ROUND(p.deep_cvr_p5+3*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),',',ROUND(p.deep_cvr_p5+4*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),']')
            WHEN deep_cvr <= p.deep_cvr_p5 + 5*(p.deep_cvr_p95-p.deep_cvr_p5)/8            THEN CONCAT('Bin06_(',ROUND(p.deep_cvr_p5+4*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),',',ROUND(p.deep_cvr_p5+5*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),']')
            WHEN deep_cvr <= p.deep_cvr_p5 + 6*(p.deep_cvr_p95-p.deep_cvr_p5)/8            THEN CONCAT('Bin07_(',ROUND(p.deep_cvr_p5+5*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),',',ROUND(p.deep_cvr_p5+6*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),']')
            WHEN deep_cvr <= p.deep_cvr_p5 + 7*(p.deep_cvr_p95-p.deep_cvr_p5)/8            THEN CONCAT('Bin08_(',ROUND(p.deep_cvr_p5+6*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),',',ROUND(p.deep_cvr_p5+7*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),']')
            WHEN deep_cvr <= p.deep_cvr_p95                                                 THEN CONCAT('Bin09_(',ROUND(p.deep_cvr_p5+7*(p.deep_cvr_p95-p.deep_cvr_p5)/8,4),',',ROUND(p.deep_cvr_p95,4),']')
            ELSE                                                                                 CONCAT('Bin10_(',ROUND(p.deep_cvr_p95,4),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 9. deep_shallow_ratio
    SELECT 'deep_shallow_ratio' AS metric, visitor_id,
        CASE
            WHEN deep_shallow_ratio <= 0 OR deep_shallow_ratio IS NULL THEN NULL
            WHEN deep_shallow_ratio <= p.deep_shallow_p5                                                                    THEN CONCAT('Bin01_(0,',ROUND(p.deep_shallow_p5,4),']')
            WHEN deep_shallow_ratio <= p.deep_shallow_p5 + 1*(p.deep_shallow_p95-p.deep_shallow_p5)/8                      THEN CONCAT('Bin02_(',ROUND(p.deep_shallow_p5,4),',',ROUND(p.deep_shallow_p5+1*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),']')
            WHEN deep_shallow_ratio <= p.deep_shallow_p5 + 2*(p.deep_shallow_p95-p.deep_shallow_p5)/8                      THEN CONCAT('Bin03_(',ROUND(p.deep_shallow_p5+1*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),',',ROUND(p.deep_shallow_p5+2*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),']')
            WHEN deep_shallow_ratio <= p.deep_shallow_p5 + 3*(p.deep_shallow_p95-p.deep_shallow_p5)/8                      THEN CONCAT('Bin04_(',ROUND(p.deep_shallow_p5+2*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),',',ROUND(p.deep_shallow_p5+3*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),']')
            WHEN deep_shallow_ratio <= p.deep_shallow_p5 + 4*(p.deep_shallow_p95-p.deep_shallow_p5)/8                      THEN CONCAT('Bin05_(',ROUND(p.deep_shallow_p5+3*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),',',ROUND(p.deep_shallow_p5+4*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),']')
            WHEN deep_shallow_ratio <= p.deep_shallow_p5 + 5*(p.deep_shallow_p95-p.deep_shallow_p5)/8                      THEN CONCAT('Bin06_(',ROUND(p.deep_shallow_p5+4*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),',',ROUND(p.deep_shallow_p5+5*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),']')
            WHEN deep_shallow_ratio <= p.deep_shallow_p5 + 6*(p.deep_shallow_p95-p.deep_shallow_p5)/8                      THEN CONCAT('Bin07_(',ROUND(p.deep_shallow_p5+5*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),',',ROUND(p.deep_shallow_p5+6*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),']')
            WHEN deep_shallow_ratio <= p.deep_shallow_p5 + 7*(p.deep_shallow_p95-p.deep_shallow_p5)/8                      THEN CONCAT('Bin08_(',ROUND(p.deep_shallow_p5+6*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),',',ROUND(p.deep_shallow_p5+7*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),']')
            WHEN deep_shallow_ratio <= p.deep_shallow_p95                                                                   THEN CONCAT('Bin09_(',ROUND(p.deep_shallow_p5+7*(p.deep_shallow_p95-p.deep_shallow_p5)/8,4),',',ROUND(p.deep_shallow_p95,4),']')
            ELSE                                                                                                                 CONCAT('Bin10_(',ROUND(p.deep_shallow_p95,4),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 10. impression_cvr
    SELECT 'impression_cvr' AS metric, visitor_id,
        CASE
            WHEN impression_cvr <= 0 OR impression_cvr IS NULL THEN NULL
            WHEN impression_cvr <= p.impr_cvr_p5                                                                THEN CONCAT('Bin01_(0,',ROUND(p.impr_cvr_p5,4),']')
            WHEN impression_cvr <= p.impr_cvr_p5 + 1*(p.impr_cvr_p95-p.impr_cvr_p5)/8                         THEN CONCAT('Bin02_(',ROUND(p.impr_cvr_p5,4),',',ROUND(p.impr_cvr_p5+1*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),']')
            WHEN impression_cvr <= p.impr_cvr_p5 + 2*(p.impr_cvr_p95-p.impr_cvr_p5)/8                         THEN CONCAT('Bin03_(',ROUND(p.impr_cvr_p5+1*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),',',ROUND(p.impr_cvr_p5+2*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),']')
            WHEN impression_cvr <= p.impr_cvr_p5 + 3*(p.impr_cvr_p95-p.impr_cvr_p5)/8                         THEN CONCAT('Bin04_(',ROUND(p.impr_cvr_p5+2*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),',',ROUND(p.impr_cvr_p5+3*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),']')
            WHEN impression_cvr <= p.impr_cvr_p5 + 4*(p.impr_cvr_p95-p.impr_cvr_p5)/8                         THEN CONCAT('Bin05_(',ROUND(p.impr_cvr_p5+3*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),',',ROUND(p.impr_cvr_p5+4*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),']')
            WHEN impression_cvr <= p.impr_cvr_p5 + 5*(p.impr_cvr_p95-p.impr_cvr_p5)/8                         THEN CONCAT('Bin06_(',ROUND(p.impr_cvr_p5+4*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),',',ROUND(p.impr_cvr_p5+5*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),']')
            WHEN impression_cvr <= p.impr_cvr_p5 + 6*(p.impr_cvr_p95-p.impr_cvr_p5)/8                         THEN CONCAT('Bin07_(',ROUND(p.impr_cvr_p5+5*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),',',ROUND(p.impr_cvr_p5+6*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),']')
            WHEN impression_cvr <= p.impr_cvr_p5 + 7*(p.impr_cvr_p95-p.impr_cvr_p5)/8                         THEN CONCAT('Bin08_(',ROUND(p.impr_cvr_p5+6*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),',',ROUND(p.impr_cvr_p5+7*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),']')
            WHEN impression_cvr <= p.impr_cvr_p95                                                               THEN CONCAT('Bin09_(',ROUND(p.impr_cvr_p5+7*(p.impr_cvr_p95-p.impr_cvr_p5)/8,4),',',ROUND(p.impr_cvr_p95,4),']')
            ELSE                                                                                                     CONCAT('Bin10_(',ROUND(p.impr_cvr_p95,4),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 11. shallow_pay_roi
    SELECT 'shallow_pay_roi' AS metric, visitor_id,
        CASE
            WHEN shallow_pay_roi <= 0 OR shallow_pay_roi IS NULL THEN NULL
            WHEN shallow_pay_roi <= p.shallow_roi_p5                                                                THEN CONCAT('Bin01_(0,',ROUND(p.shallow_roi_p5,4),']')
            WHEN shallow_pay_roi <= p.shallow_roi_p5 + 1*(p.shallow_roi_p95-p.shallow_roi_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.shallow_roi_p5,4),',',ROUND(p.shallow_roi_p5+1*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),']')
            WHEN shallow_pay_roi <= p.shallow_roi_p5 + 2*(p.shallow_roi_p95-p.shallow_roi_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.shallow_roi_p5+1*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),',',ROUND(p.shallow_roi_p5+2*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),']')
            WHEN shallow_pay_roi <= p.shallow_roi_p5 + 3*(p.shallow_roi_p95-p.shallow_roi_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.shallow_roi_p5+2*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),',',ROUND(p.shallow_roi_p5+3*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),']')
            WHEN shallow_pay_roi <= p.shallow_roi_p5 + 4*(p.shallow_roi_p95-p.shallow_roi_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.shallow_roi_p5+3*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),',',ROUND(p.shallow_roi_p5+4*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),']')
            WHEN shallow_pay_roi <= p.shallow_roi_p5 + 5*(p.shallow_roi_p95-p.shallow_roi_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.shallow_roi_p5+4*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),',',ROUND(p.shallow_roi_p5+5*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),']')
            WHEN shallow_pay_roi <= p.shallow_roi_p5 + 6*(p.shallow_roi_p95-p.shallow_roi_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.shallow_roi_p5+5*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),',',ROUND(p.shallow_roi_p5+6*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),']')
            WHEN shallow_pay_roi <= p.shallow_roi_p5 + 7*(p.shallow_roi_p95-p.shallow_roi_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.shallow_roi_p5+6*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),',',ROUND(p.shallow_roi_p5+7*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),']')
            WHEN shallow_pay_roi <= p.shallow_roi_p95                                                               THEN CONCAT('Bin09_(',ROUND(p.shallow_roi_p5+7*(p.shallow_roi_p95-p.shallow_roi_p5)/8,4),',',ROUND(p.shallow_roi_p95,4),']')
            ELSE                                                                                                         CONCAT('Bin10_(',ROUND(p.shallow_roi_p95,4),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 12. deep_pay_roi
    SELECT 'deep_pay_roi' AS metric, visitor_id,
        CASE
            WHEN deep_pay_roi <= 0 OR deep_pay_roi IS NULL THEN NULL
            WHEN deep_pay_roi <= p.deep_roi_p5                                                          THEN CONCAT('Bin01_(0,',ROUND(p.deep_roi_p5,4),']')
            WHEN deep_pay_roi <= p.deep_roi_p5 + 1*(p.deep_roi_p95-p.deep_roi_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.deep_roi_p5,4),',',ROUND(p.deep_roi_p5+1*(p.deep_roi_p95-p.deep_roi_p5)/8,4),']')
            WHEN deep_pay_roi <= p.deep_roi_p5 + 2*(p.deep_roi_p95-p.deep_roi_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.deep_roi_p5+1*(p.deep_roi_p95-p.deep_roi_p5)/8,4),',',ROUND(p.deep_roi_p5+2*(p.deep_roi_p95-p.deep_roi_p5)/8,4),']')
            WHEN deep_pay_roi <= p.deep_roi_p5 + 3*(p.deep_roi_p95-p.deep_roi_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.deep_roi_p5+2*(p.deep_roi_p95-p.deep_roi_p5)/8,4),',',ROUND(p.deep_roi_p5+3*(p.deep_roi_p95-p.deep_roi_p5)/8,4),']')
            WHEN deep_pay_roi <= p.deep_roi_p5 + 4*(p.deep_roi_p95-p.deep_roi_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.deep_roi_p5+3*(p.deep_roi_p95-p.deep_roi_p5)/8,4),',',ROUND(p.deep_roi_p5+4*(p.deep_roi_p95-p.deep_roi_p5)/8,4),']')
            WHEN deep_pay_roi <= p.deep_roi_p5 + 5*(p.deep_roi_p95-p.deep_roi_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.deep_roi_p5+4*(p.deep_roi_p95-p.deep_roi_p5)/8,4),',',ROUND(p.deep_roi_p5+5*(p.deep_roi_p95-p.deep_roi_p5)/8,4),']')
            WHEN deep_pay_roi <= p.deep_roi_p5 + 6*(p.deep_roi_p95-p.deep_roi_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.deep_roi_p5+5*(p.deep_roi_p95-p.deep_roi_p5)/8,4),',',ROUND(p.deep_roi_p5+6*(p.deep_roi_p95-p.deep_roi_p5)/8,4),']')
            WHEN deep_pay_roi <= p.deep_roi_p5 + 7*(p.deep_roi_p95-p.deep_roi_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.deep_roi_p5+6*(p.deep_roi_p95-p.deep_roi_p5)/8,4),',',ROUND(p.deep_roi_p5+7*(p.deep_roi_p95-p.deep_roi_p5)/8,4),']')
            WHEN deep_pay_roi <= p.deep_roi_p95                                                         THEN CONCAT('Bin09_(',ROUND(p.deep_roi_p5+7*(p.deep_roi_p95-p.deep_roi_p5)/8,4),',',ROUND(p.deep_roi_p95,4),']')
            ELSE                                                                                             CONCAT('Bin10_(',ROUND(p.deep_roi_p95,4),',+inf)')
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
