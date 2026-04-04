-- ============================================================
-- 转化类后效 反作弊Hive版（对齐CK口径）
-- 曝光/点击/消耗来源：ks_ad_antispam.ks_anticheat_small_log_hi
-- 转化/支付来源：    ks_origin_ad_log.ad_callback_log_from_ad_log_full
-- 日期：20260403（对应CK的 2026-04-03）
-- JOIN维度：visitor_id
-- ============================================================

WITH

-- 曝光/点击/消耗：来自 smalllog，正常流量
impr_click AS (
    SELECT
        visitor_id,
        COUNT(DISTINCT IF(is_duplicate = false AND is_retry = false, llsid, NULL)) AS impr_cnt,
        SUM(IF(is_duplicate = false AND is_retry = false, e_ad_item_click, 0))     AS click_cnt,
        SUM(IF(action_type = charge_action_type, cost_total, 0)) / 1000.0          AS cost
    FROM ks_ad_antispam.ks_anticheat_small_log_hi
    WHERE p_date = '20260403'
      AND media_app_id IN ('kuaishou_nebula', 'kuaishou')
      AND is_for_report_engine = true
    GROUP BY visitor_id
),

-- 转化/支付：来自 callback_log
convert_info AS (
    SELECT
        visitor_id,
        SUM(CASE WHEN is_conversion      = true THEN 1 ELSE 0 END)                AS conversion_num,
        SUM(CASE WHEN is_deep_conversion = true THEN 1 ELSE 0 END)                AS deep_conversion_num,
        SUM(callback_purchase_amount)                                              AS event_pay,
        COUNT(DISTINCT IF(action_type = 'EVENT_PAY', llsid, NULL))                AS event_conversion
    FROM ks_origin_ad_log.ad_callback_log_from_ad_log_full
    WHERE p_date = '20260403'
      AND is_duplicate = false
      AND is_retry = false
    GROUP BY visitor_id
),

-- 按 visitor_id JOIN，保留所有有曝光或有转化的用户
joined AS (
    SELECT
        COALESCE(i.visitor_id, c.visitor_id) AS visitor_id,
        COALESCE(i.impr_cnt,  0)             AS impr_cnt,
        COALESCE(i.click_cnt, 0)             AS click_cnt,
        COALESCE(i.cost,      0)             AS cost,
        COALESCE(c.conversion_num,      0)   AS conversion_num,
        COALESCE(c.deep_conversion_num, 0)   AS deep_conversion_num,
        COALESCE(c.event_pay,           0)   AS event_pay,
        COALESCE(c.event_conversion,    0)   AS event_conversion
    FROM impr_click i
    FULL OUTER JOIN convert_info c ON i.visitor_id = c.visitor_id
)

SELECT
    '20260403'                                                                         AS dt,

    -- 消耗（元）
    ROUND(SUM(cost), 4)                                                                AS cost,

    -- 浅度转化数（对应 CK _4）
    SUM(conversion_num)                                                                AS conversion_num,

    -- 深度转化数（对应 CK _3）
    SUM(deep_conversion_num)                                                           AS deep_conversion_num,

    -- 浅度CVR = 转化数 / 点击数（对应 CK _2）
    ROUND(SUM(conversion_num) / NULLIF(SUM(click_cnt), 0), 6)                         AS shallow_cvr,

    -- 深浅比 = 深度转化数 / 浅度转化数（对应 CK _7）
    ROUND(SUM(deep_conversion_num) / NULLIF(SUM(conversion_num), 0), 4)               AS deep_shallow_ratio,

    -- 次均支付金额 = event_pay / event_conversion（对应 CK _5）
    ROUND(SUM(event_pay) / NULLIF(SUM(event_conversion), 0), 4)                       AS avg_pay_per_conversion,

    -- 支付转化率 = event_conversion / 曝光数（对应 CK _6）
    ROUND(SUM(event_conversion) / NULLIF(SUM(impr_cnt), 0), 6)                        AS pay_conversion_rate,

    -- 附加：深度CVR = 深度转化数 / 点击数
    ROUND(SUM(deep_conversion_num) / NULLIF(SUM(click_cnt), 0), 6)                    AS deep_cvr,

    -- 附加：ROI = 支付金额 / 消耗
    ROUND(SUM(event_pay) / NULLIF(SUM(cost), 0), 4)                                   AS roi

FROM joined
;
