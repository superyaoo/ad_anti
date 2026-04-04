-- ============================================================
-- 转化类后效 反作弊Hive版（对齐CK口径）
-- 曝光/点击/消耗来源：ks_ad_antispam.ks_anticheat_small_log_hi
-- 转化/支付来源：    ks_origin_ad_log.ad_callback_log_from_ad_log_full
-- 日期：20260403（对应CK的 2026-04-03）
-- JOIN维度：visitor_id
-- ============================================================
-- CK字段映射：
--   conversion_num           ← SUM(e_event_conversion)        [0/1标记位]
--   old_deep_conversion_num  ← SUM(is_deep_conversion=true)
--   event_pay                ← SUM(callback_purchase_amount)
--   event_conversion（_5/_6分母） ← SUM(e_event_conversion)   [SIMPLEAGGREGATEFUNCTION(SUM,INT64)确认]
--   ad_item_click            ← smalllog SUM(e_ad_item_click)
--   ad_item_impression       ← smalllog COUNT(DISTINCT llsid)
-- ============================================================

WITH

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

convert_info AS (
    SELECT
        visitor_id,
        SUM(e_event_conversion)                                                    AS conversion_num,
        SUM(CASE WHEN is_deep_conversion = true THEN 1 ELSE 0 END)                AS deep_conversion_num,
        SUM(callback_purchase_amount)                                              AS event_pay,
        SUM(e_event_conversion)                                                    AS event_conversion
    FROM ks_origin_ad_log.ad_callback_log_from_ad_log_full
    WHERE p_date = '20260403'
      AND is_duplicate = false
      AND is_retry = false
    GROUP BY visitor_id
),

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
    '20260403'                                                                     AS dt,

    ROUND(SUM(cost), 4)                                                            AS cost,
    SUM(conversion_num)                                                            AS conversion_num,
    SUM(deep_conversion_num)                                                       AS deep_conversion_num,
    SUM(event_pay)                                                                 AS event_pay,
    SUM(event_conversion)                                                          AS event_conversion,

    ROUND(SUM(conversion_num)      / NULLIF(SUM(click_cnt),        0), 6)         AS shallow_cvr,
    ROUND(SUM(deep_conversion_num) / NULLIF(SUM(conversion_num),   0), 4)         AS deep_shallow_ratio,
    ROUND(SUM(event_pay)           / NULLIF(SUM(event_conversion),  0), 4)        AS avg_pay_per_conversion,
    ROUND(SUM(event_conversion)    / NULLIF(SUM(impr_cnt),          0), 6)        AS pay_conversion_rate,
    ROUND(SUM(deep_conversion_num) / NULLIF(SUM(click_cnt),         0), 6)        AS deep_cvr,
    ROUND(SUM(event_pay)           / NULLIF(SUM(cost),              0), 4)        AS roi

FROM joined
;
