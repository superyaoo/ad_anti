-- ============================================================
-- 四类后效指标 按 resource_type 单日汇总
-- 日期：20260401
-- 各类独立查询，列名与业务含义一一对应
-- ============================================================


-- ============================================================
-- 1. 浅层转化（曝光/点击类）
--    来源：ks_ad_antispam.ks_anticheat_small_log_hi
-- ============================================================
SELECT
    resource_type,
    COUNT(DISTINCT visitor_id)                                                                             AS uv,
    COUNT(DISTINCT IF(is_duplicate = false AND is_retry = false, llsid, NULL))                             AS impr_cnt,
    COUNT(DISTINCT IF(is_duplicate = false AND is_retry = false
        AND is_for_report_engine = false, llsid, NULL))                                                    AS spam_impr_cnt,
    SUM(IF(is_duplicate = false AND is_retry = false, e_ad_item_click, 0))                                 AS click_cnt,
    SUM(IF(is_duplicate = false AND is_retry = false
        AND is_for_report_engine = false, e_ad_item_click, 0))                                             AS spam_click_cnt,
    ROUND(SUM(IF(is_duplicate = false AND is_retry = false, e_ad_item_click, 0))
        / NULLIF(COUNT(DISTINCT IF(is_duplicate = false AND is_retry = false, llsid, NULL)), 0), 4)        AS ctr,
    ROUND(SUM(IF(action_type = charge_action_type, cost_total, 0)) / 1000.0
        / NULLIF(COUNT(DISTINCT IF(is_duplicate = false AND is_retry = false, llsid, NULL)) / 1000.0, 0), 4) AS cpm
FROM ks_ad_antispam.ks_anticheat_small_log_hi
WHERE p_date = '20260401'
  AND media_app_id IN ('kuaishou_nebula', 'kuaishou')
GROUP BY resource_type
ORDER BY resource_type
;


-- ============================================================
-- 2. 转化类后效
--    来源：ks_origin_ad_log.ad_callback_log_from_ad_log_full
-- ============================================================
SELECT
    resource_type,
    COUNT(DISTINCT visitor_id)                                                                             AS uv,
    COUNT(DISTINCT llsid)                                                                                  AS impr_cnt,
    SUM(e_ad_item_click)                                                                                   AS click_cnt,
    SUM(CASE WHEN is_conversion      = true THEN 1 ELSE 0 END)                                             AS convert_num,
    SUM(CASE WHEN is_deep_conversion = true THEN 1 ELSE 0 END)                                             AS deep_convert_num,
    SUM(CASE WHEN action_type = charge_action_type THEN cost_total ELSE 0 END) / 1000.0                    AS cost,
    ROUND(SUM(CASE WHEN is_conversion = true THEN 1 ELSE 0 END)
        / NULLIF(SUM(e_ad_item_click), 0), 4)                                                              AS shallow_cvr,
    ROUND(SUM(CASE WHEN is_deep_conversion = true THEN 1 ELSE 0 END)
        / NULLIF(SUM(e_ad_item_click), 0), 4)                                                              AS deep_cvr,
    ROUND(SUM(CASE WHEN is_deep_conversion = true THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN is_conversion = true THEN 1 ELSE 0 END), 0), 4)                             AS deep_shallow_ratio,
    ROUND(SUM(CASE WHEN is_conversion = true THEN 1 ELSE 0 END)
        / NULLIF(COUNT(DISTINCT llsid), 0), 4)                                                             AS impression_cvr
FROM ks_origin_ad_log.ad_callback_log_from_ad_log_full
WHERE p_date = '20260401'
  AND is_duplicate = false
  AND is_retry = false
GROUP BY resource_type
ORDER BY resource_type
;


-- ============================================================
-- 3. 线索类后效
--    来源：app_crm_clue_submit_back_link_data_all
--          + dim_ad_crm_clue_df + ks_ad_clue_crm_stat_df + dwd_ad_tfc_log_full_hi
-- ============================================================
SELECT
    clue.resource_type,
    COUNT(DISTINCT uv.visitor_id)                                                                          AS uv,
    COUNT(DISTINCT clue.id)                                                                                AS clue_cnt,
    COUNT(DISTINCT CASE WHEN is_repeat_clue = 0 THEN clue.id END)                                         AS build_clue_cnt,
    COUNT(DISTINCT CASE WHEN is_call_0d = 1 AND is_repeat_clue = 0 THEN clue.id END)                      AS call_cnt,
    COUNT(DISTINCT CASE WHEN is_call_connect_0d = 1 AND is_repeat_clue = 0 THEN clue.id END)              AS connect_cnt,
    COUNT(DISTINCT CASE WHEN call_time_duration >= 30 AND is_repeat_clue = 0 THEN clue.id END)            AS connect_30s_cnt,
    COUNT(DISTINCT CASE WHEN is_valid_clue = 1 AND is_repeat_clue = 0 THEN clue.id END)                   AS valid_clue_cnt,
    ROUND(
        COUNT(DISTINCT CASE WHEN is_call_connect_0d = 1 AND is_repeat_clue = 0 THEN clue.id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN is_call_0d = 1 AND is_repeat_clue = 0 THEN clue.id END), 0)
    , 4)                                                                                                   AS connect_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN call_time_duration >= 30 AND is_repeat_clue = 0 THEN clue.id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN is_call_0d = 1 AND is_repeat_clue = 0 THEN clue.id END), 0)
    , 4)                                                                                                   AS connect_30s_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN is_valid_clue = 1 AND is_repeat_clue = 0 THEN clue.id END)
        / NULLIF(COUNT(DISTINCT CASE WHEN is_repeat_clue = 0 THEN clue.id END), 0)
    , 4)                                                                                                   AS valid_clue_rate
FROM (
    SELECT t1.*
    FROM (
        SELECT DISTINCT resource_type, id,
            is_call_0d, is_repeat_clue, is_call_connect_0d,
            call_time_duration, is_valid_clue
        FROM ks_ad_dw.app_crm_clue_submit_back_link_data_all
        WHERE p_date = '20260401'
          AND create_time = '20260401'
          AND ad_source IN (1, 2, 3, 4, 5, 7, 9, 10)
          AND resource_type != 'UNION'
    ) t1
    INNER JOIN (
        SELECT DISTINCT id
        FROM ks_ad_dw.dim_ad_crm_clue_df
        WHERE p_date = '20260401'
          AND from_unixtime(CAST(create_time / 1000 AS BIGINT), 'yyyyMMdd') = '20260401'
          AND ad_source IN (1, 2, 3, 4, 5, 7, 9, 10)
          AND biz_id IN ('1047', '1011')
          AND attr_tag NOT IN (4, 5)
    ) t3 ON t1.id = t3.id
) clue
LEFT JOIN (
    SELECT DISTINCT CAST(llsid AS BIGINT) AS llsid, id
    FROM ks_ad_antispam.ks_ad_clue_crm_stat_df
    WHERE p_date = '20260401' AND llsid IS NOT NULL
) link ON clue.id = link.id
LEFT JOIN (
    SELECT DISTINCT llsid, visitor_id
    FROM ks_ad_dw.dwd_ad_tfc_log_full_hi
    WHERE p_date = '20260401'
      AND data_part = 'DSP'
      AND medium_attribute IN (1, 2)
      AND campaign_type = 'KWAI_PROMOTION_CONSULTATION'
      AND is_for_report_engine = true
) uv ON link.llsid = uv.llsid
GROUP BY clue.resource_type
ORDER BY clue.resource_type
;


-- ============================================================
-- 4. 下载付费类后效（首日，不含3/7日累计）
--    来源：ks_origin_ad_log.ad_callback_log_from_ad_log_full
-- ============================================================
SELECT
    resource_type,
    COUNT(DISTINCT visitor_id)                                                                             AS uv,
    COUNT(DISTINCT IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true, llsid, NULL))             AS pay_num,
    SUM(IF(action_type = charge_action_type AND is_for_report_engine = true, cost_total, 0)) / 1000.0      AS cost_total,
    SUM(IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true
        AND callback_purchase_amount > 0, callback_purchase_amount, 0))                                    AS pay_amount_first_day,
    ROUND(
        SUM(IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true
            AND callback_purchase_amount > 0, callback_purchase_amount, 0))
        / NULLIF(SUM(IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true
            AND callback_purchase_amount > 0, callback_purchase_amount, 0))
            / NULLIF(COUNT(DISTINCT IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true,
                llsid, NULL)), 0), 0)
    , 4)                                                                                                   AS avg_pay_amount,
    ROUND(
        SUM(IF(action_type = 'EVENT_PAY' AND is_for_report_engine = true
            AND callback_purchase_amount > 0, callback_purchase_amount, 0))
        / NULLIF(SUM(IF(action_type = charge_action_type AND is_for_report_engine = true,
            cost_total, 0)) / 1000.0, 0)
    , 4)                                                                                                   AS roi_first_day
FROM ks_origin_ad_log.ad_callback_log_from_ad_log_full
WHERE p_date = '20260401'
  AND is_duplicate = false
  AND is_retry = false
  AND is_for_report_engine = true
  AND ocpc_action_type IN ('AD_PURCHASE','AD_ROAS','EVENT_7_DAY_PAY_TIMES','AD_SEVEN_DAY_ROAS','AD_PURCHASE_CONVERSION')
GROUP BY resource_type
ORDER BY resource_type
;
