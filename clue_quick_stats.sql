-- ============================================================
-- 线索类后效大盘快速统计（均值/最值/中位数/p90/p99）
-- 比分箱版快很多，适合先摸底数据量级
-- ============================================================

WITH

clue AS (
    SELECT t1.*
    FROM (
        SELECT
            DISTINCT create_time, p_date, account_id, resource_type,
            unit_id, pos_id, id, clue_biz_stat,
            is_call_stat_negative, clue_convert_stat,
            is_tag_intention_negative, is_call_0d, is_repeat_clue, is_call_connect_0d,
            call_time_duration, is_valid_clue,
            CASE
                WHEN clue_biz_stat = 3
                  OR is_call_stat_negative = 1
                  OR clue_convert_stat IN (8, 10)
                  OR is_tag_intention_negative = 1 THEN 1
                ELSE 0
            END AS negtive
        FROM ks_ad_dw.app_crm_clue_submit_back_link_data_all
        WHERE p_date = '20260317'
          AND create_time BETWEEN '20260302' AND '20260315'
          AND ad_source IN (1, 2, 3, 4, 5, 7, 9, 10)
          AND resource_type != 'UNION'
    ) t1
    INNER JOIN (
        SELECT DISTINCT id
        FROM ks_ad_dw.dim_ad_crm_clue_df
        WHERE p_date = '20260317'
          AND from_unixtime(CAST(create_time / 1000 AS BIGINT), 'yyyyMMdd') BETWEEN '20260302' AND '20260315'
          AND ad_source IN (1, 2, 3, 4, 5, 7, 9, 10)
          AND biz_id IN ('1047', '1011')
          AND attr_tag NOT IN (4, 5)
    ) t3 ON t1.id = t3.id
),

link AS (
    SELECT DISTINCT
        CAST(llsid AS BIGINT) AS llsid,
        id
    FROM ks_ad_antispam.ks_ad_clue_crm_stat_df
    WHERE p_date = '20260317'
      AND llsid IS NOT NULL
),

uv AS (
    SELECT DISTINCT llsid, visitor_id
    FROM ks_ad_dw.dwd_ad_tfc_log_full_hi
    WHERE p_date BETWEEN '20260302' AND '20260315'
      AND data_part = 'DSP'
      AND medium_attribute IN (1, 2)
      AND campaign_type = 'KWAI_PROMOTION_CONSULTATION'
      AND is_for_report_engine = true
),

uv_metrics AS (
    SELECT
        uv.visitor_id,
        CAST(COUNT(DISTINCT clue.id) AS DOUBLE)                                                                                   AS clue_cnt,
        CAST(COUNT(DISTINCT CASE WHEN is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)                                            AS build_clue_cnt,
        CAST(COUNT(DISTINCT CASE WHEN is_call_0d = 1 AND is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)                         AS call_cnt,
        CAST(COUNT(DISTINCT CASE WHEN is_call_connect_0d = 1 AND is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)                 AS connect_cnt,
        CAST(COUNT(DISTINCT CASE WHEN call_time_duration >= 30 AND is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)               AS connect_30s_cnt,
        CAST(COUNT(DISTINCT CASE WHEN is_valid_clue = 1 AND is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)                      AS valid_clue_cnt,
        CAST(COUNT(DISTINCT CASE WHEN is_valid_clue = 1 AND clue_convert_stat = 4 AND is_repeat_clue = 0 THEN clue.id END) AS DOUBLE) AS valid_clue_deal_cnt,
        CAST(COUNT(DISTINCT CASE WHEN clue_convert_stat = 1 AND is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)                  AS phone_connect_cnt,
        CAST(COUNT(DISTINCT CASE WHEN clue_convert_stat = 2 AND is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)                  AS intention_cnt,
        CAST(COUNT(DISTINCT CASE WHEN clue_convert_stat = 4 AND is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)                  AS deal_cnt,
        CAST(COUNT(DISTINCT CASE WHEN negtive = 1 AND is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)                            AS negative_cnt
    FROM clue
    LEFT JOIN link ON clue.id = link.id
    LEFT JOIN uv   ON link.llsid = uv.llsid
    WHERE uv.visitor_id IS NOT NULL
    GROUP BY uv.visitor_id
)

SELECT
    -- 覆盖UV
    COUNT(*)                                                                                           AS total_uv,
    COUNT(IF(clue_cnt          > 0, 1, NULL))                                                          AS uv_with_clue,
    COUNT(IF(build_clue_cnt    > 0, 1, NULL))                                                          AS uv_with_build_clue,
    COUNT(IF(call_cnt          > 0, 1, NULL))                                                          AS uv_with_call,
    COUNT(IF(connect_cnt       > 0, 1, NULL))                                                          AS uv_with_connect,
    COUNT(IF(valid_clue_cnt    > 0, 1, NULL))                                                          AS uv_with_valid_clue,
    COUNT(IF(deal_cnt          > 0, 1, NULL))                                                          AS uv_with_deal,

    -- clue_cnt 总线索数
    ROUND(AVG(IF(clue_cnt > 0, clue_cnt, NULL)), 4)                                                    AS clue_cnt_avg,
    ROUND(MAX(clue_cnt), 0)                                                                            AS clue_cnt_max,
    ROUND(percentile_approx(IF(clue_cnt > 0, clue_cnt, NULL), 0.50), 2)                               AS clue_cnt_p50,
    ROUND(percentile_approx(IF(clue_cnt > 0, clue_cnt, NULL), 0.90), 2)                               AS clue_cnt_p90,
    ROUND(percentile_approx(IF(clue_cnt > 0, clue_cnt, NULL), 0.99), 2)                               AS clue_cnt_p99,

    -- build_clue_cnt 建站线索数
    ROUND(AVG(IF(build_clue_cnt > 0, build_clue_cnt, NULL)), 4)                                        AS build_clue_avg,
    ROUND(percentile_approx(IF(build_clue_cnt > 0, build_clue_cnt, NULL), 0.50), 2)                   AS build_clue_p50,
    ROUND(percentile_approx(IF(build_clue_cnt > 0, build_clue_cnt, NULL), 0.90), 2)                   AS build_clue_p90,

    -- call_cnt 建站拨打量
    ROUND(AVG(IF(call_cnt > 0, call_cnt, NULL)), 4)                                                    AS call_cnt_avg,
    ROUND(percentile_approx(IF(call_cnt > 0, call_cnt, NULL), 0.50), 2)                               AS call_cnt_p50,
    ROUND(percentile_approx(IF(call_cnt > 0, call_cnt, NULL), 0.90), 2)                               AS call_cnt_p90,

    -- connect_cnt 建站接通数
    ROUND(AVG(IF(connect_cnt > 0, connect_cnt, NULL)), 4)                                              AS connect_cnt_avg,
    ROUND(percentile_approx(IF(connect_cnt > 0, connect_cnt, NULL), 0.50), 2)                         AS connect_cnt_p50,
    ROUND(percentile_approx(IF(connect_cnt > 0, connect_cnt, NULL), 0.90), 2)                         AS connect_cnt_p90,

    -- connect_rate 建站接通率（仅有拨打的UV）
    ROUND(AVG(IF(call_cnt > 0, connect_cnt / call_cnt, NULL)), 4)                                      AS connect_rate_avg,
    ROUND(percentile_approx(IF(call_cnt > 0, connect_cnt / call_cnt, NULL), 0.50), 4)                 AS connect_rate_p50,
    ROUND(percentile_approx(IF(call_cnt > 0, connect_cnt / call_cnt, NULL), 0.90), 4)                 AS connect_rate_p90,

    -- connect_30s_cnt 30s接通数
    ROUND(AVG(IF(connect_30s_cnt > 0, connect_30s_cnt, NULL)), 4)                                      AS connect_30s_avg,
    ROUND(percentile_approx(IF(connect_30s_cnt > 0, connect_30s_cnt, NULL), 0.50), 2)                 AS connect_30s_p50,

    -- connect_30s_rate 30s接通率
    ROUND(AVG(IF(call_cnt > 0, connect_30s_cnt / call_cnt, NULL)), 4)                                  AS connect_30s_rate_avg,
    ROUND(percentile_approx(IF(call_cnt > 0, connect_30s_cnt / call_cnt, NULL), 0.50), 4)             AS connect_30s_rate_p50,

    -- valid_clue_cnt 有效线索数
    ROUND(AVG(IF(valid_clue_cnt > 0, valid_clue_cnt, NULL)), 4)                                        AS valid_clue_avg,
    ROUND(percentile_approx(IF(valid_clue_cnt > 0, valid_clue_cnt, NULL), 0.50), 2)                   AS valid_clue_p50,
    ROUND(percentile_approx(IF(valid_clue_cnt > 0, valid_clue_cnt, NULL), 0.90), 2)                   AS valid_clue_p90,

    -- deal_cnt 成交数
    ROUND(AVG(IF(deal_cnt > 0, deal_cnt, NULL)), 4)                                                    AS deal_cnt_avg,
    ROUND(percentile_approx(IF(deal_cnt > 0, deal_cnt, NULL), 0.50), 2)                               AS deal_cnt_p50,
    ROUND(percentile_approx(IF(deal_cnt > 0, deal_cnt, NULL), 0.90), 2)                               AS deal_cnt_p90,

    -- valid_clue_deal_cnt 有效线索成交数
    ROUND(AVG(IF(valid_clue_deal_cnt > 0, valid_clue_deal_cnt, NULL)), 4)                              AS valid_deal_avg,
    ROUND(percentile_approx(IF(valid_clue_deal_cnt > 0, valid_clue_deal_cnt, NULL), 0.50), 2)         AS valid_deal_p50,

    -- deal_rate 线索成交率（仅有建站线索的UV）
    ROUND(AVG(IF(build_clue_cnt > 0, deal_cnt / build_clue_cnt, NULL)), 4)                             AS deal_rate_avg,
    ROUND(percentile_approx(IF(build_clue_cnt > 0, deal_cnt / build_clue_cnt, NULL), 0.50), 4)        AS deal_rate_p50,
    ROUND(percentile_approx(IF(build_clue_cnt > 0, deal_cnt / build_clue_cnt, NULL), 0.90), 4)        AS deal_rate_p90,

    -- negative_cnt 负向线索数
    ROUND(AVG(IF(negative_cnt > 0, negative_cnt, NULL)), 4)                                            AS negative_avg,
    ROUND(percentile_approx(IF(negative_cnt > 0, negative_cnt, NULL), 0.50), 2)                       AS negative_p50,
    ROUND(percentile_approx(IF(negative_cnt > 0, negative_cnt, NULL), 0.90), 2)                       AS negative_p90

FROM uv_metrics
;
