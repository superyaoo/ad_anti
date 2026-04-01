-- ============================================================
-- 后效大盘分布分析 —— 线索类后效（Hive 版）
-- 主表：ks_ad_dw.app_crm_clue_submit_back_link_data_all
-- 辅助：dim_ad_crm_clue_df / ks_ad_clue_crm_stat_df / dwd_ad_tfc_log_full_hi
-- 分箱策略：Bin1=(0,p5] | Bin2~9=p5~p95等距8箱 | Bin10=(p95,+∞)
-- 维度：visitor_id
-- ============================================================

WITH

clue AS (
    SELECT t1.*
    FROM (
        SELECT
            DISTINCT create_time, p_date, account_id, resource_type,
            unit_id, pos_id, id, gap_create_call_time, clue_biz_stat,
            is_call_stat_negative, is_call_stat_connect, clue_convert_stat,
            is_tag_intention_negative, is_call_0d, is_repeat_clue, is_call_connect_0d,
            call_time_duration, is_valid_clue,
            CASE
                WHEN clue_biz_stat = 3
                  OR is_call_stat_negative = 1
                  OR clue_convert_stat IN (8, 10)
                  OR is_tag_intention_negative = 1 THEN 1
                ELSE 0
            END AS negtive,
            ori_clue_id
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
        1                                                                                                                          AS join_key,
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
        CAST(COUNT(DISTINCT CASE WHEN negtive = 1 AND is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)                            AS negative_cnt,
        -- 比率类在 uv_derived 层计算，此处先存分子分母
        CAST(COUNT(DISTINCT CASE WHEN is_call_0d = 1 AND is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)                         AS call_cnt_denom,
        CAST(COUNT(DISTINCT CASE WHEN is_repeat_clue = 0 THEN clue.id END) AS DOUBLE)                                            AS build_clue_cnt_denom
    FROM clue
    LEFT JOIN link ON clue.id = link.id
    LEFT JOIN uv   ON link.llsid = uv.llsid
    WHERE uv.visitor_id IS NOT NULL
    GROUP BY uv.visitor_id
),

uv_derived AS (
    SELECT
        join_key,
        visitor_id,
        clue_cnt, build_clue_cnt, call_cnt, connect_cnt, connect_30s_cnt,
        valid_clue_cnt, valid_clue_deal_cnt, phone_connect_cnt, intention_cnt,
        deal_cnt, negative_cnt,
        IF(call_cnt_denom > 0,        connect_cnt    / call_cnt_denom,        NULL) AS connect_rate,
        IF(call_cnt_denom > 0,        connect_30s_cnt / call_cnt_denom,       NULL) AS connect_30s_rate,
        IF(build_clue_cnt_denom > 0,  deal_cnt        / build_clue_cnt_denom, NULL) AS deal_rate
    FROM uv_metrics
),

percentiles AS (
    SELECT
        1                                                                                                   AS join_key,
        percentile_approx(IF(clue_cnt          > 0, clue_cnt,          NULL), 0.05) AS clue_cnt_p5,
        percentile_approx(IF(clue_cnt          > 0, clue_cnt,          NULL), 0.95) AS clue_cnt_p95,
        percentile_approx(IF(build_clue_cnt    > 0, build_clue_cnt,    NULL), 0.05) AS build_clue_p5,
        percentile_approx(IF(build_clue_cnt    > 0, build_clue_cnt,    NULL), 0.95) AS build_clue_p95,
        percentile_approx(IF(call_cnt          > 0, call_cnt,          NULL), 0.05) AS call_p5,
        percentile_approx(IF(call_cnt          > 0, call_cnt,          NULL), 0.95) AS call_p95,
        percentile_approx(IF(connect_cnt       > 0, connect_cnt,       NULL), 0.05) AS connect_p5,
        percentile_approx(IF(connect_cnt       > 0, connect_cnt,       NULL), 0.95) AS connect_p95,
        percentile_approx(IF(connect_30s_cnt   > 0, connect_30s_cnt,   NULL), 0.05) AS connect_30s_p5,
        percentile_approx(IF(connect_30s_cnt   > 0, connect_30s_cnt,   NULL), 0.95) AS connect_30s_p95,
        percentile_approx(IF(valid_clue_cnt    > 0, valid_clue_cnt,    NULL), 0.05) AS valid_clue_p5,
        percentile_approx(IF(valid_clue_cnt    > 0, valid_clue_cnt,    NULL), 0.95) AS valid_clue_p95,
        percentile_approx(IF(valid_clue_deal_cnt > 0, valid_clue_deal_cnt, NULL), 0.05) AS valid_deal_p5,
        percentile_approx(IF(valid_clue_deal_cnt > 0, valid_clue_deal_cnt, NULL), 0.95) AS valid_deal_p95,
        percentile_approx(IF(phone_connect_cnt > 0, phone_connect_cnt, NULL), 0.05) AS phone_p5,
        percentile_approx(IF(phone_connect_cnt > 0, phone_connect_cnt, NULL), 0.95) AS phone_p95,
        percentile_approx(IF(intention_cnt     > 0, intention_cnt,     NULL), 0.05) AS intention_p5,
        percentile_approx(IF(intention_cnt     > 0, intention_cnt,     NULL), 0.95) AS intention_p95,
        percentile_approx(IF(deal_cnt          > 0, deal_cnt,          NULL), 0.05) AS deal_p5,
        percentile_approx(IF(deal_cnt          > 0, deal_cnt,          NULL), 0.95) AS deal_p95,
        percentile_approx(IF(negative_cnt      > 0, negative_cnt,      NULL), 0.05) AS negative_p5,
        percentile_approx(IF(negative_cnt      > 0, negative_cnt,      NULL), 0.95) AS negative_p95,
        percentile_approx(IF(connect_rate      > 0, connect_rate,      NULL), 0.05) AS connect_rate_p5,
        percentile_approx(IF(connect_rate      > 0, connect_rate,      NULL), 0.95) AS connect_rate_p95,
        percentile_approx(IF(connect_30s_rate  > 0, connect_30s_rate,  NULL), 0.05) AS connect_30s_rate_p5,
        percentile_approx(IF(connect_30s_rate  > 0, connect_30s_rate,  NULL), 0.95) AS connect_30s_rate_p95,
        percentile_approx(IF(deal_rate         > 0, deal_rate,         NULL), 0.05) AS deal_rate_p5,
        percentile_approx(IF(deal_rate         > 0, deal_rate,         NULL), 0.95) AS deal_rate_p95
    FROM uv_derived
),

binned AS (
    -- 1. clue_cnt
    SELECT 'clue_cnt' AS metric, visitor_id,
        CASE
            WHEN clue_cnt <= 0 OR clue_cnt IS NULL THEN NULL
            WHEN clue_cnt <= p.clue_cnt_p5                                                  THEN CONCAT('Bin01_(0,',ROUND(p.clue_cnt_p5,2),']')
            WHEN clue_cnt <= p.clue_cnt_p5 + 1*(p.clue_cnt_p95-p.clue_cnt_p5)/8            THEN CONCAT('Bin02_(',ROUND(p.clue_cnt_p5,2),',',ROUND(p.clue_cnt_p5+1*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),']')
            WHEN clue_cnt <= p.clue_cnt_p5 + 2*(p.clue_cnt_p95-p.clue_cnt_p5)/8            THEN CONCAT('Bin03_(',ROUND(p.clue_cnt_p5+1*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),',',ROUND(p.clue_cnt_p5+2*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),']')
            WHEN clue_cnt <= p.clue_cnt_p5 + 3*(p.clue_cnt_p95-p.clue_cnt_p5)/8            THEN CONCAT('Bin04_(',ROUND(p.clue_cnt_p5+2*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),',',ROUND(p.clue_cnt_p5+3*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),']')
            WHEN clue_cnt <= p.clue_cnt_p5 + 4*(p.clue_cnt_p95-p.clue_cnt_p5)/8            THEN CONCAT('Bin05_(',ROUND(p.clue_cnt_p5+3*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),',',ROUND(p.clue_cnt_p5+4*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),']')
            WHEN clue_cnt <= p.clue_cnt_p5 + 5*(p.clue_cnt_p95-p.clue_cnt_p5)/8            THEN CONCAT('Bin06_(',ROUND(p.clue_cnt_p5+4*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),',',ROUND(p.clue_cnt_p5+5*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),']')
            WHEN clue_cnt <= p.clue_cnt_p5 + 6*(p.clue_cnt_p95-p.clue_cnt_p5)/8            THEN CONCAT('Bin07_(',ROUND(p.clue_cnt_p5+5*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),',',ROUND(p.clue_cnt_p5+6*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),']')
            WHEN clue_cnt <= p.clue_cnt_p5 + 7*(p.clue_cnt_p95-p.clue_cnt_p5)/8            THEN CONCAT('Bin08_(',ROUND(p.clue_cnt_p5+6*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),',',ROUND(p.clue_cnt_p5+7*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),']')
            WHEN clue_cnt <= p.clue_cnt_p95                                                 THEN CONCAT('Bin09_(',ROUND(p.clue_cnt_p5+7*(p.clue_cnt_p95-p.clue_cnt_p5)/8,2),',',ROUND(p.clue_cnt_p95,2),']')
            ELSE                                                                                  CONCAT('Bin10_(',ROUND(p.clue_cnt_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 2. build_clue_cnt
    SELECT 'build_clue_cnt' AS metric, visitor_id,
        CASE
            WHEN build_clue_cnt <= 0 OR build_clue_cnt IS NULL THEN NULL
            WHEN build_clue_cnt <= p.build_clue_p5                                                              THEN CONCAT('Bin01_(0,',ROUND(p.build_clue_p5,2),']')
            WHEN build_clue_cnt <= p.build_clue_p5 + 1*(p.build_clue_p95-p.build_clue_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.build_clue_p5,2),',',ROUND(p.build_clue_p5+1*(p.build_clue_p95-p.build_clue_p5)/8,2),']')
            WHEN build_clue_cnt <= p.build_clue_p5 + 2*(p.build_clue_p95-p.build_clue_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.build_clue_p5+1*(p.build_clue_p95-p.build_clue_p5)/8,2),',',ROUND(p.build_clue_p5+2*(p.build_clue_p95-p.build_clue_p5)/8,2),']')
            WHEN build_clue_cnt <= p.build_clue_p5 + 3*(p.build_clue_p95-p.build_clue_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.build_clue_p5+2*(p.build_clue_p95-p.build_clue_p5)/8,2),',',ROUND(p.build_clue_p5+3*(p.build_clue_p95-p.build_clue_p5)/8,2),']')
            WHEN build_clue_cnt <= p.build_clue_p5 + 4*(p.build_clue_p95-p.build_clue_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.build_clue_p5+3*(p.build_clue_p95-p.build_clue_p5)/8,2),',',ROUND(p.build_clue_p5+4*(p.build_clue_p95-p.build_clue_p5)/8,2),']')
            WHEN build_clue_cnt <= p.build_clue_p5 + 5*(p.build_clue_p95-p.build_clue_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.build_clue_p5+4*(p.build_clue_p95-p.build_clue_p5)/8,2),',',ROUND(p.build_clue_p5+5*(p.build_clue_p95-p.build_clue_p5)/8,2),']')
            WHEN build_clue_cnt <= p.build_clue_p5 + 6*(p.build_clue_p95-p.build_clue_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.build_clue_p5+5*(p.build_clue_p95-p.build_clue_p5)/8,2),',',ROUND(p.build_clue_p5+6*(p.build_clue_p95-p.build_clue_p5)/8,2),']')
            WHEN build_clue_cnt <= p.build_clue_p5 + 7*(p.build_clue_p95-p.build_clue_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.build_clue_p5+6*(p.build_clue_p95-p.build_clue_p5)/8,2),',',ROUND(p.build_clue_p5+7*(p.build_clue_p95-p.build_clue_p5)/8,2),']')
            WHEN build_clue_cnt <= p.build_clue_p95                                                             THEN CONCAT('Bin09_(',ROUND(p.build_clue_p5+7*(p.build_clue_p95-p.build_clue_p5)/8,2),',',ROUND(p.build_clue_p95,2),']')
            ELSE                                                                                                      CONCAT('Bin10_(',ROUND(p.build_clue_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 3. call_cnt
    SELECT 'call_cnt' AS metric, visitor_id,
        CASE
            WHEN call_cnt <= 0 OR call_cnt IS NULL THEN NULL
            WHEN call_cnt <= p.call_p5                                          THEN CONCAT('Bin01_(0,',ROUND(p.call_p5,2),']')
            WHEN call_cnt <= p.call_p5 + 1*(p.call_p95-p.call_p5)/8            THEN CONCAT('Bin02_(',ROUND(p.call_p5,2),',',ROUND(p.call_p5+1*(p.call_p95-p.call_p5)/8,2),']')
            WHEN call_cnt <= p.call_p5 + 2*(p.call_p95-p.call_p5)/8            THEN CONCAT('Bin03_(',ROUND(p.call_p5+1*(p.call_p95-p.call_p5)/8,2),',',ROUND(p.call_p5+2*(p.call_p95-p.call_p5)/8,2),']')
            WHEN call_cnt <= p.call_p5 + 3*(p.call_p95-p.call_p5)/8            THEN CONCAT('Bin04_(',ROUND(p.call_p5+2*(p.call_p95-p.call_p5)/8,2),',',ROUND(p.call_p5+3*(p.call_p95-p.call_p5)/8,2),']')
            WHEN call_cnt <= p.call_p5 + 4*(p.call_p95-p.call_p5)/8            THEN CONCAT('Bin05_(',ROUND(p.call_p5+3*(p.call_p95-p.call_p5)/8,2),',',ROUND(p.call_p5+4*(p.call_p95-p.call_p5)/8,2),']')
            WHEN call_cnt <= p.call_p5 + 5*(p.call_p95-p.call_p5)/8            THEN CONCAT('Bin06_(',ROUND(p.call_p5+4*(p.call_p95-p.call_p5)/8,2),',',ROUND(p.call_p5+5*(p.call_p95-p.call_p5)/8,2),']')
            WHEN call_cnt <= p.call_p5 + 6*(p.call_p95-p.call_p5)/8            THEN CONCAT('Bin07_(',ROUND(p.call_p5+5*(p.call_p95-p.call_p5)/8,2),',',ROUND(p.call_p5+6*(p.call_p95-p.call_p5)/8,2),']')
            WHEN call_cnt <= p.call_p5 + 7*(p.call_p95-p.call_p5)/8            THEN CONCAT('Bin08_(',ROUND(p.call_p5+6*(p.call_p95-p.call_p5)/8,2),',',ROUND(p.call_p5+7*(p.call_p95-p.call_p5)/8,2),']')
            WHEN call_cnt <= p.call_p95                                         THEN CONCAT('Bin09_(',ROUND(p.call_p5+7*(p.call_p95-p.call_p5)/8,2),',',ROUND(p.call_p95,2),']')
            ELSE                                                                     CONCAT('Bin10_(',ROUND(p.call_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 4. connect_cnt
    SELECT 'connect_cnt' AS metric, visitor_id,
        CASE
            WHEN connect_cnt <= 0 OR connect_cnt IS NULL THEN NULL
            WHEN connect_cnt <= p.connect_p5                                                    THEN CONCAT('Bin01_(0,',ROUND(p.connect_p5,2),']')
            WHEN connect_cnt <= p.connect_p5 + 1*(p.connect_p95-p.connect_p5)/8                THEN CONCAT('Bin02_(',ROUND(p.connect_p5,2),',',ROUND(p.connect_p5+1*(p.connect_p95-p.connect_p5)/8,2),']')
            WHEN connect_cnt <= p.connect_p5 + 2*(p.connect_p95-p.connect_p5)/8                THEN CONCAT('Bin03_(',ROUND(p.connect_p5+1*(p.connect_p95-p.connect_p5)/8,2),',',ROUND(p.connect_p5+2*(p.connect_p95-p.connect_p5)/8,2),']')
            WHEN connect_cnt <= p.connect_p5 + 3*(p.connect_p95-p.connect_p5)/8                THEN CONCAT('Bin04_(',ROUND(p.connect_p5+2*(p.connect_p95-p.connect_p5)/8,2),',',ROUND(p.connect_p5+3*(p.connect_p95-p.connect_p5)/8,2),']')
            WHEN connect_cnt <= p.connect_p5 + 4*(p.connect_p95-p.connect_p5)/8                THEN CONCAT('Bin05_(',ROUND(p.connect_p5+3*(p.connect_p95-p.connect_p5)/8,2),',',ROUND(p.connect_p5+4*(p.connect_p95-p.connect_p5)/8,2),']')
            WHEN connect_cnt <= p.connect_p5 + 5*(p.connect_p95-p.connect_p5)/8                THEN CONCAT('Bin06_(',ROUND(p.connect_p5+4*(p.connect_p95-p.connect_p5)/8,2),',',ROUND(p.connect_p5+5*(p.connect_p95-p.connect_p5)/8,2),']')
            WHEN connect_cnt <= p.connect_p5 + 6*(p.connect_p95-p.connect_p5)/8                THEN CONCAT('Bin07_(',ROUND(p.connect_p5+5*(p.connect_p95-p.connect_p5)/8,2),',',ROUND(p.connect_p5+6*(p.connect_p95-p.connect_p5)/8,2),']')
            WHEN connect_cnt <= p.connect_p5 + 7*(p.connect_p95-p.connect_p5)/8                THEN CONCAT('Bin08_(',ROUND(p.connect_p5+6*(p.connect_p95-p.connect_p5)/8,2),',',ROUND(p.connect_p5+7*(p.connect_p95-p.connect_p5)/8,2),']')
            WHEN connect_cnt <= p.connect_p95                                                   THEN CONCAT('Bin09_(',ROUND(p.connect_p5+7*(p.connect_p95-p.connect_p5)/8,2),',',ROUND(p.connect_p95,2),']')
            ELSE                                                                                     CONCAT('Bin10_(',ROUND(p.connect_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 5. connect_30s_cnt
    SELECT 'connect_30s_cnt' AS metric, visitor_id,
        CASE
            WHEN connect_30s_cnt <= 0 OR connect_30s_cnt IS NULL THEN NULL
            WHEN connect_30s_cnt <= p.connect_30s_p5                                                            THEN CONCAT('Bin01_(0,',ROUND(p.connect_30s_p5,2),']')
            WHEN connect_30s_cnt <= p.connect_30s_p5 + 1*(p.connect_30s_p95-p.connect_30s_p5)/8                THEN CONCAT('Bin02_(',ROUND(p.connect_30s_p5,2),',',ROUND(p.connect_30s_p5+1*(p.connect_30s_p95-p.connect_30s_p5)/8,2),']')
            WHEN connect_30s_cnt <= p.connect_30s_p5 + 2*(p.connect_30s_p95-p.connect_30s_p5)/8                THEN CONCAT('Bin03_(',ROUND(p.connect_30s_p5+1*(p.connect_30s_p95-p.connect_30s_p5)/8,2),',',ROUND(p.connect_30s_p5+2*(p.connect_30s_p95-p.connect_30s_p5)/8,2),']')
            WHEN connect_30s_cnt <= p.connect_30s_p5 + 3*(p.connect_30s_p95-p.connect_30s_p5)/8                THEN CONCAT('Bin04_(',ROUND(p.connect_30s_p5+2*(p.connect_30s_p95-p.connect_30s_p5)/8,2),',',ROUND(p.connect_30s_p5+3*(p.connect_30s_p95-p.connect_30s_p5)/8,2),']')
            WHEN connect_30s_cnt <= p.connect_30s_p5 + 4*(p.connect_30s_p95-p.connect_30s_p5)/8                THEN CONCAT('Bin05_(',ROUND(p.connect_30s_p5+3*(p.connect_30s_p95-p.connect_30s_p5)/8,2),',',ROUND(p.connect_30s_p5+4*(p.connect_30s_p95-p.connect_30s_p5)/8,2),']')
            WHEN connect_30s_cnt <= p.connect_30s_p5 + 5*(p.connect_30s_p95-p.connect_30s_p5)/8                THEN CONCAT('Bin06_(',ROUND(p.connect_30s_p5+4*(p.connect_30s_p95-p.connect_30s_p5)/8,2),',',ROUND(p.connect_30s_p5+5*(p.connect_30s_p95-p.connect_30s_p5)/8,2),']')
            WHEN connect_30s_cnt <= p.connect_30s_p5 + 6*(p.connect_30s_p95-p.connect_30s_p5)/8                THEN CONCAT('Bin07_(',ROUND(p.connect_30s_p5+5*(p.connect_30s_p95-p.connect_30s_p5)/8,2),',',ROUND(p.connect_30s_p5+6*(p.connect_30s_p95-p.connect_30s_p5)/8,2),']')
            WHEN connect_30s_cnt <= p.connect_30s_p5 + 7*(p.connect_30s_p95-p.connect_30s_p5)/8                THEN CONCAT('Bin08_(',ROUND(p.connect_30s_p5+6*(p.connect_30s_p95-p.connect_30s_p5)/8,2),',',ROUND(p.connect_30s_p5+7*(p.connect_30s_p95-p.connect_30s_p5)/8,2),']')
            WHEN connect_30s_cnt <= p.connect_30s_p95                                                           THEN CONCAT('Bin09_(',ROUND(p.connect_30s_p5+7*(p.connect_30s_p95-p.connect_30s_p5)/8,2),',',ROUND(p.connect_30s_p95,2),']')
            ELSE                                                                                                      CONCAT('Bin10_(',ROUND(p.connect_30s_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 6. valid_clue_cnt
    SELECT 'valid_clue_cnt' AS metric, visitor_id,
        CASE
            WHEN valid_clue_cnt <= 0 OR valid_clue_cnt IS NULL THEN NULL
            WHEN valid_clue_cnt <= p.valid_clue_p5                                                              THEN CONCAT('Bin01_(0,',ROUND(p.valid_clue_p5,2),']')
            WHEN valid_clue_cnt <= p.valid_clue_p5 + 1*(p.valid_clue_p95-p.valid_clue_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.valid_clue_p5,2),',',ROUND(p.valid_clue_p5+1*(p.valid_clue_p95-p.valid_clue_p5)/8,2),']')
            WHEN valid_clue_cnt <= p.valid_clue_p5 + 2*(p.valid_clue_p95-p.valid_clue_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.valid_clue_p5+1*(p.valid_clue_p95-p.valid_clue_p5)/8,2),',',ROUND(p.valid_clue_p5+2*(p.valid_clue_p95-p.valid_clue_p5)/8,2),']')
            WHEN valid_clue_cnt <= p.valid_clue_p5 + 3*(p.valid_clue_p95-p.valid_clue_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.valid_clue_p5+2*(p.valid_clue_p95-p.valid_clue_p5)/8,2),',',ROUND(p.valid_clue_p5+3*(p.valid_clue_p95-p.valid_clue_p5)/8,2),']')
            WHEN valid_clue_cnt <= p.valid_clue_p5 + 4*(p.valid_clue_p95-p.valid_clue_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.valid_clue_p5+3*(p.valid_clue_p95-p.valid_clue_p5)/8,2),',',ROUND(p.valid_clue_p5+4*(p.valid_clue_p95-p.valid_clue_p5)/8,2),']')
            WHEN valid_clue_cnt <= p.valid_clue_p5 + 5*(p.valid_clue_p95-p.valid_clue_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.valid_clue_p5+4*(p.valid_clue_p95-p.valid_clue_p5)/8,2),',',ROUND(p.valid_clue_p5+5*(p.valid_clue_p95-p.valid_clue_p5)/8,2),']')
            WHEN valid_clue_cnt <= p.valid_clue_p5 + 6*(p.valid_clue_p95-p.valid_clue_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.valid_clue_p5+5*(p.valid_clue_p95-p.valid_clue_p5)/8,2),',',ROUND(p.valid_clue_p5+6*(p.valid_clue_p95-p.valid_clue_p5)/8,2),']')
            WHEN valid_clue_cnt <= p.valid_clue_p5 + 7*(p.valid_clue_p95-p.valid_clue_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.valid_clue_p5+6*(p.valid_clue_p95-p.valid_clue_p5)/8,2),',',ROUND(p.valid_clue_p5+7*(p.valid_clue_p95-p.valid_clue_p5)/8,2),']')
            WHEN valid_clue_cnt <= p.valid_clue_p95                                                             THEN CONCAT('Bin09_(',ROUND(p.valid_clue_p5+7*(p.valid_clue_p95-p.valid_clue_p5)/8,2),',',ROUND(p.valid_clue_p95,2),']')
            ELSE                                                                                                      CONCAT('Bin10_(',ROUND(p.valid_clue_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 7. valid_clue_deal_cnt
    SELECT 'valid_clue_deal_cnt' AS metric, visitor_id,
        CASE
            WHEN valid_clue_deal_cnt <= 0 OR valid_clue_deal_cnt IS NULL THEN NULL
            WHEN valid_clue_deal_cnt <= p.valid_deal_p5                                                                 THEN CONCAT('Bin01_(0,',ROUND(p.valid_deal_p5,2),']')
            WHEN valid_clue_deal_cnt <= p.valid_deal_p5 + 1*(p.valid_deal_p95-p.valid_deal_p5)/8                       THEN CONCAT('Bin02_(',ROUND(p.valid_deal_p5,2),',',ROUND(p.valid_deal_p5+1*(p.valid_deal_p95-p.valid_deal_p5)/8,2),']')
            WHEN valid_clue_deal_cnt <= p.valid_deal_p5 + 2*(p.valid_deal_p95-p.valid_deal_p5)/8                       THEN CONCAT('Bin03_(',ROUND(p.valid_deal_p5+1*(p.valid_deal_p95-p.valid_deal_p5)/8,2),',',ROUND(p.valid_deal_p5+2*(p.valid_deal_p95-p.valid_deal_p5)/8,2),']')
            WHEN valid_clue_deal_cnt <= p.valid_deal_p5 + 3*(p.valid_deal_p95-p.valid_deal_p5)/8                       THEN CONCAT('Bin04_(',ROUND(p.valid_deal_p5+2*(p.valid_deal_p95-p.valid_deal_p5)/8,2),',',ROUND(p.valid_deal_p5+3*(p.valid_deal_p95-p.valid_deal_p5)/8,2),']')
            WHEN valid_clue_deal_cnt <= p.valid_deal_p5 + 4*(p.valid_deal_p95-p.valid_deal_p5)/8                       THEN CONCAT('Bin05_(',ROUND(p.valid_deal_p5+3*(p.valid_deal_p95-p.valid_deal_p5)/8,2),',',ROUND(p.valid_deal_p5+4*(p.valid_deal_p95-p.valid_deal_p5)/8,2),']')
            WHEN valid_clue_deal_cnt <= p.valid_deal_p5 + 5*(p.valid_deal_p95-p.valid_deal_p5)/8                       THEN CONCAT('Bin06_(',ROUND(p.valid_deal_p5+4*(p.valid_deal_p95-p.valid_deal_p5)/8,2),',',ROUND(p.valid_deal_p5+5*(p.valid_deal_p95-p.valid_deal_p5)/8,2),']')
            WHEN valid_clue_deal_cnt <= p.valid_deal_p5 + 6*(p.valid_deal_p95-p.valid_deal_p5)/8                       THEN CONCAT('Bin07_(',ROUND(p.valid_deal_p5+5*(p.valid_deal_p95-p.valid_deal_p5)/8,2),',',ROUND(p.valid_deal_p5+6*(p.valid_deal_p95-p.valid_deal_p5)/8,2),']')
            WHEN valid_clue_deal_cnt <= p.valid_deal_p5 + 7*(p.valid_deal_p95-p.valid_deal_p5)/8                       THEN CONCAT('Bin08_(',ROUND(p.valid_deal_p5+6*(p.valid_deal_p95-p.valid_deal_p5)/8,2),',',ROUND(p.valid_deal_p5+7*(p.valid_deal_p95-p.valid_deal_p5)/8,2),']')
            WHEN valid_clue_deal_cnt <= p.valid_deal_p95                                                                THEN CONCAT('Bin09_(',ROUND(p.valid_deal_p5+7*(p.valid_deal_p95-p.valid_deal_p5)/8,2),',',ROUND(p.valid_deal_p95,2),']')
            ELSE                                                                                                             CONCAT('Bin10_(',ROUND(p.valid_deal_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 8. phone_connect_cnt
    SELECT 'phone_connect_cnt' AS metric, visitor_id,
        CASE
            WHEN phone_connect_cnt <= 0 OR phone_connect_cnt IS NULL THEN NULL
            WHEN phone_connect_cnt <= p.phone_p5                                                                THEN CONCAT('Bin01_(0,',ROUND(p.phone_p5,2),']')
            WHEN phone_connect_cnt <= p.phone_p5 + 1*(p.phone_p95-p.phone_p5)/8                                THEN CONCAT('Bin02_(',ROUND(p.phone_p5,2),',',ROUND(p.phone_p5+1*(p.phone_p95-p.phone_p5)/8,2),']')
            WHEN phone_connect_cnt <= p.phone_p5 + 2*(p.phone_p95-p.phone_p5)/8                                THEN CONCAT('Bin03_(',ROUND(p.phone_p5+1*(p.phone_p95-p.phone_p5)/8,2),',',ROUND(p.phone_p5+2*(p.phone_p95-p.phone_p5)/8,2),']')
            WHEN phone_connect_cnt <= p.phone_p5 + 3*(p.phone_p95-p.phone_p5)/8                                THEN CONCAT('Bin04_(',ROUND(p.phone_p5+2*(p.phone_p95-p.phone_p5)/8,2),',',ROUND(p.phone_p5+3*(p.phone_p95-p.phone_p5)/8,2),']')
            WHEN phone_connect_cnt <= p.phone_p5 + 4*(p.phone_p95-p.phone_p5)/8                                THEN CONCAT('Bin05_(',ROUND(p.phone_p5+3*(p.phone_p95-p.phone_p5)/8,2),',',ROUND(p.phone_p5+4*(p.phone_p95-p.phone_p5)/8,2),']')
            WHEN phone_connect_cnt <= p.phone_p5 + 5*(p.phone_p95-p.phone_p5)/8                                THEN CONCAT('Bin06_(',ROUND(p.phone_p5+4*(p.phone_p95-p.phone_p5)/8,2),',',ROUND(p.phone_p5+5*(p.phone_p95-p.phone_p5)/8,2),']')
            WHEN phone_connect_cnt <= p.phone_p5 + 6*(p.phone_p95-p.phone_p5)/8                                THEN CONCAT('Bin07_(',ROUND(p.phone_p5+5*(p.phone_p95-p.phone_p5)/8,2),',',ROUND(p.phone_p5+6*(p.phone_p95-p.phone_p5)/8,2),']')
            WHEN phone_connect_cnt <= p.phone_p5 + 7*(p.phone_p95-p.phone_p5)/8                                THEN CONCAT('Bin08_(',ROUND(p.phone_p5+6*(p.phone_p95-p.phone_p5)/8,2),',',ROUND(p.phone_p5+7*(p.phone_p95-p.phone_p5)/8,2),']')
            WHEN phone_connect_cnt <= p.phone_p95                                                               THEN CONCAT('Bin09_(',ROUND(p.phone_p5+7*(p.phone_p95-p.phone_p5)/8,2),',',ROUND(p.phone_p95,2),']')
            ELSE                                                                                                     CONCAT('Bin10_(',ROUND(p.phone_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 9. intention_cnt
    SELECT 'intention_cnt' AS metric, visitor_id,
        CASE
            WHEN intention_cnt <= 0 OR intention_cnt IS NULL THEN NULL
            WHEN intention_cnt <= p.intention_p5                                                                THEN CONCAT('Bin01_(0,',ROUND(p.intention_p5,2),']')
            WHEN intention_cnt <= p.intention_p5 + 1*(p.intention_p95-p.intention_p5)/8                        THEN CONCAT('Bin02_(',ROUND(p.intention_p5,2),',',ROUND(p.intention_p5+1*(p.intention_p95-p.intention_p5)/8,2),']')
            WHEN intention_cnt <= p.intention_p5 + 2*(p.intention_p95-p.intention_p5)/8                        THEN CONCAT('Bin03_(',ROUND(p.intention_p5+1*(p.intention_p95-p.intention_p5)/8,2),',',ROUND(p.intention_p5+2*(p.intention_p95-p.intention_p5)/8,2),']')
            WHEN intention_cnt <= p.intention_p5 + 3*(p.intention_p95-p.intention_p5)/8                        THEN CONCAT('Bin04_(',ROUND(p.intention_p5+2*(p.intention_p95-p.intention_p5)/8,2),',',ROUND(p.intention_p5+3*(p.intention_p95-p.intention_p5)/8,2),']')
            WHEN intention_cnt <= p.intention_p5 + 4*(p.intention_p95-p.intention_p5)/8                        THEN CONCAT('Bin05_(',ROUND(p.intention_p5+3*(p.intention_p95-p.intention_p5)/8,2),',',ROUND(p.intention_p5+4*(p.intention_p95-p.intention_p5)/8,2),']')
            WHEN intention_cnt <= p.intention_p5 + 5*(p.intention_p95-p.intention_p5)/8                        THEN CONCAT('Bin06_(',ROUND(p.intention_p5+4*(p.intention_p95-p.intention_p5)/8,2),',',ROUND(p.intention_p5+5*(p.intention_p95-p.intention_p5)/8,2),']')
            WHEN intention_cnt <= p.intention_p5 + 6*(p.intention_p95-p.intention_p5)/8                        THEN CONCAT('Bin07_(',ROUND(p.intention_p5+5*(p.intention_p95-p.intention_p5)/8,2),',',ROUND(p.intention_p5+6*(p.intention_p95-p.intention_p5)/8,2),']')
            WHEN intention_cnt <= p.intention_p5 + 7*(p.intention_p95-p.intention_p5)/8                        THEN CONCAT('Bin08_(',ROUND(p.intention_p5+6*(p.intention_p95-p.intention_p5)/8,2),',',ROUND(p.intention_p5+7*(p.intention_p95-p.intention_p5)/8,2),']')
            WHEN intention_cnt <= p.intention_p95                                                               THEN CONCAT('Bin09_(',ROUND(p.intention_p5+7*(p.intention_p95-p.intention_p5)/8,2),',',ROUND(p.intention_p95,2),']')
            ELSE                                                                                                     CONCAT('Bin10_(',ROUND(p.intention_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 10. deal_cnt
    SELECT 'deal_cnt' AS metric, visitor_id,
        CASE
            WHEN deal_cnt <= 0 OR deal_cnt IS NULL THEN NULL
            WHEN deal_cnt <= p.deal_p5                                          THEN CONCAT('Bin01_(0,',ROUND(p.deal_p5,2),']')
            WHEN deal_cnt <= p.deal_p5 + 1*(p.deal_p95-p.deal_p5)/8            THEN CONCAT('Bin02_(',ROUND(p.deal_p5,2),',',ROUND(p.deal_p5+1*(p.deal_p95-p.deal_p5)/8,2),']')
            WHEN deal_cnt <= p.deal_p5 + 2*(p.deal_p95-p.deal_p5)/8            THEN CONCAT('Bin03_(',ROUND(p.deal_p5+1*(p.deal_p95-p.deal_p5)/8,2),',',ROUND(p.deal_p5+2*(p.deal_p95-p.deal_p5)/8,2),']')
            WHEN deal_cnt <= p.deal_p5 + 3*(p.deal_p95-p.deal_p5)/8            THEN CONCAT('Bin04_(',ROUND(p.deal_p5+2*(p.deal_p95-p.deal_p5)/8,2),',',ROUND(p.deal_p5+3*(p.deal_p95-p.deal_p5)/8,2),']')
            WHEN deal_cnt <= p.deal_p5 + 4*(p.deal_p95-p.deal_p5)/8            THEN CONCAT('Bin05_(',ROUND(p.deal_p5+3*(p.deal_p95-p.deal_p5)/8,2),',',ROUND(p.deal_p5+4*(p.deal_p95-p.deal_p5)/8,2),']')
            WHEN deal_cnt <= p.deal_p5 + 5*(p.deal_p95-p.deal_p5)/8            THEN CONCAT('Bin06_(',ROUND(p.deal_p5+4*(p.deal_p95-p.deal_p5)/8,2),',',ROUND(p.deal_p5+5*(p.deal_p95-p.deal_p5)/8,2),']')
            WHEN deal_cnt <= p.deal_p5 + 6*(p.deal_p95-p.deal_p5)/8            THEN CONCAT('Bin07_(',ROUND(p.deal_p5+5*(p.deal_p95-p.deal_p5)/8,2),',',ROUND(p.deal_p5+6*(p.deal_p95-p.deal_p5)/8,2),']')
            WHEN deal_cnt <= p.deal_p5 + 7*(p.deal_p95-p.deal_p5)/8            THEN CONCAT('Bin08_(',ROUND(p.deal_p5+6*(p.deal_p95-p.deal_p5)/8,2),',',ROUND(p.deal_p5+7*(p.deal_p95-p.deal_p5)/8,2),']')
            WHEN deal_cnt <= p.deal_p95                                         THEN CONCAT('Bin09_(',ROUND(p.deal_p5+7*(p.deal_p95-p.deal_p5)/8,2),',',ROUND(p.deal_p95,2),']')
            ELSE                                                                     CONCAT('Bin10_(',ROUND(p.deal_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 11. negative_cnt
    SELECT 'negative_cnt' AS metric, visitor_id,
        CASE
            WHEN negative_cnt <= 0 OR negative_cnt IS NULL THEN NULL
            WHEN negative_cnt <= p.negative_p5                                                                  THEN CONCAT('Bin01_(0,',ROUND(p.negative_p5,2),']')
            WHEN negative_cnt <= p.negative_p5 + 1*(p.negative_p95-p.negative_p5)/8                            THEN CONCAT('Bin02_(',ROUND(p.negative_p5,2),',',ROUND(p.negative_p5+1*(p.negative_p95-p.negative_p5)/8,2),']')
            WHEN negative_cnt <= p.negative_p5 + 2*(p.negative_p95-p.negative_p5)/8                            THEN CONCAT('Bin03_(',ROUND(p.negative_p5+1*(p.negative_p95-p.negative_p5)/8,2),',',ROUND(p.negative_p5+2*(p.negative_p95-p.negative_p5)/8,2),']')
            WHEN negative_cnt <= p.negative_p5 + 3*(p.negative_p95-p.negative_p5)/8                            THEN CONCAT('Bin04_(',ROUND(p.negative_p5+2*(p.negative_p95-p.negative_p5)/8,2),',',ROUND(p.negative_p5+3*(p.negative_p95-p.negative_p5)/8,2),']')
            WHEN negative_cnt <= p.negative_p5 + 4*(p.negative_p95-p.negative_p5)/8                            THEN CONCAT('Bin05_(',ROUND(p.negative_p5+3*(p.negative_p95-p.negative_p5)/8,2),',',ROUND(p.negative_p5+4*(p.negative_p95-p.negative_p5)/8,2),']')
            WHEN negative_cnt <= p.negative_p5 + 5*(p.negative_p95-p.negative_p5)/8                            THEN CONCAT('Bin06_(',ROUND(p.negative_p5+4*(p.negative_p95-p.negative_p5)/8,2),',',ROUND(p.negative_p5+5*(p.negative_p95-p.negative_p5)/8,2),']')
            WHEN negative_cnt <= p.negative_p5 + 6*(p.negative_p95-p.negative_p5)/8                            THEN CONCAT('Bin07_(',ROUND(p.negative_p5+5*(p.negative_p95-p.negative_p5)/8,2),',',ROUND(p.negative_p5+6*(p.negative_p95-p.negative_p5)/8,2),']')
            WHEN negative_cnt <= p.negative_p5 + 7*(p.negative_p95-p.negative_p5)/8                            THEN CONCAT('Bin08_(',ROUND(p.negative_p5+6*(p.negative_p95-p.negative_p5)/8,2),',',ROUND(p.negative_p5+7*(p.negative_p95-p.negative_p5)/8,2),']')
            WHEN negative_cnt <= p.negative_p95                                                                 THEN CONCAT('Bin09_(',ROUND(p.negative_p5+7*(p.negative_p95-p.negative_p5)/8,2),',',ROUND(p.negative_p95,2),']')
            ELSE                                                                                                     CONCAT('Bin10_(',ROUND(p.negative_p95,2),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 12. connect_rate
    SELECT 'connect_rate' AS metric, visitor_id,
        CASE
            WHEN connect_rate <= 0 OR connect_rate IS NULL THEN NULL
            WHEN connect_rate <= p.connect_rate_p5                                                                          THEN CONCAT('Bin01_(0,',ROUND(p.connect_rate_p5,4),']')
            WHEN connect_rate <= p.connect_rate_p5 + 1*(p.connect_rate_p95-p.connect_rate_p5)/8                            THEN CONCAT('Bin02_(',ROUND(p.connect_rate_p5,4),',',ROUND(p.connect_rate_p5+1*(p.connect_rate_p95-p.connect_rate_p5)/8,4),']')
            WHEN connect_rate <= p.connect_rate_p5 + 2*(p.connect_rate_p95-p.connect_rate_p5)/8                            THEN CONCAT('Bin03_(',ROUND(p.connect_rate_p5+1*(p.connect_rate_p95-p.connect_rate_p5)/8,4),',',ROUND(p.connect_rate_p5+2*(p.connect_rate_p95-p.connect_rate_p5)/8,4),']')
            WHEN connect_rate <= p.connect_rate_p5 + 3*(p.connect_rate_p95-p.connect_rate_p5)/8                            THEN CONCAT('Bin04_(',ROUND(p.connect_rate_p5+2*(p.connect_rate_p95-p.connect_rate_p5)/8,4),',',ROUND(p.connect_rate_p5+3*(p.connect_rate_p95-p.connect_rate_p5)/8,4),']')
            WHEN connect_rate <= p.connect_rate_p5 + 4*(p.connect_rate_p95-p.connect_rate_p5)/8                            THEN CONCAT('Bin05_(',ROUND(p.connect_rate_p5+3*(p.connect_rate_p95-p.connect_rate_p5)/8,4),',',ROUND(p.connect_rate_p5+4*(p.connect_rate_p95-p.connect_rate_p5)/8,4),']')
            WHEN connect_rate <= p.connect_rate_p5 + 5*(p.connect_rate_p95-p.connect_rate_p5)/8                            THEN CONCAT('Bin06_(',ROUND(p.connect_rate_p5+4*(p.connect_rate_p95-p.connect_rate_p5)/8,4),',',ROUND(p.connect_rate_p5+5*(p.connect_rate_p95-p.connect_rate_p5)/8,4),']')
            WHEN connect_rate <= p.connect_rate_p5 + 6*(p.connect_rate_p95-p.connect_rate_p5)/8                            THEN CONCAT('Bin07_(',ROUND(p.connect_rate_p5+5*(p.connect_rate_p95-p.connect_rate_p5)/8,4),',',ROUND(p.connect_rate_p5+6*(p.connect_rate_p95-p.connect_rate_p5)/8,4),']')
            WHEN connect_rate <= p.connect_rate_p5 + 7*(p.connect_rate_p95-p.connect_rate_p5)/8                            THEN CONCAT('Bin08_(',ROUND(p.connect_rate_p5+6*(p.connect_rate_p95-p.connect_rate_p5)/8,4),',',ROUND(p.connect_rate_p5+7*(p.connect_rate_p95-p.connect_rate_p5)/8,4),']')
            WHEN connect_rate <= p.connect_rate_p95                                                                         THEN CONCAT('Bin09_(',ROUND(p.connect_rate_p5+7*(p.connect_rate_p95-p.connect_rate_p5)/8,4),',',ROUND(p.connect_rate_p95,4),']')
            ELSE                                                                                                                  CONCAT('Bin10_(',ROUND(p.connect_rate_p95,4),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 13. connect_30s_rate
    SELECT 'connect_30s_rate' AS metric, visitor_id,
        CASE
            WHEN connect_30s_rate <= 0 OR connect_30s_rate IS NULL THEN NULL
            WHEN connect_30s_rate <= p.connect_30s_rate_p5                                                                          THEN CONCAT('Bin01_(0,',ROUND(p.connect_30s_rate_p5,4),']')
            WHEN connect_30s_rate <= p.connect_30s_rate_p5 + 1*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.connect_30s_rate_p5,4),',',ROUND(p.connect_30s_rate_p5+1*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),']')
            WHEN connect_30s_rate <= p.connect_30s_rate_p5 + 2*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.connect_30s_rate_p5+1*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),',',ROUND(p.connect_30s_rate_p5+2*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),']')
            WHEN connect_30s_rate <= p.connect_30s_rate_p5 + 3*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.connect_30s_rate_p5+2*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),',',ROUND(p.connect_30s_rate_p5+3*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),']')
            WHEN connect_30s_rate <= p.connect_30s_rate_p5 + 4*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.connect_30s_rate_p5+3*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),',',ROUND(p.connect_30s_rate_p5+4*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),']')
            WHEN connect_30s_rate <= p.connect_30s_rate_p5 + 5*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.connect_30s_rate_p5+4*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),',',ROUND(p.connect_30s_rate_p5+5*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),']')
            WHEN connect_30s_rate <= p.connect_30s_rate_p5 + 6*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.connect_30s_rate_p5+5*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),',',ROUND(p.connect_30s_rate_p5+6*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),']')
            WHEN connect_30s_rate <= p.connect_30s_rate_p5 + 7*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.connect_30s_rate_p5+6*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),',',ROUND(p.connect_30s_rate_p5+7*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),']')
            WHEN connect_30s_rate <= p.connect_30s_rate_p95                                                                         THEN CONCAT('Bin09_(',ROUND(p.connect_30s_rate_p5+7*(p.connect_30s_rate_p95-p.connect_30s_rate_p5)/8,4),',',ROUND(p.connect_30s_rate_p95,4),']')
            ELSE                                                                                                                          CONCAT('Bin10_(',ROUND(p.connect_30s_rate_p95,4),',+inf)')
        END AS bin_label
    FROM uv_derived JOIN percentiles p ON uv_derived.join_key = p.join_key

    UNION ALL

    -- 14. deal_rate
    SELECT 'deal_rate' AS metric, visitor_id,
        CASE
            WHEN deal_rate <= 0 OR deal_rate IS NULL THEN NULL
            WHEN deal_rate <= p.deal_rate_p5                                                                THEN CONCAT('Bin01_(0,',ROUND(p.deal_rate_p5,4),']')
            WHEN deal_rate <= p.deal_rate_p5 + 1*(p.deal_rate_p95-p.deal_rate_p5)/8                        THEN CONCAT('Bin02_(',ROUND(p.deal_rate_p5,4),',',ROUND(p.deal_rate_p5+1*(p.deal_rate_p95-p.deal_rate_p5)/8,4),']')
            WHEN deal_rate <= p.deal_rate_p5 + 2*(p.deal_rate_p95-p.deal_rate_p5)/8                        THEN CONCAT('Bin03_(',ROUND(p.deal_rate_p5+1*(p.deal_rate_p95-p.deal_rate_p5)/8,4),',',ROUND(p.deal_rate_p5+2*(p.deal_rate_p95-p.deal_rate_p5)/8,4),']')
            WHEN deal_rate <= p.deal_rate_p5 + 3*(p.deal_rate_p95-p.deal_rate_p5)/8                        THEN CONCAT('Bin04_(',ROUND(p.deal_rate_p5+2*(p.deal_rate_p95-p.deal_rate_p5)/8,4),',',ROUND(p.deal_rate_p5+3*(p.deal_rate_p95-p.deal_rate_p5)/8,4),']')
            WHEN deal_rate <= p.deal_rate_p5 + 4*(p.deal_rate_p95-p.deal_rate_p5)/8                        THEN CONCAT('Bin05_(',ROUND(p.deal_rate_p5+3*(p.deal_rate_p95-p.deal_rate_p5)/8,4),',',ROUND(p.deal_rate_p5+4*(p.deal_rate_p95-p.deal_rate_p5)/8,4),']')
            WHEN deal_rate <= p.deal_rate_p5 + 5*(p.deal_rate_p95-p.deal_rate_p5)/8                        THEN CONCAT('Bin06_(',ROUND(p.deal_rate_p5+4*(p.deal_rate_p95-p.deal_rate_p5)/8,4),',',ROUND(p.deal_rate_p5+5*(p.deal_rate_p95-p.deal_rate_p5)/8,4),']')
            WHEN deal_rate <= p.deal_rate_p5 + 6*(p.deal_rate_p95-p.deal_rate_p5)/8                        THEN CONCAT('Bin07_(',ROUND(p.deal_rate_p5+5*(p.deal_rate_p95-p.deal_rate_p5)/8,4),',',ROUND(p.deal_rate_p5+6*(p.deal_rate_p95-p.deal_rate_p5)/8,4),']')
            WHEN deal_rate <= p.deal_rate_p5 + 7*(p.deal_rate_p95-p.deal_rate_p5)/8                        THEN CONCAT('Bin08_(',ROUND(p.deal_rate_p5+6*(p.deal_rate_p95-p.deal_rate_p5)/8,4),',',ROUND(p.deal_rate_p5+7*(p.deal_rate_p95-p.deal_rate_p5)/8,4),']')
            WHEN deal_rate <= p.deal_rate_p95                                                               THEN CONCAT('Bin09_(',ROUND(p.deal_rate_p5+7*(p.deal_rate_p95-p.deal_rate_p5)/8,4),',',ROUND(p.deal_rate_p95,4),']')
            ELSE                                                                                                 CONCAT('Bin10_(',ROUND(p.deal_rate_p95,4),',+inf)')
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
