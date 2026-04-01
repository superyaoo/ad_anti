-- ============================================================
-- 后效大盘分布分析 —— 曝光点击类（Hive 版）
-- 指标：req_cnt / impr_cnt / spam_impr_cnt / click_cnt / spam_click_cnt / ssr / ctr / cpm
-- 分箱策略：Bin1=(0,p5] | Bin2~9=p5~p95等距8箱 | Bin10=(p95,+∞)
-- ============================================================

WITH uv_metrics AS (
    SELECT
        1                                                                                    AS join_key,
        visitor_id,
        COUNT(DISTINCT llsid)                                                                AS req_cnt,
        COUNT(DISTINCT IF(action_type IN ('AD_ITEM_IMPRESSION','AD_PHOTO_IMPRESSION','AD_LIVE_IMPRESSION','AD_LIVE_PLAYED_STARTED'),
                         CONCAT(llsid, creative_id, unit_id), NULL))                        AS impr_cnt,
        COUNT(DISTINCT IF(is_for_report_engine = false
                         AND action_type IN ('AD_ITEM_IMPRESSION','AD_PHOTO_IMPRESSION','AD_LIVE_IMPRESSION','AD_LIVE_PLAYED_STARTED'),
                         CONCAT(llsid, creative_id, unit_id), NULL))                        AS spam_impr_cnt,
        COUNT(DISTINCT IF(action_type IN ('AD_ITEM_CLICK','AD_LIVE_CLICK','AD_PHOTO_CLICK'),
                         CONCAT(llsid, creative_id, unit_id), NULL))                        AS click_cnt,
        COUNT(DISTINCT IF(is_for_report_engine = false
                         AND action_type IN ('AD_ITEM_CLICK','AD_LIVE_CLICK','AD_PHOTO_CLICK'),
                         CONCAT(llsid, creative_id, unit_id), NULL))                        AS spam_click_cnt,
        CASE WHEN COUNT(DISTINCT llsid) = 0 THEN 0
             ELSE COUNT(DISTINCT IF(action_type IN ('AD_ITEM_IMPRESSION','AD_PHOTO_IMPRESSION','AD_LIVE_IMPRESSION','AD_LIVE_PLAYED_STARTED'),
                                   CONCAT(llsid, creative_id, unit_id), NULL)) * 1.0
                  / COUNT(DISTINCT llsid)
        END                                                                                  AS ssr,
        CASE WHEN COUNT(DISTINCT IF(action_type IN ('AD_ITEM_IMPRESSION','AD_PHOTO_IMPRESSION','AD_LIVE_IMPRESSION','AD_LIVE_PLAYED_STARTED'),
                                   CONCAT(llsid, creative_id, unit_id), NULL)) = 0 THEN 0
             ELSE COUNT(DISTINCT IF(action_type IN ('AD_ITEM_CLICK','AD_LIVE_CLICK','AD_PHOTO_CLICK'),
                                   CONCAT(llsid, creative_id, unit_id), NULL)) * 1.0
                  / COUNT(DISTINCT IF(action_type IN ('AD_ITEM_IMPRESSION','AD_PHOTO_IMPRESSION','AD_LIVE_IMPRESSION','AD_LIVE_PLAYED_STARTED'),
                                     CONCAT(llsid, creative_id, unit_id), NULL))
        END                                                                                  AS ctr,
        CASE WHEN COUNT(DISTINCT IF(action_type IN ('AD_ITEM_IMPRESSION','AD_PHOTO_IMPRESSION','AD_LIVE_IMPRESSION','AD_LIVE_PLAYED_STARTED'),
                                   CONCAT(llsid, creative_id, unit_id), NULL)) = 0 THEN 0
             ELSE SUM(cost_total) / 1000.0 * 1000
                  / COUNT(DISTINCT IF(action_type IN ('AD_ITEM_IMPRESSION','AD_PHOTO_IMPRESSION','AD_LIVE_IMPRESSION','AD_LIVE_PLAYED_STARTED'),
                                     CONCAT(llsid, creative_id, unit_id), NULL))
        END                                                                                  AS cpm
    FROM ks_ad_antispam.ks_anticheat_small_log_hi
    WHERE p_date = '20260320'
      AND media_app_id IN ('kuaishou_nebula', 'kuaishou')
      AND is_duplicate = false
      AND is_retry = false
    GROUP BY visitor_id
),

percentiles AS (
    SELECT
        1                                                                                                        AS join_key,
        percentile_approx(IF(req_cnt        > 0, CAST(req_cnt        AS DOUBLE), NULL), 0.05) AS req_p5,
        percentile_approx(IF(req_cnt        > 0, CAST(req_cnt        AS DOUBLE), NULL), 0.95) AS req_p95,
        percentile_approx(IF(impr_cnt       > 0, CAST(impr_cnt       AS DOUBLE), NULL), 0.05) AS impr_p5,
        percentile_approx(IF(impr_cnt       > 0, CAST(impr_cnt       AS DOUBLE), NULL), 0.95) AS impr_p95,
        percentile_approx(IF(spam_impr_cnt  > 0, CAST(spam_impr_cnt  AS DOUBLE), NULL), 0.05) AS spam_impr_p5,
        percentile_approx(IF(spam_impr_cnt  > 0, CAST(spam_impr_cnt  AS DOUBLE), NULL), 0.95) AS spam_impr_p95,
        percentile_approx(IF(click_cnt      > 0, CAST(click_cnt      AS DOUBLE), NULL), 0.05) AS click_p5,
        percentile_approx(IF(click_cnt      > 0, CAST(click_cnt      AS DOUBLE), NULL), 0.95) AS click_p95,
        percentile_approx(IF(spam_click_cnt > 0, CAST(spam_click_cnt AS DOUBLE), NULL), 0.05) AS spam_click_p5,
        percentile_approx(IF(spam_click_cnt > 0, CAST(spam_click_cnt AS DOUBLE), NULL), 0.95) AS spam_click_p95,
        percentile_approx(IF(ssr            > 0, ssr,                            NULL), 0.05) AS ssr_p5,
        percentile_approx(IF(ssr            > 0, ssr,                            NULL), 0.95) AS ssr_p95,
        percentile_approx(IF(ctr            > 0, ctr,                            NULL), 0.05) AS ctr_p5,
        percentile_approx(IF(ctr            > 0, ctr,                            NULL), 0.95) AS ctr_p95,
        percentile_approx(IF(cpm            > 0, cpm,                            NULL), 0.05) AS cpm_p5,
        percentile_approx(IF(cpm            > 0, cpm,                            NULL), 0.95) AS cpm_p95
    FROM uv_metrics
),

binned AS (
    -- 1. req_cnt
    SELECT 'req_cnt' AS metric, visitor_id,
        CASE
            WHEN req_cnt <= 0 THEN NULL
            WHEN req_cnt <= p.req_p5                              THEN CONCAT('Bin01_(0,',ROUND(p.req_p5,2),']')
            WHEN req_cnt <= p.req_p5+1*(p.req_p95-p.req_p5)/8    THEN CONCAT('Bin02_(',ROUND(p.req_p5,2),',',ROUND(p.req_p5+1*(p.req_p95-p.req_p5)/8,2),']')
            WHEN req_cnt <= p.req_p5+2*(p.req_p95-p.req_p5)/8    THEN CONCAT('Bin03_(',ROUND(p.req_p5+1*(p.req_p95-p.req_p5)/8,2),',',ROUND(p.req_p5+2*(p.req_p95-p.req_p5)/8,2),']')
            WHEN req_cnt <= p.req_p5+3*(p.req_p95-p.req_p5)/8    THEN CONCAT('Bin04_(',ROUND(p.req_p5+2*(p.req_p95-p.req_p5)/8,2),',',ROUND(p.req_p5+3*(p.req_p95-p.req_p5)/8,2),']')
            WHEN req_cnt <= p.req_p5+4*(p.req_p95-p.req_p5)/8    THEN CONCAT('Bin05_(',ROUND(p.req_p5+3*(p.req_p95-p.req_p5)/8,2),',',ROUND(p.req_p5+4*(p.req_p95-p.req_p5)/8,2),']')
            WHEN req_cnt <= p.req_p5+5*(p.req_p95-p.req_p5)/8    THEN CONCAT('Bin06_(',ROUND(p.req_p5+4*(p.req_p95-p.req_p5)/8,2),',',ROUND(p.req_p5+5*(p.req_p95-p.req_p5)/8,2),']')
            WHEN req_cnt <= p.req_p5+6*(p.req_p95-p.req_p5)/8    THEN CONCAT('Bin07_(',ROUND(p.req_p5+5*(p.req_p95-p.req_p5)/8,2),',',ROUND(p.req_p5+6*(p.req_p95-p.req_p5)/8,2),']')
            WHEN req_cnt <= p.req_p5+7*(p.req_p95-p.req_p5)/8    THEN CONCAT('Bin08_(',ROUND(p.req_p5+6*(p.req_p95-p.req_p5)/8,2),',',ROUND(p.req_p5+7*(p.req_p95-p.req_p5)/8,2),']')
            WHEN req_cnt <= p.req_p95                             THEN CONCAT('Bin09_(',ROUND(p.req_p5+7*(p.req_p95-p.req_p5)/8,2),',',ROUND(p.req_p95,2),']')
            ELSE                                                       CONCAT('Bin10_(',ROUND(p.req_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 2. impr_cnt
    SELECT 'impr_cnt' AS metric, visitor_id,
        CASE
            WHEN impr_cnt <= 0 THEN NULL
            WHEN impr_cnt <= p.impr_p5                               THEN CONCAT('Bin01_(0,',ROUND(p.impr_p5,2),']')
            WHEN impr_cnt <= p.impr_p5+1*(p.impr_p95-p.impr_p5)/8   THEN CONCAT('Bin02_(',ROUND(p.impr_p5,2),',',ROUND(p.impr_p5+1*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impr_cnt <= p.impr_p5+2*(p.impr_p95-p.impr_p5)/8   THEN CONCAT('Bin03_(',ROUND(p.impr_p5+1*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+2*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impr_cnt <= p.impr_p5+3*(p.impr_p95-p.impr_p5)/8   THEN CONCAT('Bin04_(',ROUND(p.impr_p5+2*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+3*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impr_cnt <= p.impr_p5+4*(p.impr_p95-p.impr_p5)/8   THEN CONCAT('Bin05_(',ROUND(p.impr_p5+3*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+4*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impr_cnt <= p.impr_p5+5*(p.impr_p95-p.impr_p5)/8   THEN CONCAT('Bin06_(',ROUND(p.impr_p5+4*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+5*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impr_cnt <= p.impr_p5+6*(p.impr_p95-p.impr_p5)/8   THEN CONCAT('Bin07_(',ROUND(p.impr_p5+5*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+6*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impr_cnt <= p.impr_p5+7*(p.impr_p95-p.impr_p5)/8   THEN CONCAT('Bin08_(',ROUND(p.impr_p5+6*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p5+7*(p.impr_p95-p.impr_p5)/8,2),']')
            WHEN impr_cnt <= p.impr_p95                              THEN CONCAT('Bin09_(',ROUND(p.impr_p5+7*(p.impr_p95-p.impr_p5)/8,2),',',ROUND(p.impr_p95,2),']')
            ELSE                                                          CONCAT('Bin10_(',ROUND(p.impr_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 3. spam_impr_cnt
    SELECT 'spam_impr_cnt' AS metric, visitor_id,
        CASE
            WHEN spam_impr_cnt <= 0 THEN NULL
            WHEN spam_impr_cnt <= p.spam_impr_p5                                       THEN CONCAT('Bin01_(0,',ROUND(p.spam_impr_p5,2),']')
            WHEN spam_impr_cnt <= p.spam_impr_p5+1*(p.spam_impr_p95-p.spam_impr_p5)/8 THEN CONCAT('Bin02_(',ROUND(p.spam_impr_p5,2),',',ROUND(p.spam_impr_p5+1*(p.spam_impr_p95-p.spam_impr_p5)/8,2),']')
            WHEN spam_impr_cnt <= p.spam_impr_p5+2*(p.spam_impr_p95-p.spam_impr_p5)/8 THEN CONCAT('Bin03_(',ROUND(p.spam_impr_p5+1*(p.spam_impr_p95-p.spam_impr_p5)/8,2),',',ROUND(p.spam_impr_p5+2*(p.spam_impr_p95-p.spam_impr_p5)/8,2),']')
            WHEN spam_impr_cnt <= p.spam_impr_p5+3*(p.spam_impr_p95-p.spam_impr_p5)/8 THEN CONCAT('Bin04_(',ROUND(p.spam_impr_p5+2*(p.spam_impr_p95-p.spam_impr_p5)/8,2),',',ROUND(p.spam_impr_p5+3*(p.spam_impr_p95-p.spam_impr_p5)/8,2),']')
            WHEN spam_impr_cnt <= p.spam_impr_p5+4*(p.spam_impr_p95-p.spam_impr_p5)/8 THEN CONCAT('Bin05_(',ROUND(p.spam_impr_p5+3*(p.spam_impr_p95-p.spam_impr_p5)/8,2),',',ROUND(p.spam_impr_p5+4*(p.spam_impr_p95-p.spam_impr_p5)/8,2),']')
            WHEN spam_impr_cnt <= p.spam_impr_p5+5*(p.spam_impr_p95-p.spam_impr_p5)/8 THEN CONCAT('Bin06_(',ROUND(p.spam_impr_p5+4*(p.spam_impr_p95-p.spam_impr_p5)/8,2),',',ROUND(p.spam_impr_p5+5*(p.spam_impr_p95-p.spam_impr_p5)/8,2),']')
            WHEN spam_impr_cnt <= p.spam_impr_p5+6*(p.spam_impr_p95-p.spam_impr_p5)/8 THEN CONCAT('Bin07_(',ROUND(p.spam_impr_p5+5*(p.spam_impr_p95-p.spam_impr_p5)/8,2),',',ROUND(p.spam_impr_p5+6*(p.spam_impr_p95-p.spam_impr_p5)/8,2),']')
            WHEN spam_impr_cnt <= p.spam_impr_p5+7*(p.spam_impr_p95-p.spam_impr_p5)/8 THEN CONCAT('Bin08_(',ROUND(p.spam_impr_p5+6*(p.spam_impr_p95-p.spam_impr_p5)/8,2),',',ROUND(p.spam_impr_p5+7*(p.spam_impr_p95-p.spam_impr_p5)/8,2),']')
            WHEN spam_impr_cnt <= p.spam_impr_p95                                      THEN CONCAT('Bin09_(',ROUND(p.spam_impr_p5+7*(p.spam_impr_p95-p.spam_impr_p5)/8,2),',',ROUND(p.spam_impr_p95,2),']')
            ELSE                                                                            CONCAT('Bin10_(',ROUND(p.spam_impr_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 4. click_cnt
    SELECT 'click_cnt' AS metric, visitor_id,
        CASE
            WHEN click_cnt <= 0 THEN NULL
            WHEN click_cnt <= p.click_p5                                 THEN CONCAT('Bin01_(0,',ROUND(p.click_p5,2),']')
            WHEN click_cnt <= p.click_p5+1*(p.click_p95-p.click_p5)/8   THEN CONCAT('Bin02_(',ROUND(p.click_p5,2),',',ROUND(p.click_p5+1*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5+2*(p.click_p95-p.click_p5)/8   THEN CONCAT('Bin03_(',ROUND(p.click_p5+1*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+2*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5+3*(p.click_p95-p.click_p5)/8   THEN CONCAT('Bin04_(',ROUND(p.click_p5+2*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+3*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5+4*(p.click_p95-p.click_p5)/8   THEN CONCAT('Bin05_(',ROUND(p.click_p5+3*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+4*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5+5*(p.click_p95-p.click_p5)/8   THEN CONCAT('Bin06_(',ROUND(p.click_p5+4*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+5*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5+6*(p.click_p95-p.click_p5)/8   THEN CONCAT('Bin07_(',ROUND(p.click_p5+5*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+6*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p5+7*(p.click_p95-p.click_p5)/8   THEN CONCAT('Bin08_(',ROUND(p.click_p5+6*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p5+7*(p.click_p95-p.click_p5)/8,2),']')
            WHEN click_cnt <= p.click_p95                                THEN CONCAT('Bin09_(',ROUND(p.click_p5+7*(p.click_p95-p.click_p5)/8,2),',',ROUND(p.click_p95,2),']')
            ELSE                                                              CONCAT('Bin10_(',ROUND(p.click_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 5. spam_click_cnt
    SELECT 'spam_click_cnt' AS metric, visitor_id,
        CASE
            WHEN spam_click_cnt <= 0 THEN NULL
            WHEN spam_click_cnt <= p.spam_click_p5                                         THEN CONCAT('Bin01_(0,',ROUND(p.spam_click_p5,2),']')
            WHEN spam_click_cnt <= p.spam_click_p5+1*(p.spam_click_p95-p.spam_click_p5)/8  THEN CONCAT('Bin02_(',ROUND(p.spam_click_p5,2),',',ROUND(p.spam_click_p5+1*(p.spam_click_p95-p.spam_click_p5)/8,2),']')
            WHEN spam_click_cnt <= p.spam_click_p5+2*(p.spam_click_p95-p.spam_click_p5)/8  THEN CONCAT('Bin03_(',ROUND(p.spam_click_p5+1*(p.spam_click_p95-p.spam_click_p5)/8,2),',',ROUND(p.spam_click_p5+2*(p.spam_click_p95-p.spam_click_p5)/8,2),']')
            WHEN spam_click_cnt <= p.spam_click_p5+3*(p.spam_click_p95-p.spam_click_p5)/8  THEN CONCAT('Bin04_(',ROUND(p.spam_click_p5+2*(p.spam_click_p95-p.spam_click_p5)/8,2),',',ROUND(p.spam_click_p5+3*(p.spam_click_p95-p.spam_click_p5)/8,2),']')
            WHEN spam_click_cnt <= p.spam_click_p5+4*(p.spam_click_p95-p.spam_click_p5)/8  THEN CONCAT('Bin05_(',ROUND(p.spam_click_p5+3*(p.spam_click_p95-p.spam_click_p5)/8,2),',',ROUND(p.spam_click_p5+4*(p.spam_click_p95-p.spam_click_p5)/8,2),']')
            WHEN spam_click_cnt <= p.spam_click_p5+5*(p.spam_click_p95-p.spam_click_p5)/8  THEN CONCAT('Bin06_(',ROUND(p.spam_click_p5+4*(p.spam_click_p95-p.spam_click_p5)/8,2),',',ROUND(p.spam_click_p5+5*(p.spam_click_p95-p.spam_click_p5)/8,2),']')
            WHEN spam_click_cnt <= p.spam_click_p5+6*(p.spam_click_p95-p.spam_click_p5)/8  THEN CONCAT('Bin07_(',ROUND(p.spam_click_p5+5*(p.spam_click_p95-p.spam_click_p5)/8,2),',',ROUND(p.spam_click_p5+6*(p.spam_click_p95-p.spam_click_p5)/8,2),']')
            WHEN spam_click_cnt <= p.spam_click_p5+7*(p.spam_click_p95-p.spam_click_p5)/8  THEN CONCAT('Bin08_(',ROUND(p.spam_click_p5+6*(p.spam_click_p95-p.spam_click_p5)/8,2),',',ROUND(p.spam_click_p5+7*(p.spam_click_p95-p.spam_click_p5)/8,2),']')
            WHEN spam_click_cnt <= p.spam_click_p95                                         THEN CONCAT('Bin09_(',ROUND(p.spam_click_p5+7*(p.spam_click_p95-p.spam_click_p5)/8,2),',',ROUND(p.spam_click_p95,2),']')
            ELSE                                                                                 CONCAT('Bin10_(',ROUND(p.spam_click_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 6. ssr
    SELECT 'ssr' AS metric, visitor_id,
        CASE
            WHEN ssr <= 0 THEN NULL
            WHEN ssr <= p.ssr_p5                             THEN CONCAT('Bin01_(0,',ROUND(p.ssr_p5,4),']')
            WHEN ssr <= p.ssr_p5+1*(p.ssr_p95-p.ssr_p5)/8   THEN CONCAT('Bin02_(',ROUND(p.ssr_p5,4),',',ROUND(p.ssr_p5+1*(p.ssr_p95-p.ssr_p5)/8,4),']')
            WHEN ssr <= p.ssr_p5+2*(p.ssr_p95-p.ssr_p5)/8   THEN CONCAT('Bin03_(',ROUND(p.ssr_p5+1*(p.ssr_p95-p.ssr_p5)/8,4),',',ROUND(p.ssr_p5+2*(p.ssr_p95-p.ssr_p5)/8,4),']')
            WHEN ssr <= p.ssr_p5+3*(p.ssr_p95-p.ssr_p5)/8   THEN CONCAT('Bin04_(',ROUND(p.ssr_p5+2*(p.ssr_p95-p.ssr_p5)/8,4),',',ROUND(p.ssr_p5+3*(p.ssr_p95-p.ssr_p5)/8,4),']')
            WHEN ssr <= p.ssr_p5+4*(p.ssr_p95-p.ssr_p5)/8   THEN CONCAT('Bin05_(',ROUND(p.ssr_p5+3*(p.ssr_p95-p.ssr_p5)/8,4),',',ROUND(p.ssr_p5+4*(p.ssr_p95-p.ssr_p5)/8,4),']')
            WHEN ssr <= p.ssr_p5+5*(p.ssr_p95-p.ssr_p5)/8   THEN CONCAT('Bin06_(',ROUND(p.ssr_p5+4*(p.ssr_p95-p.ssr_p5)/8,4),',',ROUND(p.ssr_p5+5*(p.ssr_p95-p.ssr_p5)/8,4),']')
            WHEN ssr <= p.ssr_p5+6*(p.ssr_p95-p.ssr_p5)/8   THEN CONCAT('Bin07_(',ROUND(p.ssr_p5+5*(p.ssr_p95-p.ssr_p5)/8,4),',',ROUND(p.ssr_p5+6*(p.ssr_p95-p.ssr_p5)/8,4),']')
            WHEN ssr <= p.ssr_p5+7*(p.ssr_p95-p.ssr_p5)/8   THEN CONCAT('Bin08_(',ROUND(p.ssr_p5+6*(p.ssr_p95-p.ssr_p5)/8,4),',',ROUND(p.ssr_p5+7*(p.ssr_p95-p.ssr_p5)/8,4),']')
            WHEN ssr <= p.ssr_p95                            THEN CONCAT('Bin09_(',ROUND(p.ssr_p5+7*(p.ssr_p95-p.ssr_p5)/8,4),',',ROUND(p.ssr_p95,4),']')
            ELSE                                                  CONCAT('Bin10_(',ROUND(p.ssr_p95,4),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 7. ctr
    SELECT 'ctr' AS metric, visitor_id,
        CASE
            WHEN ctr <= 0 THEN NULL
            WHEN ctr <= p.ctr_p5                             THEN CONCAT('Bin01_(0,',ROUND(p.ctr_p5,4),']')
            WHEN ctr <= p.ctr_p5+1*(p.ctr_p95-p.ctr_p5)/8   THEN CONCAT('Bin02_(',ROUND(p.ctr_p5,4),',',ROUND(p.ctr_p5+1*(p.ctr_p95-p.ctr_p5)/8,4),']')
            WHEN ctr <= p.ctr_p5+2*(p.ctr_p95-p.ctr_p5)/8   THEN CONCAT('Bin03_(',ROUND(p.ctr_p5+1*(p.ctr_p95-p.ctr_p5)/8,4),',',ROUND(p.ctr_p5+2*(p.ctr_p95-p.ctr_p5)/8,4),']')
            WHEN ctr <= p.ctr_p5+3*(p.ctr_p95-p.ctr_p5)/8   THEN CONCAT('Bin04_(',ROUND(p.ctr_p5+2*(p.ctr_p95-p.ctr_p5)/8,4),',',ROUND(p.ctr_p5+3*(p.ctr_p95-p.ctr_p5)/8,4),']')
            WHEN ctr <= p.ctr_p5+4*(p.ctr_p95-p.ctr_p5)/8   THEN CONCAT('Bin05_(',ROUND(p.ctr_p5+3*(p.ctr_p95-p.ctr_p5)/8,4),',',ROUND(p.ctr_p5+4*(p.ctr_p95-p.ctr_p5)/8,4),']')
            WHEN ctr <= p.ctr_p5+5*(p.ctr_p95-p.ctr_p5)/8   THEN CONCAT('Bin06_(',ROUND(p.ctr_p5+4*(p.ctr_p95-p.ctr_p5)/8,4),',',ROUND(p.ctr_p5+5*(p.ctr_p95-p.ctr_p5)/8,4),']')
            WHEN ctr <= p.ctr_p5+6*(p.ctr_p95-p.ctr_p5)/8   THEN CONCAT('Bin07_(',ROUND(p.ctr_p5+5*(p.ctr_p95-p.ctr_p5)/8,4),',',ROUND(p.ctr_p5+6*(p.ctr_p95-p.ctr_p5)/8,4),']')
            WHEN ctr <= p.ctr_p5+7*(p.ctr_p95-p.ctr_p5)/8   THEN CONCAT('Bin08_(',ROUND(p.ctr_p5+6*(p.ctr_p95-p.ctr_p5)/8,4),',',ROUND(p.ctr_p5+7*(p.ctr_p95-p.ctr_p5)/8,4),']')
            WHEN ctr <= p.ctr_p95                            THEN CONCAT('Bin09_(',ROUND(p.ctr_p5+7*(p.ctr_p95-p.ctr_p5)/8,4),',',ROUND(p.ctr_p95,4),']')
            ELSE                                                  CONCAT('Bin10_(',ROUND(p.ctr_p95,4),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 8. cpm
    SELECT 'cpm' AS metric, visitor_id,
        CASE
            WHEN cpm <= 0 THEN NULL
            WHEN cpm <= p.cpm_p5                             THEN CONCAT('Bin01_(0,',ROUND(p.cpm_p5,2),']')
            WHEN cpm <= p.cpm_p5+1*(p.cpm_p95-p.cpm_p5)/8   THEN CONCAT('Bin02_(',ROUND(p.cpm_p5,2),',',ROUND(p.cpm_p5+1*(p.cpm_p95-p.cpm_p5)/8,2),']')
            WHEN cpm <= p.cpm_p5+2*(p.cpm_p95-p.cpm_p5)/8   THEN CONCAT('Bin03_(',ROUND(p.cpm_p5+1*(p.cpm_p95-p.cpm_p5)/8,2),',',ROUND(p.cpm_p5+2*(p.cpm_p95-p.cpm_p5)/8,2),']')
            WHEN cpm <= p.cpm_p5+3*(p.cpm_p95-p.cpm_p5)/8   THEN CONCAT('Bin04_(',ROUND(p.cpm_p5+2*(p.cpm_p95-p.cpm_p5)/8,2),',',ROUND(p.cpm_p5+3*(p.cpm_p95-p.cpm_p5)/8,2),']')
            WHEN cpm <= p.cpm_p5+4*(p.cpm_p95-p.cpm_p5)/8   THEN CONCAT('Bin05_(',ROUND(p.cpm_p5+3*(p.cpm_p95-p.cpm_p5)/8,2),',',ROUND(p.cpm_p5+4*(p.cpm_p95-p.cpm_p5)/8,2),']')
            WHEN cpm <= p.cpm_p5+5*(p.cpm_p95-p.cpm_p5)/8   THEN CONCAT('Bin06_(',ROUND(p.cpm_p5+4*(p.cpm_p95-p.cpm_p5)/8,2),',',ROUND(p.cpm_p5+5*(p.cpm_p95-p.cpm_p5)/8,2),']')
            WHEN cpm <= p.cpm_p5+6*(p.cpm_p95-p.cpm_p5)/8   THEN CONCAT('Bin07_(',ROUND(p.cpm_p5+5*(p.cpm_p95-p.cpm_p5)/8,2),',',ROUND(p.cpm_p5+6*(p.cpm_p95-p.cpm_p5)/8,2),']')
            WHEN cpm <= p.cpm_p5+7*(p.cpm_p95-p.cpm_p5)/8   THEN CONCAT('Bin08_(',ROUND(p.cpm_p5+6*(p.cpm_p95-p.cpm_p5)/8,2),',',ROUND(p.cpm_p5+7*(p.cpm_p95-p.cpm_p5)/8,2),']')
            WHEN cpm <= p.cpm_p95                            THEN CONCAT('Bin09_(',ROUND(p.cpm_p5+7*(p.cpm_p95-p.cpm_p5)/8,2),',',ROUND(p.cpm_p95,2),']')
            ELSE                                                  CONCAT('Bin10_(',ROUND(p.cpm_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key
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
