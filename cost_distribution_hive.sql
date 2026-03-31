-- ============================================================
-- 后效大盘分布分析 —— 消耗类（Hive 版）
-- 分箱策略：Bin1=(0,p5] | Bin2~9=p5~p95等距8箱 | Bin10=(p95,+∞)
-- 修改 p_date 后直接在 Hive 执行
-- ============================================================

WITH cost_per_uv AS (
    SELECT
        1                                                                                                                       AS join_key,
        visitor_id,
        SUM(cost_total) / 1000.0                                                                                                AS total_cost_yuan,
        SUM(IF(is_for_report_engine = true,  cost_total, 0)) / 1000.0                                                          AS normal_cost_yuan,
        SUM(IF(is_for_report_engine = false, cost_total, 0)) / 1000.0                                                          AS spam_cost_yuan,
        SUM(IF(ocpc_action_type IN ('AD_MERCHANT_ROAS','EVENT_ORDER_PAIED','AD_STOREWIDE_ROAS','AD_MERCHANT_T7_ROI','AD_FANS_TOP_ROI'),          cost_total, 0)) / 1000.0 AS incycle_cost_yuan,
        SUM(IF(ocpc_action_type IN ('EVENT_ORDER_SUBMIT','AD_CID_ROAS','CID_ROAS','CID_EVENT_ORDER_PAID'),                                       cost_total, 0)) / 1000.0 AS excycle_cid_cost_yuan,
        SUM(IF(ocpc_action_type IN ('AD_LANDING_PAGE_FORM_SUBMITTED','EVENT_VALID_CLUES','AD_EFFECTIVE_CUSTOMER_ACQUISITION','EVENT_INTENTION_CONFIRMED','LEADS_SUBMIT','EVENT_PHONE_GET_THROUGH'), cost_total, 0)) / 1000.0 AS clue_cost_yuan,
        SUM(IF(ocpc_action_type IN ('AD_CONVERSION','AD_PURCHASE','AD_PURCHASE_CONVERSION','AD_ROAS','AD_SEVEN_DAY_ROAS','AD_ITEM_DOWNLOAD_COMPLETED','AD_ITEM_CLICK_DOWNLOAD'),                   cost_total, 0)) / 1000.0 AS download_cost_yuan
    FROM ks_origin_ad_log.ad_callback_log_from_ad_log_full
    WHERE p_date = '20250101'
      AND is_duplicate = false
      AND is_retry = false
      AND action_type = charge_action_type
    GROUP BY visitor_id
),

percentiles AS (
    SELECT
        1                                                                                                                       AS join_key,
        percentile_approx(IF(total_cost_yuan       > 0, total_cost_yuan,       NULL), 0.05) AS total_p5,
        percentile_approx(IF(total_cost_yuan       > 0, total_cost_yuan,       NULL), 0.95) AS total_p95,
        percentile_approx(IF(normal_cost_yuan      > 0, normal_cost_yuan,      NULL), 0.05) AS normal_p5,
        percentile_approx(IF(normal_cost_yuan      > 0, normal_cost_yuan,      NULL), 0.95) AS normal_p95,
        percentile_approx(IF(spam_cost_yuan        > 0, spam_cost_yuan,        NULL), 0.05) AS spam_p5,
        percentile_approx(IF(spam_cost_yuan        > 0, spam_cost_yuan,        NULL), 0.95) AS spam_p95,
        percentile_approx(IF(incycle_cost_yuan     > 0, incycle_cost_yuan,     NULL), 0.05) AS incycle_p5,
        percentile_approx(IF(incycle_cost_yuan     > 0, incycle_cost_yuan,     NULL), 0.95) AS incycle_p95,
        percentile_approx(IF(excycle_cid_cost_yuan > 0, excycle_cid_cost_yuan, NULL), 0.05) AS excycle_p5,
        percentile_approx(IF(excycle_cid_cost_yuan > 0, excycle_cid_cost_yuan, NULL), 0.95) AS excycle_p95,
        percentile_approx(IF(clue_cost_yuan        > 0, clue_cost_yuan,        NULL), 0.05) AS clue_p5,
        percentile_approx(IF(clue_cost_yuan        > 0, clue_cost_yuan,        NULL), 0.95) AS clue_p95,
        percentile_approx(IF(download_cost_yuan    > 0, download_cost_yuan,    NULL), 0.05) AS download_p5,
        percentile_approx(IF(download_cost_yuan    > 0, download_cost_yuan,    NULL), 0.95) AS download_p95
    FROM cost_per_uv
),

binned AS (
    -- 1. total_cost_yuan
    SELECT 'total_cost_yuan' AS metric, visitor_id,
        CASE
            WHEN total_cost_yuan <= 0 OR total_cost_yuan IS NULL THEN NULL
            WHEN total_cost_yuan <= p.total_p5                                                  THEN CONCAT('Bin01_(0,',         ROUND(p.total_p5,2), ']')
            WHEN total_cost_yuan <= p.total_p5 + 1*(p.total_p95-p.total_p5)/8                  THEN CONCAT('Bin02_(',ROUND(p.total_p5,2),',',ROUND(p.total_p5+1*(p.total_p95-p.total_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_p5 + 2*(p.total_p95-p.total_p5)/8                  THEN CONCAT('Bin03_(',ROUND(p.total_p5+1*(p.total_p95-p.total_p5)/8,2),',',ROUND(p.total_p5+2*(p.total_p95-p.total_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_p5 + 3*(p.total_p95-p.total_p5)/8                  THEN CONCAT('Bin04_(',ROUND(p.total_p5+2*(p.total_p95-p.total_p5)/8,2),',',ROUND(p.total_p5+3*(p.total_p95-p.total_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_p5 + 4*(p.total_p95-p.total_p5)/8                  THEN CONCAT('Bin05_(',ROUND(p.total_p5+3*(p.total_p95-p.total_p5)/8,2),',',ROUND(p.total_p5+4*(p.total_p95-p.total_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_p5 + 5*(p.total_p95-p.total_p5)/8                  THEN CONCAT('Bin06_(',ROUND(p.total_p5+4*(p.total_p95-p.total_p5)/8,2),',',ROUND(p.total_p5+5*(p.total_p95-p.total_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_p5 + 6*(p.total_p95-p.total_p5)/8                  THEN CONCAT('Bin07_(',ROUND(p.total_p5+5*(p.total_p95-p.total_p5)/8,2),',',ROUND(p.total_p5+6*(p.total_p95-p.total_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_p5 + 7*(p.total_p95-p.total_p5)/8                  THEN CONCAT('Bin08_(',ROUND(p.total_p5+6*(p.total_p95-p.total_p5)/8,2),',',ROUND(p.total_p5+7*(p.total_p95-p.total_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_p95                                                 THEN CONCAT('Bin09_(',ROUND(p.total_p5+7*(p.total_p95-p.total_p5)/8,2),',',ROUND(p.total_p95,2),']')
            ELSE                                                                                     CONCAT('Bin10_(',ROUND(p.total_p95,2), ',+inf)')
        END AS bin_label
    FROM cost_per_uv JOIN percentiles p ON cost_per_uv.join_key = p.join_key

    UNION ALL

    -- 2. normal_cost_yuan
    SELECT 'normal_cost_yuan' AS metric, visitor_id,
        CASE
            WHEN normal_cost_yuan <= 0 OR normal_cost_yuan IS NULL THEN NULL
            WHEN normal_cost_yuan <= p.normal_p5                                                THEN CONCAT('Bin01_(0,',         ROUND(p.normal_p5,2), ']')
            WHEN normal_cost_yuan <= p.normal_p5 + 1*(p.normal_p95-p.normal_p5)/8              THEN CONCAT('Bin02_(',ROUND(p.normal_p5,2),',',ROUND(p.normal_p5+1*(p.normal_p95-p.normal_p5)/8,2),']')
            WHEN normal_cost_yuan <= p.normal_p5 + 2*(p.normal_p95-p.normal_p5)/8              THEN CONCAT('Bin03_(',ROUND(p.normal_p5+1*(p.normal_p95-p.normal_p5)/8,2),',',ROUND(p.normal_p5+2*(p.normal_p95-p.normal_p5)/8,2),']')
            WHEN normal_cost_yuan <= p.normal_p5 + 3*(p.normal_p95-p.normal_p5)/8              THEN CONCAT('Bin04_(',ROUND(p.normal_p5+2*(p.normal_p95-p.normal_p5)/8,2),',',ROUND(p.normal_p5+3*(p.normal_p95-p.normal_p5)/8,2),']')
            WHEN normal_cost_yuan <= p.normal_p5 + 4*(p.normal_p95-p.normal_p5)/8              THEN CONCAT('Bin05_(',ROUND(p.normal_p5+3*(p.normal_p95-p.normal_p5)/8,2),',',ROUND(p.normal_p5+4*(p.normal_p95-p.normal_p5)/8,2),']')
            WHEN normal_cost_yuan <= p.normal_p5 + 5*(p.normal_p95-p.normal_p5)/8              THEN CONCAT('Bin06_(',ROUND(p.normal_p5+4*(p.normal_p95-p.normal_p5)/8,2),',',ROUND(p.normal_p5+5*(p.normal_p95-p.normal_p5)/8,2),']')
            WHEN normal_cost_yuan <= p.normal_p5 + 6*(p.normal_p95-p.normal_p5)/8              THEN CONCAT('Bin07_(',ROUND(p.normal_p5+5*(p.normal_p95-p.normal_p5)/8,2),',',ROUND(p.normal_p5+6*(p.normal_p95-p.normal_p5)/8,2),']')
            WHEN normal_cost_yuan <= p.normal_p5 + 7*(p.normal_p95-p.normal_p5)/8              THEN CONCAT('Bin08_(',ROUND(p.normal_p5+6*(p.normal_p95-p.normal_p5)/8,2),',',ROUND(p.normal_p5+7*(p.normal_p95-p.normal_p5)/8,2),']')
            WHEN normal_cost_yuan <= p.normal_p95                                               THEN CONCAT('Bin09_(',ROUND(p.normal_p5+7*(p.normal_p95-p.normal_p5)/8,2),',',ROUND(p.normal_p95,2),']')
            ELSE                                                                                     CONCAT('Bin10_(',ROUND(p.normal_p95,2), ',+inf)')
        END AS bin_label
    FROM cost_per_uv JOIN percentiles p ON cost_per_uv.join_key = p.join_key

    UNION ALL

    -- 3. spam_cost_yuan
    SELECT 'spam_cost_yuan' AS metric, visitor_id,
        CASE
            WHEN spam_cost_yuan <= 0 OR spam_cost_yuan IS NULL THEN NULL
            WHEN spam_cost_yuan <= p.spam_p5                                                    THEN CONCAT('Bin01_(0,',         ROUND(p.spam_p5,2), ']')
            WHEN spam_cost_yuan <= p.spam_p5 + 1*(p.spam_p95-p.spam_p5)/8                      THEN CONCAT('Bin02_(',ROUND(p.spam_p5,2),',',ROUND(p.spam_p5+1*(p.spam_p95-p.spam_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_p5 + 2*(p.spam_p95-p.spam_p5)/8                      THEN CONCAT('Bin03_(',ROUND(p.spam_p5+1*(p.spam_p95-p.spam_p5)/8,2),',',ROUND(p.spam_p5+2*(p.spam_p95-p.spam_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_p5 + 3*(p.spam_p95-p.spam_p5)/8                      THEN CONCAT('Bin04_(',ROUND(p.spam_p5+2*(p.spam_p95-p.spam_p5)/8,2),',',ROUND(p.spam_p5+3*(p.spam_p95-p.spam_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_p5 + 4*(p.spam_p95-p.spam_p5)/8                      THEN CONCAT('Bin05_(',ROUND(p.spam_p5+3*(p.spam_p95-p.spam_p5)/8,2),',',ROUND(p.spam_p5+4*(p.spam_p95-p.spam_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_p5 + 5*(p.spam_p95-p.spam_p5)/8                      THEN CONCAT('Bin06_(',ROUND(p.spam_p5+4*(p.spam_p95-p.spam_p5)/8,2),',',ROUND(p.spam_p5+5*(p.spam_p95-p.spam_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_p5 + 6*(p.spam_p95-p.spam_p5)/8                      THEN CONCAT('Bin07_(',ROUND(p.spam_p5+5*(p.spam_p95-p.spam_p5)/8,2),',',ROUND(p.spam_p5+6*(p.spam_p95-p.spam_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_p5 + 7*(p.spam_p95-p.spam_p5)/8                      THEN CONCAT('Bin08_(',ROUND(p.spam_p5+6*(p.spam_p95-p.spam_p5)/8,2),',',ROUND(p.spam_p5+7*(p.spam_p95-p.spam_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_p95                                                   THEN CONCAT('Bin09_(',ROUND(p.spam_p5+7*(p.spam_p95-p.spam_p5)/8,2),',',ROUND(p.spam_p95,2),']')
            ELSE                                                                                     CONCAT('Bin10_(',ROUND(p.spam_p95,2), ',+inf)')
        END AS bin_label
    FROM cost_per_uv JOIN percentiles p ON cost_per_uv.join_key = p.join_key

    UNION ALL

    -- 4. incycle_cost_yuan
    SELECT 'incycle_cost_yuan' AS metric, visitor_id,
        CASE
            WHEN incycle_cost_yuan <= 0 OR incycle_cost_yuan IS NULL THEN NULL
            WHEN incycle_cost_yuan <= p.incycle_p5                                              THEN CONCAT('Bin01_(0,',         ROUND(p.incycle_p5,2), ']')
            WHEN incycle_cost_yuan <= p.incycle_p5 + 1*(p.incycle_p95-p.incycle_p5)/8          THEN CONCAT('Bin02_(',ROUND(p.incycle_p5,2),',',ROUND(p.incycle_p5+1*(p.incycle_p95-p.incycle_p5)/8,2),']')
            WHEN incycle_cost_yuan <= p.incycle_p5 + 2*(p.incycle_p95-p.incycle_p5)/8          THEN CONCAT('Bin03_(',ROUND(p.incycle_p5+1*(p.incycle_p95-p.incycle_p5)/8,2),',',ROUND(p.incycle_p5+2*(p.incycle_p95-p.incycle_p5)/8,2),']')
            WHEN incycle_cost_yuan <= p.incycle_p5 + 3*(p.incycle_p95-p.incycle_p5)/8          THEN CONCAT('Bin04_(',ROUND(p.incycle_p5+2*(p.incycle_p95-p.incycle_p5)/8,2),',',ROUND(p.incycle_p5+3*(p.incycle_p95-p.incycle_p5)/8,2),']')
            WHEN incycle_cost_yuan <= p.incycle_p5 + 4*(p.incycle_p95-p.incycle_p5)/8          THEN CONCAT('Bin05_(',ROUND(p.incycle_p5+3*(p.incycle_p95-p.incycle_p5)/8,2),',',ROUND(p.incycle_p5+4*(p.incycle_p95-p.incycle_p5)/8,2),']')
            WHEN incycle_cost_yuan <= p.incycle_p5 + 5*(p.incycle_p95-p.incycle_p5)/8          THEN CONCAT('Bin06_(',ROUND(p.incycle_p5+4*(p.incycle_p95-p.incycle_p5)/8,2),',',ROUND(p.incycle_p5+5*(p.incycle_p95-p.incycle_p5)/8,2),']')
            WHEN incycle_cost_yuan <= p.incycle_p5 + 6*(p.incycle_p95-p.incycle_p5)/8          THEN CONCAT('Bin07_(',ROUND(p.incycle_p5+5*(p.incycle_p95-p.incycle_p5)/8,2),',',ROUND(p.incycle_p5+6*(p.incycle_p95-p.incycle_p5)/8,2),']')
            WHEN incycle_cost_yuan <= p.incycle_p5 + 7*(p.incycle_p95-p.incycle_p5)/8          THEN CONCAT('Bin08_(',ROUND(p.incycle_p5+6*(p.incycle_p95-p.incycle_p5)/8,2),',',ROUND(p.incycle_p5+7*(p.incycle_p95-p.incycle_p5)/8,2),']')
            WHEN incycle_cost_yuan <= p.incycle_p95                                             THEN CONCAT('Bin09_(',ROUND(p.incycle_p5+7*(p.incycle_p95-p.incycle_p5)/8,2),',',ROUND(p.incycle_p95,2),']')
            ELSE                                                                                     CONCAT('Bin10_(',ROUND(p.incycle_p95,2), ',+inf)')
        END AS bin_label
    FROM cost_per_uv JOIN percentiles p ON cost_per_uv.join_key = p.join_key

    UNION ALL

    -- 5. excycle_cid_cost_yuan
    SELECT 'excycle_cid_cost_yuan' AS metric, visitor_id,
        CASE
            WHEN excycle_cid_cost_yuan <= 0 OR excycle_cid_cost_yuan IS NULL THEN NULL
            WHEN excycle_cid_cost_yuan <= p.excycle_p5                                          THEN CONCAT('Bin01_(0,',         ROUND(p.excycle_p5,2), ']')
            WHEN excycle_cid_cost_yuan <= p.excycle_p5 + 1*(p.excycle_p95-p.excycle_p5)/8      THEN CONCAT('Bin02_(',ROUND(p.excycle_p5,2),',',ROUND(p.excycle_p5+1*(p.excycle_p95-p.excycle_p5)/8,2),']')
            WHEN excycle_cid_cost_yuan <= p.excycle_p5 + 2*(p.excycle_p95-p.excycle_p5)/8      THEN CONCAT('Bin03_(',ROUND(p.excycle_p5+1*(p.excycle_p95-p.excycle_p5)/8,2),',',ROUND(p.excycle_p5+2*(p.excycle_p95-p.excycle_p5)/8,2),']')
            WHEN excycle_cid_cost_yuan <= p.excycle_p5 + 3*(p.excycle_p95-p.excycle_p5)/8      THEN CONCAT('Bin04_(',ROUND(p.excycle_p5+2*(p.excycle_p95-p.excycle_p5)/8,2),',',ROUND(p.excycle_p5+3*(p.excycle_p95-p.excycle_p5)/8,2),']')
            WHEN excycle_cid_cost_yuan <= p.excycle_p5 + 4*(p.excycle_p95-p.excycle_p5)/8      THEN CONCAT('Bin05_(',ROUND(p.excycle_p5+3*(p.excycle_p95-p.excycle_p5)/8,2),',',ROUND(p.excycle_p5+4*(p.excycle_p95-p.excycle_p5)/8,2),']')
            WHEN excycle_cid_cost_yuan <= p.excycle_p5 + 5*(p.excycle_p95-p.excycle_p5)/8      THEN CONCAT('Bin06_(',ROUND(p.excycle_p5+4*(p.excycle_p95-p.excycle_p5)/8,2),',',ROUND(p.excycle_p5+5*(p.excycle_p95-p.excycle_p5)/8,2),']')
            WHEN excycle_cid_cost_yuan <= p.excycle_p5 + 6*(p.excycle_p95-p.excycle_p5)/8      THEN CONCAT('Bin07_(',ROUND(p.excycle_p5+5*(p.excycle_p95-p.excycle_p5)/8,2),',',ROUND(p.excycle_p5+6*(p.excycle_p95-p.excycle_p5)/8,2),']')
            WHEN excycle_cid_cost_yuan <= p.excycle_p5 + 7*(p.excycle_p95-p.excycle_p5)/8      THEN CONCAT('Bin08_(',ROUND(p.excycle_p5+6*(p.excycle_p95-p.excycle_p5)/8,2),',',ROUND(p.excycle_p5+7*(p.excycle_p95-p.excycle_p5)/8,2),']')
            WHEN excycle_cid_cost_yuan <= p.excycle_p95                                         THEN CONCAT('Bin09_(',ROUND(p.excycle_p5+7*(p.excycle_p95-p.excycle_p5)/8,2),',',ROUND(p.excycle_p95,2),']')
            ELSE                                                                                     CONCAT('Bin10_(',ROUND(p.excycle_p95,2), ',+inf)')
        END AS bin_label
    FROM cost_per_uv JOIN percentiles p ON cost_per_uv.join_key = p.join_key

    UNION ALL

    -- 6. clue_cost_yuan
    SELECT 'clue_cost_yuan' AS metric, visitor_id,
        CASE
            WHEN clue_cost_yuan <= 0 OR clue_cost_yuan IS NULL THEN NULL
            WHEN clue_cost_yuan <= p.clue_p5                                                    THEN CONCAT('Bin01_(0,',         ROUND(p.clue_p5,2), ']')
            WHEN clue_cost_yuan <= p.clue_p5 + 1*(p.clue_p95-p.clue_p5)/8                      THEN CONCAT('Bin02_(',ROUND(p.clue_p5,2),',',ROUND(p.clue_p5+1*(p.clue_p95-p.clue_p5)/8,2),']')
            WHEN clue_cost_yuan <= p.clue_p5 + 2*(p.clue_p95-p.clue_p5)/8                      THEN CONCAT('Bin03_(',ROUND(p.clue_p5+1*(p.clue_p95-p.clue_p5)/8,2),',',ROUND(p.clue_p5+2*(p.clue_p95-p.clue_p5)/8,2),']')
            WHEN clue_cost_yuan <= p.clue_p5 + 3*(p.clue_p95-p.clue_p5)/8                      THEN CONCAT('Bin04_(',ROUND(p.clue_p5+2*(p.clue_p95-p.clue_p5)/8,2),',',ROUND(p.clue_p5+3*(p.clue_p95-p.clue_p5)/8,2),']')
            WHEN clue_cost_yuan <= p.clue_p5 + 4*(p.clue_p95-p.clue_p5)/8                      THEN CONCAT('Bin05_(',ROUND(p.clue_p5+3*(p.clue_p95-p.clue_p5)/8,2),',',ROUND(p.clue_p5+4*(p.clue_p95-p.clue_p5)/8,2),']')
            WHEN clue_cost_yuan <= p.clue_p5 + 5*(p.clue_p95-p.clue_p5)/8                      THEN CONCAT('Bin06_(',ROUND(p.clue_p5+4*(p.clue_p95-p.clue_p5)/8,2),',',ROUND(p.clue_p5+5*(p.clue_p95-p.clue_p5)/8,2),']')
            WHEN clue_cost_yuan <= p.clue_p5 + 6*(p.clue_p95-p.clue_p5)/8                      THEN CONCAT('Bin07_(',ROUND(p.clue_p5+5*(p.clue_p95-p.clue_p5)/8,2),',',ROUND(p.clue_p5+6*(p.clue_p95-p.clue_p5)/8,2),']')
            WHEN clue_cost_yuan <= p.clue_p5 + 7*(p.clue_p95-p.clue_p5)/8                      THEN CONCAT('Bin08_(',ROUND(p.clue_p5+6*(p.clue_p95-p.clue_p5)/8,2),',',ROUND(p.clue_p5+7*(p.clue_p95-p.clue_p5)/8,2),']')
            WHEN clue_cost_yuan <= p.clue_p95                                                   THEN CONCAT('Bin09_(',ROUND(p.clue_p5+7*(p.clue_p95-p.clue_p5)/8,2),',',ROUND(p.clue_p95,2),']')
            ELSE                                                                                     CONCAT('Bin10_(',ROUND(p.clue_p95,2), ',+inf)')
        END AS bin_label
    FROM cost_per_uv JOIN percentiles p ON cost_per_uv.join_key = p.join_key

    UNION ALL

    -- 7. download_cost_yuan
    SELECT 'download_cost_yuan' AS metric, visitor_id,
        CASE
            WHEN download_cost_yuan <= 0 OR download_cost_yuan IS NULL THEN NULL
            WHEN download_cost_yuan <= p.download_p5                                            THEN CONCAT('Bin01_(0,',         ROUND(p.download_p5,2), ']')
            WHEN download_cost_yuan <= p.download_p5 + 1*(p.download_p95-p.download_p5)/8      THEN CONCAT('Bin02_(',ROUND(p.download_p5,2),',',ROUND(p.download_p5+1*(p.download_p95-p.download_p5)/8,2),']')
            WHEN download_cost_yuan <= p.download_p5 + 2*(p.download_p95-p.download_p5)/8      THEN CONCAT('Bin03_(',ROUND(p.download_p5+1*(p.download_p95-p.download_p5)/8,2),',',ROUND(p.download_p5+2*(p.download_p95-p.download_p5)/8,2),']')
            WHEN download_cost_yuan <= p.download_p5 + 3*(p.download_p95-p.download_p5)/8      THEN CONCAT('Bin04_(',ROUND(p.download_p5+2*(p.download_p95-p.download_p5)/8,2),',',ROUND(p.download_p5+3*(p.download_p95-p.download_p5)/8,2),']')
            WHEN download_cost_yuan <= p.download_p5 + 4*(p.download_p95-p.download_p5)/8      THEN CONCAT('Bin05_(',ROUND(p.download_p5+3*(p.download_p95-p.download_p5)/8,2),',',ROUND(p.download_p5+4*(p.download_p95-p.download_p5)/8,2),']')
            WHEN download_cost_yuan <= p.download_p5 + 5*(p.download_p95-p.download_p5)/8      THEN CONCAT('Bin06_(',ROUND(p.download_p5+4*(p.download_p95-p.download_p5)/8,2),',',ROUND(p.download_p5+5*(p.download_p95-p.download_p5)/8,2),']')
            WHEN download_cost_yuan <= p.download_p5 + 6*(p.download_p95-p.download_p5)/8      THEN CONCAT('Bin07_(',ROUND(p.download_p5+5*(p.download_p95-p.download_p5)/8,2),',',ROUND(p.download_p5+6*(p.download_p95-p.download_p5)/8,2),']')
            WHEN download_cost_yuan <= p.download_p5 + 7*(p.download_p95-p.download_p5)/8      THEN CONCAT('Bin08_(',ROUND(p.download_p5+6*(p.download_p95-p.download_p5)/8,2),',',ROUND(p.download_p5+7*(p.download_p95-p.download_p5)/8,2),']')
            WHEN download_cost_yuan <= p.download_p95                                           THEN CONCAT('Bin09_(',ROUND(p.download_p5+7*(p.download_p95-p.download_p5)/8,2),',',ROUND(p.download_p95,2),']')
            ELSE                                                                                     CONCAT('Bin10_(',ROUND(p.download_p95,2), ',+inf)')
        END AS bin_label
    FROM cost_per_uv JOIN percentiles p ON cost_per_uv.join_key = p.join_key
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
