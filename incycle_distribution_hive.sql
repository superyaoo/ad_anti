-- ============================================================
-- 后效大盘分布分析 —— 内循环后效（Hive 版）
-- 订单数据来源：ks_ad_antispam.ad_merchant_order_wide_feature_base_di
-- 消耗数据来源：ad_rc_data.ad_kuaishou_account_visitor_stat_di
-- ocpc_action_type：AD_MERCHANT_ROAS / EVENT_ORDER_PAIED / AD_STOREWIDE_ROAS / AD_MERCHANT_T7_ROI / AD_FANS_TOP_ROI
-- 分箱策略：Bin1=(0,p5] | Bin2~9=p5~p95等距8箱 | Bin10=(p95,+∞)
-- 维度：visitor_id
-- ============================================================

WITH

order_info AS (
    SELECT
        visitor_id,
        1                                                                        AS join_key,
        SUM(order_product_payment_amt) / 100000.0                                AS gmv,
        SUM(IF(is_refund > 0 AND refund_type <> 2, order_product_payment_amt, 0)) / 100000.0 AS refund_gmv,
        COUNT(*)                                                                 AS order_cnt,
        SUM(IF(is_refund > 0 AND refund_type <> 2, 1, 0))                       AS refund_cnt
    FROM ks_ad_antispam.ad_merchant_order_wide_feature_base_di
    WHERE p_date = '20250101'
      AND attribution_type = 1
      AND resource_type <> 'UNION'
      AND ocpc_action_type IN ('EVENT_ORDER_PAIED','AD_MERCHANT_T7_ROI','AD_STOREWIDE_ROAS','AD_MERCHANT_ROAS','AD_FANS_TOP_ROI')
    GROUP BY visitor_id
),

cost_info AS (
    SELECT
        visitor_id,
        1                                                                        AS join_key,
        SUM(cost_yuan)                                                           AS cost_yuan,
        SUM(spam_cost_yuan)                                                      AS spam_cost_yuan,
        SUM(cost_yuan + spam_cost_yuan)                                          AS total_cost_yuan
    FROM ad_rc_data.ad_kuaishou_account_visitor_stat_di
    WHERE p_date = '20250101'
      AND ocpc_action_type IN ('EVENT_ORDER_PAIED','AD_MERCHANT_T7_ROI','AD_STOREWIDE_ROAS','AD_MERCHANT_ROAS','AD_FANS_TOP_ROI')
    GROUP BY visitor_id
),

uv_metrics AS (
    SELECT
        1                                                                        AS join_key,
        COALESCE(o.visitor_id, c.visitor_id)                                     AS visitor_id,
        o.gmv,
        o.refund_gmv,
        CAST(o.order_cnt AS DOUBLE)                                              AS order_cnt,
        CAST(o.refund_cnt AS DOUBLE)                                             AS refund_cnt,
        IF(o.order_cnt > 0,     o.gmv / o.order_cnt,                        NULL) AS avg_price,
        IF(o.order_cnt > 0,     (o.gmv - o.refund_gmv) / o.order_cnt,       NULL) AS avg_net_price,
        IF(o.order_cnt > 0,     o.refund_gmv / o.order_cnt,                 NULL) AS refund_price,
        c.cost_yuan,
        c.spam_cost_yuan,
        c.total_cost_yuan,
        IF(c.cost_yuan > 0,       (o.gmv - o.refund_gmv) / c.cost_yuan,     NULL) AS roi,
        IF(c.total_cost_yuan > 0, (o.gmv - o.refund_gmv) / c.total_cost_yuan, NULL) AS roi_total
    FROM order_info o
    FULL OUTER JOIN cost_info c ON o.visitor_id = c.visitor_id
),

percentiles AS (
    SELECT
        1                                                                        AS join_key,
        percentile_approx(IF(gmv           > 0, gmv,           NULL), 0.05)     AS gmv_p5,
        percentile_approx(IF(gmv           > 0, gmv,           NULL), 0.95)     AS gmv_p95,
        percentile_approx(IF(refund_gmv    > 0, refund_gmv,    NULL), 0.05)     AS refund_gmv_p5,
        percentile_approx(IF(refund_gmv    > 0, refund_gmv,    NULL), 0.95)     AS refund_gmv_p95,
        percentile_approx(IF(order_cnt     > 0, order_cnt,     NULL), 0.05)     AS order_cnt_p5,
        percentile_approx(IF(order_cnt     > 0, order_cnt,     NULL), 0.95)     AS order_cnt_p95,
        percentile_approx(IF(refund_cnt    > 0, refund_cnt,    NULL), 0.05)     AS refund_cnt_p5,
        percentile_approx(IF(refund_cnt    > 0, refund_cnt,    NULL), 0.95)     AS refund_cnt_p95,
        percentile_approx(IF(avg_price     > 0, avg_price,     NULL), 0.05)     AS avg_price_p5,
        percentile_approx(IF(avg_price     > 0, avg_price,     NULL), 0.95)     AS avg_price_p95,
        percentile_approx(IF(avg_net_price > 0, avg_net_price, NULL), 0.05)     AS avg_net_price_p5,
        percentile_approx(IF(avg_net_price > 0, avg_net_price, NULL), 0.95)     AS avg_net_price_p95,
        percentile_approx(IF(refund_price  > 0, refund_price,  NULL), 0.05)     AS refund_price_p5,
        percentile_approx(IF(refund_price  > 0, refund_price,  NULL), 0.95)     AS refund_price_p95,
        percentile_approx(IF(cost_yuan     > 0, cost_yuan,     NULL), 0.05)     AS cost_yuan_p5,
        percentile_approx(IF(cost_yuan     > 0, cost_yuan,     NULL), 0.95)     AS cost_yuan_p95,
        percentile_approx(IF(spam_cost_yuan > 0, spam_cost_yuan, NULL), 0.05)   AS spam_cost_p5,
        percentile_approx(IF(spam_cost_yuan > 0, spam_cost_yuan, NULL), 0.95)   AS spam_cost_p95,
        percentile_approx(IF(total_cost_yuan > 0, total_cost_yuan, NULL), 0.05) AS total_cost_p5,
        percentile_approx(IF(total_cost_yuan > 0, total_cost_yuan, NULL), 0.95) AS total_cost_p95,
        percentile_approx(IF(roi           > 0, roi,           NULL), 0.05)     AS roi_p5,
        percentile_approx(IF(roi           > 0, roi,           NULL), 0.95)     AS roi_p95,
        percentile_approx(IF(roi_total     > 0, roi_total,     NULL), 0.05)     AS roi_total_p5,
        percentile_approx(IF(roi_total     > 0, roi_total,     NULL), 0.95)     AS roi_total_p95
    FROM uv_metrics
),

binned AS (
    -- 1. gmv
    SELECT 'gmv' AS metric, visitor_id,
        CASE
            WHEN gmv <= 0 OR gmv IS NULL THEN NULL
            WHEN gmv <= p.gmv_p5                                        THEN CONCAT('Bin01_(0,',ROUND(p.gmv_p5,2),']')
            WHEN gmv <= p.gmv_p5 + 1*(p.gmv_p95-p.gmv_p5)/8           THEN CONCAT('Bin02_(',ROUND(p.gmv_p5,2),',',ROUND(p.gmv_p5+1*(p.gmv_p95-p.gmv_p5)/8,2),']')
            WHEN gmv <= p.gmv_p5 + 2*(p.gmv_p95-p.gmv_p5)/8           THEN CONCAT('Bin03_(',ROUND(p.gmv_p5+1*(p.gmv_p95-p.gmv_p5)/8,2),',',ROUND(p.gmv_p5+2*(p.gmv_p95-p.gmv_p5)/8,2),']')
            WHEN gmv <= p.gmv_p5 + 3*(p.gmv_p95-p.gmv_p5)/8           THEN CONCAT('Bin04_(',ROUND(p.gmv_p5+2*(p.gmv_p95-p.gmv_p5)/8,2),',',ROUND(p.gmv_p5+3*(p.gmv_p95-p.gmv_p5)/8,2),']')
            WHEN gmv <= p.gmv_p5 + 4*(p.gmv_p95-p.gmv_p5)/8           THEN CONCAT('Bin05_(',ROUND(p.gmv_p5+3*(p.gmv_p95-p.gmv_p5)/8,2),',',ROUND(p.gmv_p5+4*(p.gmv_p95-p.gmv_p5)/8,2),']')
            WHEN gmv <= p.gmv_p5 + 5*(p.gmv_p95-p.gmv_p5)/8           THEN CONCAT('Bin06_(',ROUND(p.gmv_p5+4*(p.gmv_p95-p.gmv_p5)/8,2),',',ROUND(p.gmv_p5+5*(p.gmv_p95-p.gmv_p5)/8,2),']')
            WHEN gmv <= p.gmv_p5 + 6*(p.gmv_p95-p.gmv_p5)/8           THEN CONCAT('Bin07_(',ROUND(p.gmv_p5+5*(p.gmv_p95-p.gmv_p5)/8,2),',',ROUND(p.gmv_p5+6*(p.gmv_p95-p.gmv_p5)/8,2),']')
            WHEN gmv <= p.gmv_p5 + 7*(p.gmv_p95-p.gmv_p5)/8           THEN CONCAT('Bin08_(',ROUND(p.gmv_p5+6*(p.gmv_p95-p.gmv_p5)/8,2),',',ROUND(p.gmv_p5+7*(p.gmv_p95-p.gmv_p5)/8,2),']')
            WHEN gmv <= p.gmv_p95                                       THEN CONCAT('Bin09_(',ROUND(p.gmv_p5+7*(p.gmv_p95-p.gmv_p5)/8,2),',',ROUND(p.gmv_p95,2),']')
            ELSE                                                             CONCAT('Bin10_(',ROUND(p.gmv_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 2. refund_gmv
    SELECT 'refund_gmv' AS metric, visitor_id,
        CASE
            WHEN refund_gmv <= 0 OR refund_gmv IS NULL THEN NULL
            WHEN refund_gmv <= p.refund_gmv_p5                                                              THEN CONCAT('Bin01_(0,',ROUND(p.refund_gmv_p5,2),']')
            WHEN refund_gmv <= p.refund_gmv_p5 + 1*(p.refund_gmv_p95-p.refund_gmv_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.refund_gmv_p5,2),',',ROUND(p.refund_gmv_p5+1*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),']')
            WHEN refund_gmv <= p.refund_gmv_p5 + 2*(p.refund_gmv_p95-p.refund_gmv_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.refund_gmv_p5+1*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),',',ROUND(p.refund_gmv_p5+2*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),']')
            WHEN refund_gmv <= p.refund_gmv_p5 + 3*(p.refund_gmv_p95-p.refund_gmv_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.refund_gmv_p5+2*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),',',ROUND(p.refund_gmv_p5+3*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),']')
            WHEN refund_gmv <= p.refund_gmv_p5 + 4*(p.refund_gmv_p95-p.refund_gmv_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.refund_gmv_p5+3*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),',',ROUND(p.refund_gmv_p5+4*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),']')
            WHEN refund_gmv <= p.refund_gmv_p5 + 5*(p.refund_gmv_p95-p.refund_gmv_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.refund_gmv_p5+4*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),',',ROUND(p.refund_gmv_p5+5*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),']')
            WHEN refund_gmv <= p.refund_gmv_p5 + 6*(p.refund_gmv_p95-p.refund_gmv_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.refund_gmv_p5+5*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),',',ROUND(p.refund_gmv_p5+6*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),']')
            WHEN refund_gmv <= p.refund_gmv_p5 + 7*(p.refund_gmv_p95-p.refund_gmv_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.refund_gmv_p5+6*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),',',ROUND(p.refund_gmv_p5+7*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),']')
            WHEN refund_gmv <= p.refund_gmv_p95                                                             THEN CONCAT('Bin09_(',ROUND(p.refund_gmv_p5+7*(p.refund_gmv_p95-p.refund_gmv_p5)/8,2),',',ROUND(p.refund_gmv_p95,2),']')
            ELSE                                                                                                  CONCAT('Bin10_(',ROUND(p.refund_gmv_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 3. order_cnt
    SELECT 'order_cnt' AS metric, visitor_id,
        CASE
            WHEN order_cnt <= 0 OR order_cnt IS NULL THEN NULL
            WHEN order_cnt <= p.order_cnt_p5                                                                THEN CONCAT('Bin01_(0,',ROUND(p.order_cnt_p5,2),']')
            WHEN order_cnt <= p.order_cnt_p5 + 1*(p.order_cnt_p95-p.order_cnt_p5)/8                        THEN CONCAT('Bin02_(',ROUND(p.order_cnt_p5,2),',',ROUND(p.order_cnt_p5+1*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN order_cnt <= p.order_cnt_p5 + 2*(p.order_cnt_p95-p.order_cnt_p5)/8                        THEN CONCAT('Bin03_(',ROUND(p.order_cnt_p5+1*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+2*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN order_cnt <= p.order_cnt_p5 + 3*(p.order_cnt_p95-p.order_cnt_p5)/8                        THEN CONCAT('Bin04_(',ROUND(p.order_cnt_p5+2*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+3*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN order_cnt <= p.order_cnt_p5 + 4*(p.order_cnt_p95-p.order_cnt_p5)/8                        THEN CONCAT('Bin05_(',ROUND(p.order_cnt_p5+3*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+4*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN order_cnt <= p.order_cnt_p5 + 5*(p.order_cnt_p95-p.order_cnt_p5)/8                        THEN CONCAT('Bin06_(',ROUND(p.order_cnt_p5+4*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+5*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN order_cnt <= p.order_cnt_p5 + 6*(p.order_cnt_p95-p.order_cnt_p5)/8                        THEN CONCAT('Bin07_(',ROUND(p.order_cnt_p5+5*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+6*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN order_cnt <= p.order_cnt_p5 + 7*(p.order_cnt_p95-p.order_cnt_p5)/8                        THEN CONCAT('Bin08_(',ROUND(p.order_cnt_p5+6*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p5+7*(p.order_cnt_p95-p.order_cnt_p5)/8,2),']')
            WHEN order_cnt <= p.order_cnt_p95                                                               THEN CONCAT('Bin09_(',ROUND(p.order_cnt_p5+7*(p.order_cnt_p95-p.order_cnt_p5)/8,2),',',ROUND(p.order_cnt_p95,2),']')
            ELSE                                                                                                  CONCAT('Bin10_(',ROUND(p.order_cnt_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 4. refund_cnt
    SELECT 'refund_cnt' AS metric, visitor_id,
        CASE
            WHEN refund_cnt <= 0 OR refund_cnt IS NULL THEN NULL
            WHEN refund_cnt <= p.refund_cnt_p5                                                              THEN CONCAT('Bin01_(0,',ROUND(p.refund_cnt_p5,2),']')
            WHEN refund_cnt <= p.refund_cnt_p5 + 1*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.refund_cnt_p5,2),',',ROUND(p.refund_cnt_p5+1*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN refund_cnt <= p.refund_cnt_p5 + 2*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.refund_cnt_p5+1*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+2*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN refund_cnt <= p.refund_cnt_p5 + 3*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.refund_cnt_p5+2*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+3*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN refund_cnt <= p.refund_cnt_p5 + 4*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.refund_cnt_p5+3*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+4*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN refund_cnt <= p.refund_cnt_p5 + 5*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.refund_cnt_p5+4*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+5*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN refund_cnt <= p.refund_cnt_p5 + 6*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.refund_cnt_p5+5*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+6*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN refund_cnt <= p.refund_cnt_p5 + 7*(p.refund_cnt_p95-p.refund_cnt_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.refund_cnt_p5+6*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p5+7*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),']')
            WHEN refund_cnt <= p.refund_cnt_p95                                                             THEN CONCAT('Bin09_(',ROUND(p.refund_cnt_p5+7*(p.refund_cnt_p95-p.refund_cnt_p5)/8,2),',',ROUND(p.refund_cnt_p95,2),']')
            ELSE                                                                                                  CONCAT('Bin10_(',ROUND(p.refund_cnt_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 5. avg_price
    SELECT 'avg_price' AS metric, visitor_id,
        CASE
            WHEN avg_price <= 0 OR avg_price IS NULL THEN NULL
            WHEN avg_price <= p.avg_price_p5                                                                THEN CONCAT('Bin01_(0,',ROUND(p.avg_price_p5,2),']')
            WHEN avg_price <= p.avg_price_p5 + 1*(p.avg_price_p95-p.avg_price_p5)/8                        THEN CONCAT('Bin02_(',ROUND(p.avg_price_p5,2),',',ROUND(p.avg_price_p5+1*(p.avg_price_p95-p.avg_price_p5)/8,2),']')
            WHEN avg_price <= p.avg_price_p5 + 2*(p.avg_price_p95-p.avg_price_p5)/8                        THEN CONCAT('Bin03_(',ROUND(p.avg_price_p5+1*(p.avg_price_p95-p.avg_price_p5)/8,2),',',ROUND(p.avg_price_p5+2*(p.avg_price_p95-p.avg_price_p5)/8,2),']')
            WHEN avg_price <= p.avg_price_p5 + 3*(p.avg_price_p95-p.avg_price_p5)/8                        THEN CONCAT('Bin04_(',ROUND(p.avg_price_p5+2*(p.avg_price_p95-p.avg_price_p5)/8,2),',',ROUND(p.avg_price_p5+3*(p.avg_price_p95-p.avg_price_p5)/8,2),']')
            WHEN avg_price <= p.avg_price_p5 + 4*(p.avg_price_p95-p.avg_price_p5)/8                        THEN CONCAT('Bin05_(',ROUND(p.avg_price_p5+3*(p.avg_price_p95-p.avg_price_p5)/8,2),',',ROUND(p.avg_price_p5+4*(p.avg_price_p95-p.avg_price_p5)/8,2),']')
            WHEN avg_price <= p.avg_price_p5 + 5*(p.avg_price_p95-p.avg_price_p5)/8                        THEN CONCAT('Bin06_(',ROUND(p.avg_price_p5+4*(p.avg_price_p95-p.avg_price_p5)/8,2),',',ROUND(p.avg_price_p5+5*(p.avg_price_p95-p.avg_price_p5)/8,2),']')
            WHEN avg_price <= p.avg_price_p5 + 6*(p.avg_price_p95-p.avg_price_p5)/8                        THEN CONCAT('Bin07_(',ROUND(p.avg_price_p5+5*(p.avg_price_p95-p.avg_price_p5)/8,2),',',ROUND(p.avg_price_p5+6*(p.avg_price_p95-p.avg_price_p5)/8,2),']')
            WHEN avg_price <= p.avg_price_p5 + 7*(p.avg_price_p95-p.avg_price_p5)/8                        THEN CONCAT('Bin08_(',ROUND(p.avg_price_p5+6*(p.avg_price_p95-p.avg_price_p5)/8,2),',',ROUND(p.avg_price_p5+7*(p.avg_price_p95-p.avg_price_p5)/8,2),']')
            WHEN avg_price <= p.avg_price_p95                                                               THEN CONCAT('Bin09_(',ROUND(p.avg_price_p5+7*(p.avg_price_p95-p.avg_price_p5)/8,2),',',ROUND(p.avg_price_p95,2),']')
            ELSE                                                                                                  CONCAT('Bin10_(',ROUND(p.avg_price_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 6. avg_net_price
    SELECT 'avg_net_price' AS metric, visitor_id,
        CASE
            WHEN avg_net_price <= 0 OR avg_net_price IS NULL THEN NULL
            WHEN avg_net_price <= p.avg_net_price_p5                                                                    THEN CONCAT('Bin01_(0,',ROUND(p.avg_net_price_p5,2),']')
            WHEN avg_net_price <= p.avg_net_price_p5 + 1*(p.avg_net_price_p95-p.avg_net_price_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.avg_net_price_p5,2),',',ROUND(p.avg_net_price_p5+1*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),']')
            WHEN avg_net_price <= p.avg_net_price_p5 + 2*(p.avg_net_price_p95-p.avg_net_price_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.avg_net_price_p5+1*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),',',ROUND(p.avg_net_price_p5+2*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),']')
            WHEN avg_net_price <= p.avg_net_price_p5 + 3*(p.avg_net_price_p95-p.avg_net_price_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.avg_net_price_p5+2*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),',',ROUND(p.avg_net_price_p5+3*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),']')
            WHEN avg_net_price <= p.avg_net_price_p5 + 4*(p.avg_net_price_p95-p.avg_net_price_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.avg_net_price_p5+3*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),',',ROUND(p.avg_net_price_p5+4*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),']')
            WHEN avg_net_price <= p.avg_net_price_p5 + 5*(p.avg_net_price_p95-p.avg_net_price_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.avg_net_price_p5+4*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),',',ROUND(p.avg_net_price_p5+5*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),']')
            WHEN avg_net_price <= p.avg_net_price_p5 + 6*(p.avg_net_price_p95-p.avg_net_price_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.avg_net_price_p5+5*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),',',ROUND(p.avg_net_price_p5+6*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),']')
            WHEN avg_net_price <= p.avg_net_price_p5 + 7*(p.avg_net_price_p95-p.avg_net_price_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.avg_net_price_p5+6*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),',',ROUND(p.avg_net_price_p5+7*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),']')
            WHEN avg_net_price <= p.avg_net_price_p95                                                                   THEN CONCAT('Bin09_(',ROUND(p.avg_net_price_p5+7*(p.avg_net_price_p95-p.avg_net_price_p5)/8,2),',',ROUND(p.avg_net_price_p95,2),']')
            ELSE                                                                                                              CONCAT('Bin10_(',ROUND(p.avg_net_price_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 7. refund_price
    SELECT 'refund_price' AS metric, visitor_id,
        CASE
            WHEN refund_price <= 0 OR refund_price IS NULL THEN NULL
            WHEN refund_price <= p.refund_price_p5                                                                  THEN CONCAT('Bin01_(0,',ROUND(p.refund_price_p5,2),']')
            WHEN refund_price <= p.refund_price_p5 + 1*(p.refund_price_p95-p.refund_price_p5)/8                    THEN CONCAT('Bin02_(',ROUND(p.refund_price_p5,2),',',ROUND(p.refund_price_p5+1*(p.refund_price_p95-p.refund_price_p5)/8,2),']')
            WHEN refund_price <= p.refund_price_p5 + 2*(p.refund_price_p95-p.refund_price_p5)/8                    THEN CONCAT('Bin03_(',ROUND(p.refund_price_p5+1*(p.refund_price_p95-p.refund_price_p5)/8,2),',',ROUND(p.refund_price_p5+2*(p.refund_price_p95-p.refund_price_p5)/8,2),']')
            WHEN refund_price <= p.refund_price_p5 + 3*(p.refund_price_p95-p.refund_price_p5)/8                    THEN CONCAT('Bin04_(',ROUND(p.refund_price_p5+2*(p.refund_price_p95-p.refund_price_p5)/8,2),',',ROUND(p.refund_price_p5+3*(p.refund_price_p95-p.refund_price_p5)/8,2),']')
            WHEN refund_price <= p.refund_price_p5 + 4*(p.refund_price_p95-p.refund_price_p5)/8                    THEN CONCAT('Bin05_(',ROUND(p.refund_price_p5+3*(p.refund_price_p95-p.refund_price_p5)/8,2),',',ROUND(p.refund_price_p5+4*(p.refund_price_p95-p.refund_price_p5)/8,2),']')
            WHEN refund_price <= p.refund_price_p5 + 5*(p.refund_price_p95-p.refund_price_p5)/8                    THEN CONCAT('Bin06_(',ROUND(p.refund_price_p5+4*(p.refund_price_p95-p.refund_price_p5)/8,2),',',ROUND(p.refund_price_p5+5*(p.refund_price_p95-p.refund_price_p5)/8,2),']')
            WHEN refund_price <= p.refund_price_p5 + 6*(p.refund_price_p95-p.refund_price_p5)/8                    THEN CONCAT('Bin07_(',ROUND(p.refund_price_p5+5*(p.refund_price_p95-p.refund_price_p5)/8,2),',',ROUND(p.refund_price_p5+6*(p.refund_price_p95-p.refund_price_p5)/8,2),']')
            WHEN refund_price <= p.refund_price_p5 + 7*(p.refund_price_p95-p.refund_price_p5)/8                    THEN CONCAT('Bin08_(',ROUND(p.refund_price_p5+6*(p.refund_price_p95-p.refund_price_p5)/8,2),',',ROUND(p.refund_price_p5+7*(p.refund_price_p95-p.refund_price_p5)/8,2),']')
            WHEN refund_price <= p.refund_price_p95                                                                 THEN CONCAT('Bin09_(',ROUND(p.refund_price_p5+7*(p.refund_price_p95-p.refund_price_p5)/8,2),',',ROUND(p.refund_price_p95,2),']')
            ELSE                                                                                                          CONCAT('Bin10_(',ROUND(p.refund_price_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 8. cost_yuan
    SELECT 'cost_yuan' AS metric, visitor_id,
        CASE
            WHEN cost_yuan <= 0 OR cost_yuan IS NULL THEN NULL
            WHEN cost_yuan <= p.cost_yuan_p5                                                                THEN CONCAT('Bin01_(0,',ROUND(p.cost_yuan_p5,2),']')
            WHEN cost_yuan <= p.cost_yuan_p5 + 1*(p.cost_yuan_p95-p.cost_yuan_p5)/8                        THEN CONCAT('Bin02_(',ROUND(p.cost_yuan_p5,2),',',ROUND(p.cost_yuan_p5+1*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),']')
            WHEN cost_yuan <= p.cost_yuan_p5 + 2*(p.cost_yuan_p95-p.cost_yuan_p5)/8                        THEN CONCAT('Bin03_(',ROUND(p.cost_yuan_p5+1*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),',',ROUND(p.cost_yuan_p5+2*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),']')
            WHEN cost_yuan <= p.cost_yuan_p5 + 3*(p.cost_yuan_p95-p.cost_yuan_p5)/8                        THEN CONCAT('Bin04_(',ROUND(p.cost_yuan_p5+2*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),',',ROUND(p.cost_yuan_p5+3*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),']')
            WHEN cost_yuan <= p.cost_yuan_p5 + 4*(p.cost_yuan_p95-p.cost_yuan_p5)/8                        THEN CONCAT('Bin05_(',ROUND(p.cost_yuan_p5+3*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),',',ROUND(p.cost_yuan_p5+4*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),']')
            WHEN cost_yuan <= p.cost_yuan_p5 + 5*(p.cost_yuan_p95-p.cost_yuan_p5)/8                        THEN CONCAT('Bin06_(',ROUND(p.cost_yuan_p5+4*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),',',ROUND(p.cost_yuan_p5+5*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),']')
            WHEN cost_yuan <= p.cost_yuan_p5 + 6*(p.cost_yuan_p95-p.cost_yuan_p5)/8                        THEN CONCAT('Bin07_(',ROUND(p.cost_yuan_p5+5*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),',',ROUND(p.cost_yuan_p5+6*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),']')
            WHEN cost_yuan <= p.cost_yuan_p5 + 7*(p.cost_yuan_p95-p.cost_yuan_p5)/8                        THEN CONCAT('Bin08_(',ROUND(p.cost_yuan_p5+6*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),',',ROUND(p.cost_yuan_p5+7*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),']')
            WHEN cost_yuan <= p.cost_yuan_p95                                                               THEN CONCAT('Bin09_(',ROUND(p.cost_yuan_p5+7*(p.cost_yuan_p95-p.cost_yuan_p5)/8,2),',',ROUND(p.cost_yuan_p95,2),']')
            ELSE                                                                                                  CONCAT('Bin10_(',ROUND(p.cost_yuan_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 9. spam_cost_yuan
    SELECT 'spam_cost_yuan' AS metric, visitor_id,
        CASE
            WHEN spam_cost_yuan <= 0 OR spam_cost_yuan IS NULL THEN NULL
            WHEN spam_cost_yuan <= p.spam_cost_p5                                                               THEN CONCAT('Bin01_(0,',ROUND(p.spam_cost_p5,2),']')
            WHEN spam_cost_yuan <= p.spam_cost_p5 + 1*(p.spam_cost_p95-p.spam_cost_p5)/8                       THEN CONCAT('Bin02_(',ROUND(p.spam_cost_p5,2),',',ROUND(p.spam_cost_p5+1*(p.spam_cost_p95-p.spam_cost_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_cost_p5 + 2*(p.spam_cost_p95-p.spam_cost_p5)/8                       THEN CONCAT('Bin03_(',ROUND(p.spam_cost_p5+1*(p.spam_cost_p95-p.spam_cost_p5)/8,2),',',ROUND(p.spam_cost_p5+2*(p.spam_cost_p95-p.spam_cost_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_cost_p5 + 3*(p.spam_cost_p95-p.spam_cost_p5)/8                       THEN CONCAT('Bin04_(',ROUND(p.spam_cost_p5+2*(p.spam_cost_p95-p.spam_cost_p5)/8,2),',',ROUND(p.spam_cost_p5+3*(p.spam_cost_p95-p.spam_cost_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_cost_p5 + 4*(p.spam_cost_p95-p.spam_cost_p5)/8                       THEN CONCAT('Bin05_(',ROUND(p.spam_cost_p5+3*(p.spam_cost_p95-p.spam_cost_p5)/8,2),',',ROUND(p.spam_cost_p5+4*(p.spam_cost_p95-p.spam_cost_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_cost_p5 + 5*(p.spam_cost_p95-p.spam_cost_p5)/8                       THEN CONCAT('Bin06_(',ROUND(p.spam_cost_p5+4*(p.spam_cost_p95-p.spam_cost_p5)/8,2),',',ROUND(p.spam_cost_p5+5*(p.spam_cost_p95-p.spam_cost_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_cost_p5 + 6*(p.spam_cost_p95-p.spam_cost_p5)/8                       THEN CONCAT('Bin07_(',ROUND(p.spam_cost_p5+5*(p.spam_cost_p95-p.spam_cost_p5)/8,2),',',ROUND(p.spam_cost_p5+6*(p.spam_cost_p95-p.spam_cost_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_cost_p5 + 7*(p.spam_cost_p95-p.spam_cost_p5)/8                       THEN CONCAT('Bin08_(',ROUND(p.spam_cost_p5+6*(p.spam_cost_p95-p.spam_cost_p5)/8,2),',',ROUND(p.spam_cost_p5+7*(p.spam_cost_p95-p.spam_cost_p5)/8,2),']')
            WHEN spam_cost_yuan <= p.spam_cost_p95                                                              THEN CONCAT('Bin09_(',ROUND(p.spam_cost_p5+7*(p.spam_cost_p95-p.spam_cost_p5)/8,2),',',ROUND(p.spam_cost_p95,2),']')
            ELSE                                                                                                     CONCAT('Bin10_(',ROUND(p.spam_cost_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 10. total_cost_yuan
    SELECT 'total_cost_yuan' AS metric, visitor_id,
        CASE
            WHEN total_cost_yuan <= 0 OR total_cost_yuan IS NULL THEN NULL
            WHEN total_cost_yuan <= p.total_cost_p5                                                             THEN CONCAT('Bin01_(0,',ROUND(p.total_cost_p5,2),']')
            WHEN total_cost_yuan <= p.total_cost_p5 + 1*(p.total_cost_p95-p.total_cost_p5)/8                   THEN CONCAT('Bin02_(',ROUND(p.total_cost_p5,2),',',ROUND(p.total_cost_p5+1*(p.total_cost_p95-p.total_cost_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_cost_p5 + 2*(p.total_cost_p95-p.total_cost_p5)/8                   THEN CONCAT('Bin03_(',ROUND(p.total_cost_p5+1*(p.total_cost_p95-p.total_cost_p5)/8,2),',',ROUND(p.total_cost_p5+2*(p.total_cost_p95-p.total_cost_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_cost_p5 + 3*(p.total_cost_p95-p.total_cost_p5)/8                   THEN CONCAT('Bin04_(',ROUND(p.total_cost_p5+2*(p.total_cost_p95-p.total_cost_p5)/8,2),',',ROUND(p.total_cost_p5+3*(p.total_cost_p95-p.total_cost_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_cost_p5 + 4*(p.total_cost_p95-p.total_cost_p5)/8                   THEN CONCAT('Bin05_(',ROUND(p.total_cost_p5+3*(p.total_cost_p95-p.total_cost_p5)/8,2),',',ROUND(p.total_cost_p5+4*(p.total_cost_p95-p.total_cost_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_cost_p5 + 5*(p.total_cost_p95-p.total_cost_p5)/8                   THEN CONCAT('Bin06_(',ROUND(p.total_cost_p5+4*(p.total_cost_p95-p.total_cost_p5)/8,2),',',ROUND(p.total_cost_p5+5*(p.total_cost_p95-p.total_cost_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_cost_p5 + 6*(p.total_cost_p95-p.total_cost_p5)/8                   THEN CONCAT('Bin07_(',ROUND(p.total_cost_p5+5*(p.total_cost_p95-p.total_cost_p5)/8,2),',',ROUND(p.total_cost_p5+6*(p.total_cost_p95-p.total_cost_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_cost_p5 + 7*(p.total_cost_p95-p.total_cost_p5)/8                   THEN CONCAT('Bin08_(',ROUND(p.total_cost_p5+6*(p.total_cost_p95-p.total_cost_p5)/8,2),',',ROUND(p.total_cost_p5+7*(p.total_cost_p95-p.total_cost_p5)/8,2),']')
            WHEN total_cost_yuan <= p.total_cost_p95                                                            THEN CONCAT('Bin09_(',ROUND(p.total_cost_p5+7*(p.total_cost_p95-p.total_cost_p5)/8,2),',',ROUND(p.total_cost_p95,2),']')
            ELSE                                                                                                     CONCAT('Bin10_(',ROUND(p.total_cost_p95,2),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 11. roi
    SELECT 'roi' AS metric, visitor_id,
        CASE
            WHEN roi <= 0 OR roi IS NULL THEN NULL
            WHEN roi <= p.roi_p5                                        THEN CONCAT('Bin01_(0,',ROUND(p.roi_p5,4),']')
            WHEN roi <= p.roi_p5 + 1*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin02_(',ROUND(p.roi_p5,4),',',ROUND(p.roi_p5+1*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN roi <= p.roi_p5 + 2*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin03_(',ROUND(p.roi_p5+1*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+2*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN roi <= p.roi_p5 + 3*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin04_(',ROUND(p.roi_p5+2*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+3*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN roi <= p.roi_p5 + 4*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin05_(',ROUND(p.roi_p5+3*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+4*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN roi <= p.roi_p5 + 5*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin06_(',ROUND(p.roi_p5+4*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+5*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN roi <= p.roi_p5 + 6*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin07_(',ROUND(p.roi_p5+5*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+6*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN roi <= p.roi_p5 + 7*(p.roi_p95-p.roi_p5)/8           THEN CONCAT('Bin08_(',ROUND(p.roi_p5+6*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p5+7*(p.roi_p95-p.roi_p5)/8,4),']')
            WHEN roi <= p.roi_p95                                       THEN CONCAT('Bin09_(',ROUND(p.roi_p5+7*(p.roi_p95-p.roi_p5)/8,4),',',ROUND(p.roi_p95,4),']')
            ELSE                                                             CONCAT('Bin10_(',ROUND(p.roi_p95,4),',+inf)')
        END AS bin_label
    FROM uv_metrics JOIN percentiles p ON uv_metrics.join_key = p.join_key

    UNION ALL

    -- 12. roi_total
    SELECT 'roi_total' AS metric, visitor_id,
        CASE
            WHEN roi_total <= 0 OR roi_total IS NULL THEN NULL
            WHEN roi_total <= p.roi_total_p5                                                                THEN CONCAT('Bin01_(0,',ROUND(p.roi_total_p5,4),']')
            WHEN roi_total <= p.roi_total_p5 + 1*(p.roi_total_p95-p.roi_total_p5)/8                        THEN CONCAT('Bin02_(',ROUND(p.roi_total_p5,4),',',ROUND(p.roi_total_p5+1*(p.roi_total_p95-p.roi_total_p5)/8,4),']')
            WHEN roi_total <= p.roi_total_p5 + 2*(p.roi_total_p95-p.roi_total_p5)/8                        THEN CONCAT('Bin03_(',ROUND(p.roi_total_p5+1*(p.roi_total_p95-p.roi_total_p5)/8,4),',',ROUND(p.roi_total_p5+2*(p.roi_total_p95-p.roi_total_p5)/8,4),']')
            WHEN roi_total <= p.roi_total_p5 + 3*(p.roi_total_p95-p.roi_total_p5)/8                        THEN CONCAT('Bin04_(',ROUND(p.roi_total_p5+2*(p.roi_total_p95-p.roi_total_p5)/8,4),',',ROUND(p.roi_total_p5+3*(p.roi_total_p95-p.roi_total_p5)/8,4),']')
            WHEN roi_total <= p.roi_total_p5 + 4*(p.roi_total_p95-p.roi_total_p5)/8                        THEN CONCAT('Bin05_(',ROUND(p.roi_total_p5+3*(p.roi_total_p95-p.roi_total_p5)/8,4),',',ROUND(p.roi_total_p5+4*(p.roi_total_p95-p.roi_total_p5)/8,4),']')
            WHEN roi_total <= p.roi_total_p5 + 5*(p.roi_total_p95-p.roi_total_p5)/8                        THEN CONCAT('Bin06_(',ROUND(p.roi_total_p5+4*(p.roi_total_p95-p.roi_total_p5)/8,4),',',ROUND(p.roi_total_p5+5*(p.roi_total_p95-p.roi_total_p5)/8,4),']')
            WHEN roi_total <= p.roi_total_p5 + 6*(p.roi_total_p95-p.roi_total_p5)/8                        THEN CONCAT('Bin07_(',ROUND(p.roi_total_p5+5*(p.roi_total_p95-p.roi_total_p5)/8,4),',',ROUND(p.roi_total_p5+6*(p.roi_total_p95-p.roi_total_p5)/8,4),']')
            WHEN roi_total <= p.roi_total_p5 + 7*(p.roi_total_p95-p.roi_total_p5)/8                        THEN CONCAT('Bin08_(',ROUND(p.roi_total_p5+6*(p.roi_total_p95-p.roi_total_p5)/8,4),',',ROUND(p.roi_total_p5+7*(p.roi_total_p95-p.roi_total_p5)/8,4),']')
            WHEN roi_total <= p.roi_total_p95                                                               THEN CONCAT('Bin09_(',ROUND(p.roi_total_p5+7*(p.roi_total_p95-p.roi_total_p5)/8,4),',',ROUND(p.roi_total_p95,4),']')
            ELSE                                                                                                  CONCAT('Bin10_(',ROUND(p.roi_total_p95,4),',+inf)')
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
