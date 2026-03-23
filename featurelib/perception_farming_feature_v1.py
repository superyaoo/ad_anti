#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@File  : perception_farming_feature_v1.py
@Author: zengjunyao(@kuaishou.com)
@Date  : 2026-03-23
@Desc  : 养机羊毛党感知 - 特征工程 V1（探查版）
         样本来源：本地 CSV（圈差后效样本），筛选 hit_score==6 的用户（约2.28万）
         特征维度与 perception_farming_feature.py 完全一致：
           feat1 / tag1：搜索行为
           feat2 / tag2：观播/视频行为
           feat3 / tag3：社交行为
           feat4 / tag4：订单交易
           feat5      ：开播辅助特征
         输出：
           特征宽表：featurelib/farming_feat_wide_v1_{end_date}.csv
           异常用户：featurelib/farming_anomaly_v1_{end_date}.csv
         注意：所有 SQL 均为纯嵌套子查询，无任何 WITH CTE，
               避免 kmlutils INSERT OVERWRITE 包装后 Hive 报
               "cannot recognize input near '(' 'WITH'" 的问题。
"""
import os
os.environ['HADOOP_USER_NAME'] = 'ad_antispam'
import sys
import datetime
import pandas as pd
from kmlutils.kml_hive import Hive

CSV_PATH = '圈差后效样本_Snippet 1_30113910.csv'


def get_date_range(end_date_str, days=7):
    end = datetime.datetime.strptime(end_date_str, '%Y%m%d')
    start = end - datetime.timedelta(days=days - 1)
    return start.strftime('%Y%m%d'), end.strftime('%Y%m%d')


# ─────────────────────────────────────────────────────────────
# feat1: 搜索行为特征（纯嵌套子查询版，无 CTE）
# ─────────────────────────────────────────────────────────────
def build_feat1_sql(start_date, end_date, sample_table, cold_split_date):
    return f"""
SELECT
  sa.user_id,
  sa.search_cnt_7d,
  sa.search_day_cnt_7d,
  sa.search_session_cnt_7d,
  ROUND(sa.search_cnt_7d * 1.0 / NULLIF(sa.search_day_cnt_7d, 0), 2)  AS avg_daily_search_cnt,
  ROUND(ec.search_entropy_7d, 4)                                        AS search_entropy_7d,
  ROUND(ec.top1_query_ratio, 4)                                         AS top1_query_ratio,
  ec.high_value_query_cnt,
  ROUND(ec.high_value_query_ratio, 4)                                   AS high_value_query_ratio,
  ROUND(sr.search_sug_ratio, 4)                                         AS search_sug_ratio,
  COALESCE(cs.cold_start_flag, 0)                                       AS cold_start_flag,
  CASE
    WHEN (
      (CASE WHEN ec.search_entropy_7d < 1.5             THEN 1 ELSE 0 END)
    + (CASE WHEN ec.high_value_query_ratio >= 0.6       THEN 1 ELSE 0 END)
    + (CASE WHEN ec.top1_query_ratio >= 0.4             THEN 1 ELSE 0 END)
    + (CASE WHEN sr.search_sug_ratio >= 0.7             THEN 1 ELSE 0 END)
    + (CASE WHEN COALESCE(cs.cold_start_flag, 0) = 1   THEN 1 ELSE 0 END)
    ) >= 2
    THEN 1 ELSE 0
  END                                                                   AS tag1
FROM (
  SELECT
    user_id,
    COUNT(*)                       AS search_cnt_7d,
    COUNT(DISTINCT p_date)         AS search_day_cnt_7d,
    COUNT(DISTINCT search_session_id) AS search_session_cnt_7d
  FROM (
    SELECT s.user_id, s.p_date, s.search_session_id
    FROM kscdm.dwd_ks_search_show_diversion_di s
    INNER JOIN (SELECT CAST(visitor_id AS BIGINT) AS uid FROM {sample_table}) smp
      ON s.user_id = smp.uid
    WHERE s.p_date >= '{start_date}' AND s.p_date <= '{end_date}'
      AND s.item_type = 'QUERY'
      AND s.product IN ('KUAISHOU', 'NEBULA')
  ) raw
  GROUP BY user_id
) sa
LEFT JOIN (
  SELECT
    qf.user_id,
    -SUM((qf.qcnt * 1.0 / qt.total_qcnt) * LN(qf.qcnt * 1.0 / qt.total_qcnt)) AS search_entropy_7d,
    MAX(qf.qcnt * 1.0 / qt.total_qcnt)                                          AS top1_query_ratio,
    SUM(CASE WHEN qf.query_word IN (
          '购物','优惠','领券','红包','赚钱','金币','提现','签到','任务',
          '免费领','试用','拼多多','淘宝','京东','抢购','秒杀','福利',
          '羊毛','白嫖','薅','每日签到','做任务','赚','免费')
         THEN qf.qcnt ELSE 0 END)                                                AS high_value_query_cnt,
    SUM(CASE WHEN qf.query_word IN (
          '购物','优惠','领券','红包','赚钱','金币','提现','签到','任务',
          '免费领','试用','拼多多','淘宝','京东','抢购','秒杀','福利',
          '羊毛','白嫖','薅','每日签到','做任务','赚','免费')
         THEN qf.qcnt ELSE 0 END) * 1.0 / NULLIF(qt.total_qcnt, 0)              AS high_value_query_ratio
  FROM (
    SELECT user_id, item_id AS query_word, COUNT(*) AS qcnt
    FROM kscdm.dwd_ks_search_show_diversion_di
    WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
      AND item_type = 'QUERY'
      AND product IN ('KUAISHOU', 'NEBULA')
      AND user_id IN (SELECT CAST(visitor_id AS BIGINT) FROM {sample_table})
    GROUP BY user_id, item_id
  ) qf
  JOIN (
    SELECT user_id, SUM(qcnt) AS total_qcnt
    FROM (
      SELECT user_id, item_id AS query_word, COUNT(*) AS qcnt
      FROM kscdm.dwd_ks_search_show_diversion_di
      WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
        AND item_type = 'QUERY'
        AND product IN ('KUAISHOU', 'NEBULA')
        AND user_id IN (SELECT CAST(visitor_id AS BIGINT) FROM {sample_table})
      GROUP BY user_id, item_id
    ) qt_inner
    GROUP BY user_id
  ) qt ON qf.user_id = qt.user_id
  GROUP BY qf.user_id, qt.total_qcnt
) ec ON sa.user_id = ec.user_id
LEFT JOIN (
  SELECT
    user_id,
    SUM(CASE WHEN module = 'sug' THEN 1 ELSE 0 END) * 1.0
      / NULLIF(COUNT(*), 0)                                             AS search_sug_ratio
  FROM kscdm.dwd_ks_search_show_diversion_di
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND item_type = 'QUERY'
    AND product IN ('KUAISHOU', 'NEBULA')
    AND user_id IN (SELECT CAST(visitor_id AS BIGINT) FROM {sample_table})
  GROUP BY user_id
) sr ON sa.user_id = sr.user_id
LEFT JOIN (
  SELECT
    user_id,
    CASE
      WHEN COUNT(CASE WHEN p_date <= '{cold_split_date}' THEN 1 END) = 0
       AND COUNT(CASE WHEN p_date >  '{cold_split_date}' THEN 1 END) > 0
      THEN 1 ELSE 0
    END                                                                 AS cold_start_flag
  FROM kscdm.dwd_ks_search_show_diversion_di
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND item_type = 'QUERY'
    AND product IN ('KUAISHOU', 'NEBULA')
    AND user_id IN (SELECT CAST(visitor_id AS BIGINT) FROM {sample_table})
  GROUP BY user_id
) cs ON sa.user_id = cs.user_id
"""


# ─────────────────────────────────────────────────────────────
# feat2: 观播/视频行为特征（纯嵌套子查询版，无 CTE）
# ─────────────────────────────────────────────────────────────
def build_feat2_sql(start_date, end_date, sample_table):
    return f"""
SELECT
  COALESCE(la.user_id, pa.user_id)                                      AS user_id,
  COALESCE(la.live_play_cnt_7d, 0)                                      AS live_play_cnt_7d,
  COALESCE(la.live_play_duration_7d, 0)                                 AS live_play_duration_7d,
  COALESCE(la.live_play_day_cnt_7d, 0)                                  AS live_play_day_cnt_7d,
  COALESCE(la.avg_single_live_duration, 0)                              AS avg_single_live_duration,
  COALESCE(la.max_single_live_duration, 0)                              AS max_single_live_duration,
  COALESCE(dlm.daily_live_duration_max, 0)                              AS daily_live_duration_max,
  COALESCE(la.live_ad_play_ratio, 0)                                    AS live_ad_play_ratio,
  COALESCE(la.live_background_ratio, 0)                                 AS live_background_ratio,
  COALESCE(pa.photo_play_cnt_7d, 0)                                     AS photo_play_cnt_7d,
  COALESCE(pa.photo_play_duration_7d, 0)                                AS photo_play_duration_7d,
  COALESCE(pa.ad_photo_cnt_7d, 0)                                       AS ad_photo_cnt_7d,
  COALESCE(pa.ad_photo_ratio, 0)                                        AS ad_photo_ratio,
  COALESCE(pa.ad_complete_rate, 0)                                      AS ad_complete_rate,
  COALESCE(pa.non_ad_complete_rate, 0)                                  AS non_ad_complete_rate,
  ROUND(COALESCE(pa.ad_complete_rate, 0)
        / NULLIF(COALESCE(pa.non_ad_complete_rate, 0), 0), 4)           AS ad_vs_non_complete_ratio,
  CASE
    WHEN (
      (CASE WHEN COALESCE(dlm.daily_live_duration_max, 0) >= 18000      THEN 1 ELSE 0 END)
    + (CASE WHEN COALESCE(la.max_single_live_duration, 0) >= 7200       THEN 1 ELSE 0 END)
    + (CASE WHEN COALESCE(pa.ad_photo_ratio, 0) >= 0.7                  THEN 1 ELSE 0 END)
    + (CASE WHEN COALESCE(pa.ad_complete_rate, 0)
              / NULLIF(COALESCE(pa.non_ad_complete_rate, 0), 0) >= 2.0  THEN 1 ELSE 0 END)
    + (CASE WHEN COALESCE(la.live_ad_play_ratio, 0) >= 0.5              THEN 1 ELSE 0 END)
    ) >= 2
    THEN 1 ELSE 0
  END                                                                   AS tag2
FROM (
  SELECT
    user_id,
    COUNT(*)                                                            AS live_play_cnt_7d,
    ROUND(SUM(play_duration / 1000.0), 0)                               AS live_play_duration_7d,
    COUNT(DISTINCT p_date)                                              AS live_play_day_cnt_7d,
    ROUND(AVG(play_duration / 1000.0), 0)                               AS avg_single_live_duration,
    ROUND(MAX(play_duration / 1000.0), 0)                               AS max_single_live_duration,
    ROUND(SUM(CASE WHEN page_category = 'ADS' THEN play_duration / 1000.0 ELSE 0 END)
          / NULLIF(SUM(play_duration / 1000.0), 0), 4)                  AS live_ad_play_ratio,
    ROUND(SUM(background_duration / 1000.0)
          / NULLIF(SUM(play_duration / 1000.0), 0), 4)                  AS live_background_ratio
  FROM kscdm.dwd_ks_csm_play_live_di
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND product IN ('KUAISHOU', 'NEBULA')
    AND play_duration > 0
    AND user_id IN (SELECT CAST(visitor_id AS BIGINT) FROM {sample_table})
  GROUP BY user_id
) la
FULL OUTER JOIN (
  SELECT
    user_id,
    COUNT(*)                                                            AS photo_play_cnt_7d,
    ROUND(SUM(play_duration / 1000.0), 0)                               AS photo_play_duration_7d,
    SUM(CASE WHEN is_ad_feed = 1 THEN 1 ELSE 0 END)                     AS ad_photo_cnt_7d,
    ROUND(SUM(CASE WHEN is_ad_feed = 1 THEN 1 ELSE 0 END) * 1.0
          / NULLIF(COUNT(*), 0), 4)                                     AS ad_photo_ratio,
    ROUND(SUM(CASE WHEN is_ad_feed = 1 AND is_complete_play = 1 THEN 1 ELSE 0 END) * 1.0
          / NULLIF(SUM(CASE WHEN is_ad_feed = 1 THEN 1 ELSE 0 END), 0), 4) AS ad_complete_rate,
    ROUND(SUM(CASE WHEN is_ad_feed = 0 AND is_complete_play = 1 THEN 1 ELSE 0 END) * 1.0
          / NULLIF(SUM(CASE WHEN is_ad_feed = 0 THEN 1 ELSE 0 END), 0), 4) AS non_ad_complete_rate
  FROM kscdm.dwd_ks_csm_play_photo_hi
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND product IN ('KUAISHOU', 'NEBULA')
    AND play_duration > 0
    AND user_id IN (SELECT CAST(visitor_id AS BIGINT) FROM {sample_table})
  GROUP BY user_id
) pa ON la.user_id = pa.user_id
LEFT JOIN (
  SELECT user_id, MAX(day_live_s) AS daily_live_duration_max
  FROM (
    SELECT user_id, p_date, SUM(play_duration / 1000.0) AS day_live_s
    FROM kscdm.dwd_ks_csm_play_live_di
    WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
      AND product IN ('KUAISHOU', 'NEBULA')
      AND play_duration > 0
      AND user_id IN (SELECT CAST(visitor_id AS BIGINT) FROM {sample_table})
    GROUP BY user_id, p_date
  ) t
  GROUP BY user_id
) dlm ON COALESCE(la.user_id, pa.user_id) = dlm.user_id
"""


# ─────────────────────────────────────────────────────────────
# feat3: 社交行为特征（纯嵌套子查询版，无 CTE）
# ─────────────────────────────────────────────────────────────
def build_feat3_sql(start_date, end_date, sample_table):
    return f"""
SELECT
  fa.user_id,
  fa.follow_cnt_7d,
  fa.unfollow_cnt_7d,
  fa.follow_day_cnt_7d,
  fa.avg_daily_follow,
  fa.follow_from_live_ratio,
  fa.follow_from_ad_ratio,
  fa.follow_unfollow_ratio,
  COALESCE(sf.sudden_follow_flag, 0)                                   AS sudden_follow_flag,
  CASE
    WHEN (
      (CASE WHEN fa.follow_cnt_7d >= 50                               THEN 1 ELSE 0 END)
    + (CASE WHEN fa.avg_daily_follow >= 10                            THEN 1 ELSE 0 END)
    + (CASE WHEN fa.follow_from_live_ratio >= 0.8                     THEN 1 ELSE 0 END)
    + (CASE WHEN COALESCE(sf.sudden_follow_flag, 0) = 1               THEN 1 ELSE 0 END)
    + (CASE WHEN fa.follow_unfollow_ratio >= 5.0                      THEN 1 ELSE 0 END)
    ) >= 2
    THEN 1 ELSE 0
  END                                                                  AS tag3
FROM (
  SELECT
    user_id,
    SUM(CASE WHEN follow_type = 1 THEN 1 ELSE 0 END)                   AS follow_cnt_7d,
    SUM(CASE WHEN follow_type IN (2, 3) THEN 1 ELSE 0 END)             AS unfollow_cnt_7d,
    COUNT(DISTINCT p_date)                                              AS follow_day_cnt_7d,
    ROUND(SUM(CASE WHEN follow_type = 1 THEN 1 ELSE 0 END) * 1.0
          / NULLIF(COUNT(DISTINCT p_date), 0), 2)                       AS avg_daily_follow,
    ROUND(SUM(CASE WHEN follow_type = 1 AND content_type = 'LIVE_STREAM' THEN 1 ELSE 0 END) * 1.0
          / NULLIF(SUM(CASE WHEN follow_type = 1 THEN 1 ELSE 0 END), 0), 4) AS follow_from_live_ratio,
    ROUND(SUM(CASE WHEN follow_type = 1 AND LOWER(follow_page) LIKE '%ad%' THEN 1 ELSE 0 END) * 1.0
          / NULLIF(SUM(CASE WHEN follow_type = 1 THEN 1 ELSE 0 END), 0), 4) AS follow_from_ad_ratio,
    ROUND(SUM(CASE WHEN follow_type = 1 THEN 1 ELSE 0 END) * 1.0
          / NULLIF(SUM(CASE WHEN follow_type IN (2, 3) THEN 1 ELSE 0 END), 0), 4) AS follow_unfollow_ratio
  FROM kscdm.dwd_ks_soc_follow_di
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND product IN ('KUAISHOU', 'NEBULA')
    AND user_id IN (SELECT CAST(visitor_id AS BIGINT) FROM {sample_table})
  GROUP BY user_id
) fa
LEFT JOIN (
  SELECT
    user_id,
    CASE WHEN COUNT(CASE WHEN day_follow_cnt >= 30 THEN 1 END) >= 2
         THEN 1 ELSE 0 END                                             AS sudden_follow_flag
  FROM (
    SELECT
      user_id,
      p_date,
      SUM(CASE WHEN follow_type = 1 THEN 1 ELSE 0 END) AS day_follow_cnt
    FROM kscdm.dwd_ks_soc_follow_di
    WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
      AND product IN ('KUAISHOU', 'NEBULA')
      AND user_id IN (SELECT CAST(visitor_id AS BIGINT) FROM {sample_table})
    GROUP BY user_id, p_date
  ) daily
  GROUP BY user_id
) sf ON fa.user_id = sf.user_id
"""


# ─────────────────────────────────────────────────────────────
# feat4: 订单交易特征（纯嵌套子查询版，无 CTE）
# ─────────────────────────────────────────────────────────────
def build_feat4_sql(start_date, end_date, sample_table):
    return f"""
SELECT
  visitor_id                                                           AS user_id,
  COUNT(*)                                                             AS order_cnt_7d,
  SUM(CASE WHEN is_refund != 0 THEN 1 ELSE 0 END)                     AS refund_order_cnt_7d,
  ROUND(SUM(CASE WHEN is_refund != 0 THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0), 4)                                      AS refund_rate_7d,
  ROUND(AVG(order_product_payment_amt / 1000000.0), 2)                 AS avg_order_amount,
  SUM(CASE
        WHEN is_refund != 0
         AND refund_create_time IS NOT NULL
         AND order_pay_time IS NOT NULL
         AND (refund_create_time - order_pay_time) / 60.0 <= 30
        THEN 1 ELSE 0 END)                                             AS fast_refund_cnt,
  ROUND(SUM(CASE
              WHEN is_refund != 0
               AND refund_create_time IS NOT NULL
               AND order_pay_time IS NOT NULL
               AND (refund_create_time - order_pay_time) / 60.0 <= 30
              THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4)      AS fast_refund_ratio,
  ROUND(SUM(CASE WHEN (p_exp_tag = 's' OR (search_query IS NOT NULL AND search_query != ''))
                 THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0), 4)                                      AS search_to_order_ratio,
  SUM(CASE WHEN business_line = 'LIVE' THEN 1 ELSE 0 END)             AS live_to_order_cnt,
  ROUND(SUM(CASE WHEN business_line = 'LIVE' THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(*), 0), 4)                                      AS live_to_order_ratio,
  CASE WHEN COUNT(*) >= 3
        AND AVG(order_product_payment_amt / 1000000.0) <= 15
       THEN 1 ELSE 0 END                                               AS multi_small_order_flag,
  CASE
    WHEN (
      (CASE WHEN SUM(CASE WHEN is_refund != 0 THEN 1 ELSE 0 END) * 1.0
                  / NULLIF(COUNT(*), 0) >= 0.7                         THEN 1 ELSE 0 END)
    + (CASE WHEN AVG(order_product_payment_amt / 1000000.0) <= 15      THEN 1 ELSE 0 END)
    + (CASE WHEN SUM(CASE
                       WHEN is_refund != 0
                        AND refund_create_time IS NOT NULL
                        AND order_pay_time IS NOT NULL
                        AND (refund_create_time - order_pay_time) / 60.0 <= 30
                       THEN 1 ELSE 0 END) * 1.0
                  / NULLIF(COUNT(*), 0) >= 0.5                         THEN 1 ELSE 0 END)
    + (CASE WHEN COUNT(*) >= 3
             AND AVG(order_product_payment_amt / 1000000.0) <= 15      THEN 1 ELSE 0 END)
    + (CASE WHEN SUM(CASE WHEN business_line = 'LIVE' THEN 1 ELSE 0 END) * 1.0
                  / NULLIF(COUNT(*), 0) >= 0.8                         THEN 1 ELSE 0 END)
    ) >= 2
    THEN 1 ELSE 0
  END                                                                  AS tag4
FROM ks_ad_antispam.ad_merchant_order_wide_feature_base_di
WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
  AND attribution_type = 1
  AND resource_type != 'UNION'
  AND visitor_id IN (SELECT CAST(visitor_id AS BIGINT) FROM {sample_table})
GROUP BY visitor_id
"""


# ─────────────────────────────────────────────────────────────
# feat5: 开播辅助特征（纯嵌套子查询版，无 CTE）
# ─────────────────────────────────────────────────────────────
def build_feat5_sql(start_date, end_date, sample_table):
    return f"""
SELECT
  lw.user_id,
  COUNT(DISTINCT lw.live_id)                                           AS targeted_live_cnt,
  ROUND(AVG(lm.acu), 2)                                               AS avg_live_acu,
  SUM(CASE WHEN lm.live_content_category = 'shop' THEN 1 ELSE 0 END) AS shop_live_cnt,
  ROUND(SUM(CASE WHEN lm.live_content_category = 'shop' THEN 1 ELSE 0 END) * 1.0
        / NULLIF(COUNT(DISTINCT lw.live_id), 0), 4)                   AS shop_live_ratio,
  ROUND(AVG(lm.live_duration / 1000.0), 0)                            AS avg_live_duration_7d
FROM (
  SELECT user_id, author_id, live_id
  FROM kscdm.dwd_ks_csm_play_live_di
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND product IN ('KUAISHOU', 'NEBULA')
    AND play_duration > 0
    AND user_id IN (SELECT CAST(visitor_id AS BIGINT) FROM {sample_table})
  GROUP BY user_id, author_id, live_id
) lw
LEFT JOIN (
  SELECT author_id, live_id, acu, live_content_category, live_duration
  FROM ksapp.ads_ks_live_aggr_1d
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND product = 'KUAISHOU'
) lm ON lw.live_id = lm.live_id AND lw.author_id = lm.author_id
GROUP BY lw.user_id
"""


def print_distribution(df, feat_cols, tag_cols):
    print('\n========== 特征分布统计 ==========')
    for col in feat_cols:
        if col in df.columns:
            s = df[col].dropna()
            print(f'  {col}: mean={s.mean():.4f}, median={s.median():.4f}, '
                  f'p90={s.quantile(0.9):.4f}, max={s.max():.4f}, '
                  f'non_zero_rate={( s > 0).mean():.4f}')
    print('\n========== Tag 命中率 ==========')
    for col in tag_cols:
        if col in df.columns:
            rate = df[col].fillna(0).mean()
            cnt = df[col].fillna(0).sum()
            print(f'  {col}: 命中率={rate:.4f}  命中数={int(cnt)}')


def filter_anomaly(df):
    cond = (
        (df.get('tag1', 0).fillna(0) == 1) |
        (df.get('tag2', 0).fillna(0) == 1) |
        (df.get('tag3', 0).fillna(0) == 1) |
        (df.get('tag4', 0).fillna(0) == 1)
    )
    anomaly = df[cond].copy()
    anomaly['tag_score'] = (
        df.get('tag1', 0).fillna(0) +
        df.get('tag2', 0).fillna(0) +
        df.get('tag3', 0).fillna(0) +
        df.get('tag4', 0).fillna(0)
    )
    return anomaly.sort_values('tag_score', ascending=False)


def main():
    now = datetime.datetime.now()
    try:
        p_date = sys.argv[1]
        datetime.datetime.strptime(p_date, '%Y%m%d')
    except (IndexError, ValueError):
        p_date = (now - datetime.timedelta(days=1)).strftime('%Y%m%d')

    start_date, end_date = get_date_range(p_date, days=7)
    cold_split_date = (datetime.datetime.strptime(end_date, '%Y%m%d') - datetime.timedelta(days=4)).strftime('%Y%m%d')
    print(f'[INFO] bizdate={p_date}, 7天窗口: {start_date} ~ {end_date}, cold_split={cold_split_date}')

    out_dir = os.path.dirname(os.path.abspath(__file__))

    print('[INFO] Step0: 读取 CSV，筛选 hit_score==6 的用户...')
    raw_df = pd.read_csv(CSV_PATH)
    sample_df = raw_df[raw_df['hit_score'] == 6][['s.visitor_id']].drop_duplicates()
    sample_df = sample_df.rename(columns={'s.visitor_id': 'visitor_id'})
    print(f'[INFO] 样本量: {len(sample_df)}')

    hive = Hive(username='ad_antispam', group_id=239)

    print(f'[INFO] 写入临时样本表 {SAMPLE_HIVE_TABLE}...')
    hive.save_from_df(
        sample_df,
        name=SAMPLE_HIVE_TABLE,
        columns='auto',
        partition_cols=[],
        mode='overwrite'
    )

    print('[INFO] Step1: 计算 feat1（搜索行为）...')
    feat1_df = hive.query(build_feat1_sql(start_date, end_date, SAMPLE_HIVE_TABLE, cold_split_date)).compute()
    print(f'[INFO] feat1 行数={len(feat1_df)}, tag1命中={feat1_df["tag1"].sum() if "tag1" in feat1_df else 0}')

    print('[INFO] Step2: 计算 feat2（观播/视频行为）...')
    feat2_df = hive.query(build_feat2_sql(start_date, end_date, SAMPLE_HIVE_TABLE)).compute()
    print(f'[INFO] feat2 行数={len(feat2_df)}, tag2命中={feat2_df["tag2"].sum() if "tag2" in feat2_df else 0}')

    print('[INFO] Step3: 计算 feat3（社交行为）...')
    feat3_df = hive.query(build_feat3_sql(start_date, end_date, SAMPLE_HIVE_TABLE)).compute()
    print(f'[INFO] feat3 行数={len(feat3_df)}, tag3命中={feat3_df["tag3"].sum() if "tag3" in feat3_df else 0}')

    print('[INFO] Step4: 计算 feat4（订单交易）...')
    feat4_df = hive.query(build_feat4_sql(start_date, end_date, SAMPLE_HIVE_TABLE)).compute()
    print(f'[INFO] feat4 行数={len(feat4_df)}, tag4命中={feat4_df["tag4"].sum() if "tag4" in feat4_df else 0}')

    print('[INFO] Step5: 计算 feat5（开播辅助特征）...')
    feat5_df = hive.query(build_feat5_sql(start_date, end_date, SAMPLE_HIVE_TABLE)).compute()
    print(f'[INFO] feat5 行数={len(feat5_df)}')

    print('[INFO] Step6: 合并特征宽表...')
    feat_wide = (
        sample_df.rename(columns={'visitor_id': 'user_id'})
        .merge(feat1_df, on='user_id', how='left')
        .merge(feat2_df, on='user_id', how='left')
        .merge(feat3_df, on='user_id', how='left')
        .merge(feat4_df, on='user_id', how='left')
        .merge(feat5_df, on='user_id', how='left')
    )
    for col in ['tag1', 'tag2', 'tag3', 'tag4']:
        if col in feat_wide.columns:
            feat_wide[col] = feat_wide[col].fillna(0).astype(int)

    feat_cols = [
        'search_cnt_7d', 'search_day_cnt_7d', 'avg_daily_search_cnt',
        'search_entropy_7d', 'top1_query_ratio', 'high_value_query_ratio', 'search_sug_ratio',
        'live_play_cnt_7d', 'live_play_duration_7d', 'max_single_live_duration',
        'daily_live_duration_max', 'live_ad_play_ratio', 'live_background_ratio',
        'ad_photo_ratio', 'ad_complete_rate', 'non_ad_complete_rate', 'ad_vs_non_complete_ratio',
        'follow_cnt_7d', 'avg_daily_follow', 'follow_from_live_ratio', 'follow_unfollow_ratio',
        'order_cnt_7d', 'refund_rate_7d', 'avg_order_amount', 'fast_refund_ratio',
        'live_to_order_ratio',
        'targeted_live_cnt', 'avg_live_acu', 'shop_live_ratio',
    ]
    tag_cols = ['tag1', 'tag2', 'tag3', 'tag4']

    print_distribution(feat_wide, feat_cols, tag_cols)

    wide_path = os.path.join(out_dir, f'farming_feat_wide_v1_{end_date}.csv')
    feat_wide.to_csv(wide_path, index=False, encoding='utf-8')
    print(f'\n[INFO] 特征宽表已保存: {wide_path}')

    print('[INFO] Step7: 筛选异常用户...')
    anomaly_df = filter_anomaly(feat_wide)
    print(f'[INFO] 命中任意 tag 的用户数: {len(anomaly_df)}')
    print(f'[INFO] tag_score 分布:\n{anomaly_df["tag_score"].value_counts().sort_index().to_string()}')

    anomaly_path = os.path.join(out_dir, f'farming_anomaly_v1_{end_date}.csv')
    anomaly_df.to_csv(anomaly_path, index=False, encoding='utf-8')
    print(f'[INFO] 异常用户已保存: {anomaly_path}')
    print('[INFO] 完成！')


if __name__ == '__main__':
    main()
