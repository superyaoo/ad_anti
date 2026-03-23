#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@File  : perception_farming_initial_sample.py
@Author: zengjunyao(@kuaishou.com)
@Date  : 2026-03-20
@Desc  : 养机羊毛党感知 - 初始样本圈定（后效版）
         时间窗口：近3天（T-2 ~ T，T = bizdate-1）
         逻辑：
           浅层后效：广告曝光≥200 AND 金币≥1600
           深度后效：退款率≥90% OR ROI<0.1 OR 小额多单(cnt≥3 & avg≤15) OR 无次日留存
         三类样本：
           label=2  养机 + 后效明确差（强正样本）
           label=1  养机 + 后效临界（弱正样本）
           label=0  白样本（无养机行为 + 后效正常）
         输出表：ks_ad_antispam_dev.ad_farming_initial_sample_di  partition(p_date)
"""

import sys
import datetime
import pandas as pd
from kmlutils.kml_hive import Hive


def get_date_range(end_date_str, days=3):
    """返回近 days 天的 (start_date, end_date)"""
    end = datetime.datetime.strptime(end_date_str, '%Y%m%d')
    start = end - datetime.timedelta(days=days - 1)
    return start.strftime('%Y%m%d'), end.strftime('%Y%m%d')


def build_black_sample_sql(start_date, end_date):
    inner = f"""
WITH

shallow_ad AS (
  SELECT
    visitor_id,
    SUM(CASE WHEN action_type IN (
          'AD_ITEM_IMPRESSION', 'AD_PHOTO_IMPRESSION',
          'AD_LIVE_IMPRESSION', 'AD_LIVE_PLAYED_STARTED')
         THEN cnt ELSE 0 END)                                      AS impr_cnt,
    SUM(CASE WHEN action_type IN (
          'AD_ITEM_CLICK', 'AD_LIVE_CLICK', 'AD_PHOTO_CLICK')
         THEN cnt ELSE 0 END)                                      AS click_cnt,
    SUM(CASE WHEN action_type = charge_action_type
         THEN cost_yuan ELSE 0 END)                                AS cost_yuan
  FROM ad_rc_data.ad_kuaishou_account_visitor_stat_di
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND media_app_id IN ('kuaishou', 'kuaishou_nebula')
  GROUP BY visitor_id
  HAVING SUM(CASE WHEN action_type IN (
    'AD_ITEM_IMPRESSION', 'AD_PHOTO_IMPRESSION',
    'AD_LIVE_IMPRESSION', 'AD_LIVE_PLAYED_STARTED')
    THEN cnt ELSE 0 END) >= 200
),

coin AS (
  SELECT
    user_id                   AS visitor_id,
    SUM(reward_coin_cnt)      AS total_coin
  FROM ks_ad_antispam.ad_coin_user_biztype_encourage_1d
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
  GROUP BY user_id
  HAVING SUM(reward_coin_cnt) >= 1600
),

shallow_hit AS (
  SELECT
    a.visitor_id,
    a.impr_cnt,
    a.click_cnt,
    a.cost_yuan,
    CASE WHEN a.impr_cnt = 0 THEN 0
         ELSE ROUND(a.click_cnt * 1.0 / a.impr_cnt, 4) END        AS ctr,
    c.total_coin
  FROM shallow_ad a
  INNER JOIN coin c ON a.visitor_id = c.visitor_id
),

deep_conv AS (
  SELECT
    visitor_id,
    SUM(incycle_order_paied_cnt)    AS paied_cnt,
    SUM(incycle_order_refund_cnt)   AS refund_cnt,
    SUM(incycle_order_lowprice_cnt) AS lowprice_cnt,
    SUM(nextday_stay_cnt)           AS nextday_stay_cnt,
    SUM(conversion_cnt)             AS conversion_cnt
  FROM ks_ad_antispam.ad_kuaishou_user_resource_type_conv_stat_di
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND flow_type NOT IN ('UNION')
  GROUP BY visitor_id
),

order_amt AS (
  SELECT
    visitor_id,
    COUNT(*)                                                        AS order_cnt,
    SUM(order_product_payment_amt) / 1000000.0                     AS gmv_yuan,
    SUM(CASE WHEN is_refund != 0
      THEN order_product_payment_amt ELSE 0 END) / 1000000.0       AS refund_gmv_yuan,
    SUM(CASE WHEN is_refund != 0 THEN 1 ELSE 0 END)                AS refund_order_cnt,
    AVG(order_product_payment_amt) / 1000000.0                     AS avg_order_yuan
  FROM ks_ad_antispam.ad_merchant_order_wide_feature_base_di
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND attribution_type = 1
    AND resource_type != 'UNION'
  GROUP BY visitor_id
),

combined AS (
  SELECT
    s.visitor_id,
    s.impr_cnt,
    s.click_cnt,
    s.ctr,
    s.cost_yuan,
    s.total_coin,
    COALESCE(d.paied_cnt, 0)                                       AS paied_cnt,
    COALESCE(d.refund_cnt, 0)                                      AS refund_cnt,
    COALESCE(d.nextday_stay_cnt, 0)                                AS nextday_stay_cnt,
    COALESCE(d.conversion_cnt, 0)                                  AS conversion_cnt,
    CASE WHEN COALESCE(d.paied_cnt, 0) = 0 THEN NULL
         ELSE ROUND(d.refund_cnt * 1.0 / d.paied_cnt, 4)
    END                                                             AS refund_rate,
    COALESCE(o.order_cnt, 0)                                       AS order_cnt,
    ROUND(COALESCE(o.gmv_yuan, 0), 2)                              AS gmv_yuan,
    ROUND(COALESCE(o.refund_gmv_yuan, 0), 2)                       AS refund_gmv_yuan,
    ROUND(COALESCE(o.avg_order_yuan, 0), 2)                        AS avg_order_yuan,
    CASE WHEN COALESCE(s.cost_yuan, 0) = 0 THEN NULL
         ELSE ROUND((COALESCE(o.gmv_yuan, 0) - COALESCE(o.refund_gmv_yuan, 0))
              / s.cost_yuan, 4)
    END                                                             AS roi,
    -- 后效命中 flag
    CASE WHEN COALESCE(d.paied_cnt, 0) > 0
          AND d.refund_cnt * 1.0 / d.paied_cnt >= 0.9
         THEN 1 ELSE 0 END                                         AS f_refund_90,
    CASE WHEN COALESCE(d.paied_cnt, 0) > 0
          AND d.refund_cnt * 1.0 / d.paied_cnt >= 0.5
          AND d.refund_cnt * 1.0 / d.paied_cnt <  0.9
         THEN 1 ELSE 0 END                                         AS f_refund_50_90,
    CASE WHEN COALESCE(o.order_cnt, 0) >= 3
          AND COALESCE(o.avg_order_yuan, 0) <= 15
         THEN 1 ELSE 0 END                                         AS f_multi_small_order,
    CASE WHEN COALESCE(s.cost_yuan, 0) > 0
          AND (COALESCE(o.gmv_yuan, 0) - COALESCE(o.refund_gmv_yuan, 0))
              / s.cost_yuan < 0.1
         THEN 1 ELSE 0 END                                         AS f_low_roi,
    CASE WHEN COALESCE(d.nextday_stay_cnt, 0) = 0
          AND COALESCE(d.conversion_cnt, 0) > 0
         THEN 1 ELSE 0 END                                         AS f_no_retention
  FROM shallow_hit s
  LEFT JOIN deep_conv d ON s.visitor_id = d.visitor_id
  LEFT JOIN order_amt o ON s.visitor_id = o.visitor_id
)

SELECT * FROM (
SELECT
  visitor_id,
  impr_cnt,
  click_cnt,
  ctr,
  cost_yuan,
  total_coin,
  paied_cnt,
  refund_cnt,
  refund_rate,
  order_cnt,
  avg_order_yuan,
  gmv_yuan,
  refund_gmv_yuan,
  roi,
  nextday_stay_cnt,
  conversion_cnt,
  f_refund_90,
  f_refund_50_90,
  f_multi_small_order,
  f_low_roi,
  f_no_retention,
  CASE
    WHEN f_refund_90 = 1 OR f_multi_small_order = 1 OR f_low_roi = 1 THEN 2
    WHEN f_refund_50_90 = 1 OR f_no_retention = 1                    THEN 1
    ELSE NULL
  END AS label
FROM combined
WHERE
  f_refund_90 = 1 OR f_refund_50_90 = 1
  OR f_multi_small_order = 1
  OR f_low_roi = 1
  OR f_no_retention = 1
) t
"""
    return "SELECT * FROM (" + inner + ") _black_result"


def build_white_sample_sql(start_date, end_date, white_limit):
    return f"""
SELECT
  visitor_id,
  impr_cnt,
  0     AS click_cnt,
  0.0   AS ctr,
  0.0   AS cost_yuan,
  0     AS total_coin,
  0     AS paied_cnt,
  0     AS refund_cnt,
  NULL  AS refund_rate,
  0     AS order_cnt,
  0.0   AS avg_order_yuan,
  0.0   AS gmv_yuan,
  0.0   AS refund_gmv_yuan,
  NULL  AS roi,
  0     AS nextday_stay_cnt,
  0     AS conversion_cnt,
  0     AS f_refund_90,
  0     AS f_refund_50_90,
  0     AS f_multi_small_order,
  0     AS f_low_roi,
  0     AS f_no_retention,
  0     AS label
FROM (
  SELECT
    visitor_id,
    SUM(CASE WHEN action_type IN (
          'AD_ITEM_IMPRESSION', 'AD_PHOTO_IMPRESSION',
          'AD_LIVE_IMPRESSION', 'AD_LIVE_PLAYED_STARTED')
         THEN cnt ELSE 0 END) AS impr_cnt
  FROM ad_rc_data.ad_kuaishou_account_visitor_stat_di
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND media_app_id IN ('kuaishou', 'kuaishou_nebula')
  GROUP BY visitor_id
  HAVING SUM(CASE WHEN action_type IN (
    'AD_ITEM_IMPRESSION', 'AD_PHOTO_IMPRESSION',
    'AD_LIVE_IMPRESSION', 'AD_LIVE_PLAYED_STARTED')
    THEN cnt ELSE 0 END) BETWEEN 10 AND 199
) t
WHERE visitor_id NOT IN (
  SELECT user_id
  FROM ks_ad_antispam.ad_coin_user_biztype_encourage_1d
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
  GROUP BY user_id
  HAVING SUM(reward_coin_cnt) >= 1600
)
ORDER BY RAND()
LIMIT {white_limit}
"""


if __name__ == '__main__':
    now = datetime.datetime.now()

    # 支持命令行传入 p_date（bizdate），默认取昨天
    try:
        p_date = sys.argv[1]
        datetime.datetime.strptime(p_date, '%Y%m%d')
    except (IndexError, ValueError):
        p_date = (now - datetime.timedelta(days=1)).strftime('%Y%m%d')

    start_date, end_date = get_date_range(p_date, days=3)
    print(f'[INFO] bizdate={p_date}，时间窗口: {start_date} ~ {end_date}')

    output_table = 'ks_ad_antispam_dev.ad_farming_initial_sample_di'
    partition_cols = [f"p_date='{end_date}'"]

    hive = Hive(username='ad_antispam', group_id=239)

    # ── 1. 黑样本 ──────────────────────────────────────────────
    print('[INFO] Step1: 查询黑样本...')
    black_df = hive.query(build_black_sample_sql(start_date, end_date)).compute()
    black_cnt = len(black_df)
    label2_cnt = int((black_df['label'] == 2).sum())
    label1_cnt = int((black_df['label'] == 1).sum())
    print(f'[INFO] 黑样本总量={black_cnt}，label=2（强正）={label2_cnt}，label=1（弱正）={label1_cnt}')

    if black_cnt == 0:
        print('[WARN] 黑样本为空，请检查数据是否就绪，跳过本次写表')
        sys.exit(0)

    # ── 2. 白样本（黑:白 = 1:10）─────────────────────────────
    white_limit = black_cnt * 10
    print(f'[INFO] Step2: 查询白样本（目标量={white_limit}）...')
    white_df = hive.query(build_white_sample_sql(start_date, end_date, white_limit)).compute()
    white_cnt = len(white_df)
    print(f'[INFO] 白样本量={white_cnt}，实际黑白比=1:{round(white_cnt / black_cnt, 1)}')

    # ── 3. 合并写表 + 导出 CSV ─────────────────────────────────
    final_df = pd.concat([black_df, white_df], ignore_index=True)
    print(f'[INFO] Step3: 写表 {output_table} partition={partition_cols}，总量={len(final_df)}')
    hive.save_from_df(
        final_df,
        name=output_table,
        columns='auto',
        partition_cols=partition_cols,
        mode='overwrite'
    )

    import os
    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'farming_initial_sample_{end_date}.csv')
    final_df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f'[INFO] CSV 已保存: {csv_path}')
    print('[INFO] 完成！')
