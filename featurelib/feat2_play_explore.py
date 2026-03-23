#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@File  : feat2_play_explore.py
@Author: zengjunyao(@kuaishou.com)
@Date  : 2026-03-23
@Desc  : 养机羊毛党感知 - feat2 观播/视频行为特征（独立探查版）
         样本：ks_ad_antispam_dev.ad_farming_initial_sample_di p_date='20260319'
         特征窗口：20260313 ~ 20260319（7天）
         数据源：
           kscdm.dwd_ks_csm_play_live_di  （直播观看）
           kscdm.dwd_ks_csm_play_photo_hi （视频观看）
         不设阈值、不产出 tag，取所有有用数值型特征 + 衍生特征
         输出：feat2_play_explore_20260319.csv
         注意：SQL 为纯嵌套子查询，无 WITH CTE，避免 kmlutils 包装报错
"""

import os
import sys
import datetime
import pandas as pd
from kmlutils.kml_hive import Hive

SAMPLE_TABLE = 'ks_ad_antispam_dev.ad_farming_initial_sample_di'


def get_date_range(end_date_str, days=7):
    end = datetime.datetime.strptime(end_date_str, '%Y%m%d')
    start = end - datetime.timedelta(days=days - 1)
    return start.strftime('%Y%m%d'), end.strftime('%Y%m%d')


def build_feat2_sql(start_date, end_date, sample_table, p_date):
    return f"""
SELECT
  COALESCE(la.user_id, pa.user_id)                                      AS user_id,

  -- ── 直播观看基础特征 ──────────────────────────────────────────
  COALESCE(la.live_play_cnt_7d, 0)                                      AS live_play_cnt_7d,
  COALESCE(la.live_play_duration_7d, 0)                                 AS live_play_duration_7d,
  COALESCE(la.live_play_day_cnt_7d, 0)                                  AS live_play_day_cnt_7d,
  COALESCE(la.avg_single_live_duration, 0)                              AS avg_single_live_duration,
  COALESCE(la.max_single_live_duration, 0)                              AS max_single_live_duration,
  COALESCE(dlm.daily_live_duration_max, 0)                              AS daily_live_duration_max,
  COALESCE(la.live_ad_play_duration, 0)                                 AS live_ad_play_duration,
  COALESCE(la.live_ad_play_ratio, 0)                                    AS live_ad_play_ratio,
  COALESCE(la.live_background_duration, 0)                              AS live_background_duration,
  COALESCE(la.live_background_ratio, 0)                                 AS live_background_ratio,
  COALESCE(la.live_ad_play_cnt, 0)                                      AS live_ad_play_cnt,
  COALESCE(la.live_non_ad_play_cnt, 0)                                  AS live_non_ad_play_cnt,

  -- ── 视频观看基础特征（play_duration列无权限，仅保留count/完播率类）──
  COALESCE(pa.photo_play_cnt_7d, 0)                                     AS photo_play_cnt_7d,
  COALESCE(pa.photo_play_day_cnt_7d, 0)                                 AS photo_play_day_cnt_7d,
  COALESCE(pa.ad_photo_cnt_7d, 0)                                       AS ad_photo_cnt_7d,
  COALESCE(pa.non_ad_photo_cnt_7d, 0)                                   AS non_ad_photo_cnt_7d,
  COALESCE(pa.ad_photo_ratio, 0)                                        AS ad_photo_ratio,
  COALESCE(pa.ad_complete_cnt, 0)                                       AS ad_complete_cnt,
  COALESCE(pa.non_ad_complete_cnt, 0)                                   AS non_ad_complete_cnt,
  COALESCE(pa.ad_complete_rate, 0)                                      AS ad_complete_rate,
  COALESCE(pa.non_ad_complete_rate, 0)                                  AS non_ad_complete_rate,

  -- ── 衍生特征 ────────────────────────────────────────────────
  ROUND(COALESCE(pa.ad_complete_rate, 0)
        / NULLIF(COALESCE(pa.non_ad_complete_rate, 0), 0), 4)           AS ad_vs_non_complete_ratio,
  ROUND(COALESCE(dlm.daily_live_duration_max, 0)
        / NULLIF(COALESCE(la.live_play_duration_7d, 0)
                 / NULLIF(COALESCE(la.live_play_day_cnt_7d, 0), 0), 0), 4)
                                                                        AS daily_live_max_vs_avg_ratio,
  ROUND(COALESCE(la.live_background_duration, 0)
        / NULLIF(COALESCE(la.live_play_duration_7d, 0), 0), 4)          AS live_background_ratio_recalc

FROM (
  SELECT
    user_id,
    COUNT(*)                                                            AS live_play_cnt_7d,
    ROUND(SUM(play_duration / 1000.0), 0)                               AS live_play_duration_7d,
    COUNT(DISTINCT p_date)                                              AS live_play_day_cnt_7d,
    ROUND(AVG(play_duration / 1000.0), 0)                               AS avg_single_live_duration,
    ROUND(MAX(play_duration / 1000.0), 0)                               AS max_single_live_duration,
    ROUND(SUM(CASE WHEN page_category = 'ADS' THEN play_duration / 1000.0 ELSE 0 END), 0)
                                                                        AS live_ad_play_duration,
    ROUND(SUM(CASE WHEN page_category = 'ADS' THEN play_duration / 1000.0 ELSE 0 END)
          / NULLIF(SUM(play_duration / 1000.0), 0), 4)                  AS live_ad_play_ratio,
    ROUND(SUM(background_duration / 1000.0), 0)                         AS live_background_duration,
    ROUND(SUM(background_duration / 1000.0)
          / NULLIF(SUM(play_duration / 1000.0), 0), 4)                  AS live_background_ratio,
    SUM(CASE WHEN page_category = 'ADS' THEN 1 ELSE 0 END)             AS live_ad_play_cnt,
    SUM(CASE WHEN page_category != 'ADS' THEN 1 ELSE 0 END)            AS live_non_ad_play_cnt
  FROM kscdm.dwd_ks_csm_play_live_di
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND product IN ('KUAISHOU', 'NEBULA')
    AND play_duration > 0
    AND user_id IN (
      SELECT CAST(visitor_id AS BIGINT)
      FROM {sample_table}
      WHERE p_date = '{p_date}'
    )
  GROUP BY user_id
) la
FULL OUTER JOIN (
  SELECT
    user_id,
    COUNT(*)                                                            AS photo_play_cnt_7d,
    COUNT(DISTINCT p_date)                                              AS photo_play_day_cnt_7d,
    SUM(CASE WHEN is_ad_feed = 1 THEN 1 ELSE 0 END)                     AS ad_photo_cnt_7d,
    SUM(CASE WHEN is_ad_feed = 0 THEN 1 ELSE 0 END)                     AS non_ad_photo_cnt_7d,
    ROUND(SUM(CASE WHEN is_ad_feed = 1 THEN 1 ELSE 0 END) * 1.0
          / NULLIF(COUNT(*), 0), 4)                                     AS ad_photo_ratio,
    SUM(CASE WHEN is_ad_feed = 1 AND is_complete_play = 1 THEN 1 ELSE 0 END)
                                                                        AS ad_complete_cnt,
    SUM(CASE WHEN is_ad_feed = 0 AND is_complete_play = 1 THEN 1 ELSE 0 END)
                                                                        AS non_ad_complete_cnt,
    ROUND(SUM(CASE WHEN is_ad_feed = 1 AND is_complete_play = 1 THEN 1 ELSE 0 END) * 1.0
          / NULLIF(SUM(CASE WHEN is_ad_feed = 1 THEN 1 ELSE 0 END), 0), 4)
                                                                        AS ad_complete_rate,
    ROUND(SUM(CASE WHEN is_ad_feed = 0 AND is_complete_play = 1 THEN 1 ELSE 0 END) * 1.0
          / NULLIF(SUM(CASE WHEN is_ad_feed = 0 THEN 1 ELSE 0 END), 0), 4)
                                                                        AS non_ad_complete_rate
  FROM kscdm.dwd_ks_csm_play_photo_hi
  WHERE p_date >= '{start_date}' AND p_date <= '{end_date}'
    AND product IN ('KUAISHOU', 'NEBULA')
    AND user_id IN (
      SELECT CAST(visitor_id AS BIGINT)
      FROM {sample_table}
      WHERE p_date = '{p_date}'
    )
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
      AND user_id IN (
        SELECT CAST(visitor_id AS BIGINT)
        FROM {sample_table}
        WHERE p_date = '{p_date}'
      )
    GROUP BY user_id, p_date
  ) t
  GROUP BY user_id
) dlm ON COALESCE(la.user_id, pa.user_id) = dlm.user_id
"""


def main():
    now = datetime.datetime.now()
    try:
        p_date = sys.argv[1]
        datetime.datetime.strptime(p_date, '%Y%m%d')
    except (IndexError, ValueError):
        p_date = (now - datetime.timedelta(days=1)).strftime('%Y%m%d')

    start_date, end_date = get_date_range(p_date, days=7)
    print(f'[INFO] p_date={p_date}, 7天窗口: {start_date} ~ {end_date}')

    hive = Hive(username='ad_antispam', group_id=239)

    print('[INFO] 计算 feat2（观播/视频行为）...')
    df = hive.query(build_feat2_sql(start_date, end_date, SAMPLE_TABLE, p_date)).compute()
    print(f'[INFO] 行数={len(df)}, 列数={len(df.columns)}')

    print('\n========== 特征分布统计 ==========')
    for col in df.columns:
        if col == 'user_id':
            continue
        s = df[col].dropna()
        if len(s) == 0:
            continue
        print(f'  {col:40s} mean={s.mean():10.4f}  median={s.median():10.4f}  '
              f'p90={s.quantile(0.9):10.4f}  max={s.max():12.2f}  '
              f'non_zero={( s > 0).mean():.4f}')

    out_dir = os.path.dirname(os.path.abspath(__file__))
    out_path = os.path.join(out_dir, f'feat2_play_explore_{end_date}.csv')
    df.to_csv(out_path, index=False, encoding='utf-8')
    print(f'\n[INFO] 已保存: {out_path}')


if __name__ == '__main__':
    main()
