#!/usr/bin/env python3
"""
批量查询用户信息（多表拼接版）

查询三个表：
1. ks_rc_ad_antispam.hive_ad_rc_data_ad_kuaishou_visitor_device_product_featurev2_di
2. ks_ad_antispam.ad_kuaishou_visitor_device_product_feature_di  
3. ks_ad_antispam.refund_routine_wide_for_policy_di

并将结果按 visitor_id 拼接
"""

import pandas as pd
import requests
from typing import List, Optional
from datetime import datetime, timedelta

# 用户ID列表
USER_IDS = """5248133582
4786416885
4917976750
210681723
5143012032
4687981811
3542828217
4849798178
4515153529
5015512641
4217872205
5121800667
5023673012
140974376
1643204952
5222457645
4496526188
5157470319
5176304347
3779947014
4898944313
5116855062
3519276919
1575802752
4538323725
5190333556
4795430097
4964886347
4842560695
4253257986
1321945708
4697660390
4648283033
5023887688
2903192027
5090609178
1720904532
1493464856
4709563781
4874216651
1088232434
4652748358
3983404005
5201811788
3511726320
2449768674
4238037509
4570687347
2439177269
4454945370
4987527730
3768878166
3073720338
1309792331
4805486674
2320880706
4753714874
1463962941
4156037386
4520500896
4852921099
4664261037
4958218613
4674292213
5141958960
5083738561
350414715
4726882675
4646830401
5228898448
4966550597
4803645671
5029425256
5175074844
4917166358
3332615121
4749318117
4866445335
4190308550
4154381080
1096806792
1921906531
2849782088
3345748615
4955372241
4857676866
4907262776
3888492912
2049184492
3955418049
4593350121
4313001109
4048816126
4948014315
4794225746
4807408777
5013288023
1450484388
4378740760
1892491821
4848733025
1972686332
2186269046
4913156271
3701723341
4660151536
4233671866
4424899956
533619295
2653187661
5206979850
5090679653
5130911956
4644117090
5175010973
1377142975
5013377580
245137780
657797935
2440609037
4708584617
3699076560
2927467752
1058125676
2193290065
5148519028
4690820412
4664661633
4565345037
1880561942
4674430434
1644544946
4776295517
2161262131
4786045134
5050832003
5176052738
5070207058
4932258030
4906297592
2705285073
2199239000
5073735619
5094974115
1242587908
4422977377
5208333796
3377358141
4811559433
5007339223
5250380291
727850984
4769280776
2088208073
5150114043
5105019116
4188036331
5070184728
3341556327
4786130544
4675897435
5050123643
361152405
4842366959
4935530626
3971505810
5168569077
4419045357
4996910856
2955754371
4913128343
2877995418
4447884383
4292641207
5065608064
4542803499
147659662
5209733655
2605236037
4715512129
4504053765
996132270
3676965776
375474110
4138722598
4388960945
5222478642
5013586923
3341380576
3005605831
4570479472
4834583966
4361863827
4768498451
5029300638
4317273328
4621542073
5033934203
5147073842
4648353866
5066331768
3834236056
2721985020
3209612095
1310647443
4965446885
1640143292
3922725687
610147698
5072707748
4164719925
1082895393
532661337
4810473318
5069752961
4796992249
4874974033
3997186568
4166976054
4968484005
4789244995
210254403
162365393
4536285008
4171134309
4771988437
4789366636
4723735360
3180178066
5177345194
4844860257
5199217887
4444746173
3613561843
4916293194
4859780312
4969033325
4725003989
4376805191
4932725510
4786176318
4887561528
1131004360
4358995734
4581238275
4837890265
4080136202
4826953119
4648387058
5198639296
2411812056
4881652188
4786189580
4391186764
4782500632
4748633140
5177333720
4352060090
5083247747
4657517735
4439982912
5179580356
162457862
870846202
236327787
1107198282
1403535688
5128909411
4836468995
4838352327
5006920837
4834785017
2993860364
4807215202
4862879095
2982128235
2002687103
4787655590
2364884895
4386730369
3405187480
4787108062
782690492
4557854377
3186569622
4386187872
911070980
4390956544
242850984
3776071389
5060185517
2760311469
4187563605
4651186410
2470615619
4261178278
1526270474
4901013431
4698839144
4644907164
4993223784
4554429381
4967310978
2912760464
4556095829
4358926353
1877029691
4252147570
3853841310
4695751020
5258409865
4312544894
4829957644
5276372589
4769831958
4752442542
4787570385
5185977565
4165740281
3315196428
3718749988
5022228161
4938454459
155642916
4984319708
4779167755
4786286551
5077662694
1390946646
5071879485
5013287263
4774349574
4420356011
4990911242
4213644791
4655797628
4031051893
2732526023
5090622559
4953153540
3406118524
1244639112
4359923130
4687835528
4797052297
2666587594
4071416600
2741554207
4643284335
5022078127
3360892704
525370126
2195427972
4899937336
5214522270
5206138941
3769955693
3785933108
4575500162
5265895211
5090632967
4615148959
4473329852
4578070712
4176424714
2944635459
4844830070
4360933321
1859296607
4505826122
4535572251
4606849344
823282326
279944364
4244142409
4157157284
5185970712
2144477553
4893168073
4855449817
4917921850
4851004251
4668196417
3612038075
4637193560
4841378973
5087243753
5120290602
772551083
5047862677
5060195234
4880293997
2340024187
5140836410
4648252457
74046722
3762981618
5195854437
4365132819
5020120630
5083710241
4842145706
4538766121
5080645551
587314027
4984653487
4929751012
4647896303
4798010023
1997300289
4049771318
4668189013
5101011048
4378683539
1852238493
5075876504
5039854924
5151281346
5011003330
2201696190
4444737407
2001497193
2470465527
5082110088
4830369741
4339583279
4816458719
4963643379
5040646375
872491172
4040377913
1806030714
5090222891
4214652467
1364470425
1307979107
3749740312
5090649046
2375569166
3897484728
1546430638
4873891141
4641815507
3816205174
2565275853
4775647462
1811214812
5215498598
5070190148
4982436706
4615205578
2572166110
4861774022
3085503878
4402508932
3513662936
2193248765
4836410257
4657896859
4665208834
2302973335
2902422120
5154173617
5267280536
3662287089
1296461424
5196234688
4595157332
2888550850
4992229591
2944285660
1304961408
4388157331
4270047112
4807532983
4466812076
5104745139
4407058164
3624833012
4834646603
4503899347
2427353691
5078318615"""


def parse_user_ids(user_ids_text: str) -> List[str]:
    """解析用户ID列表"""
    return [uid.strip() for uid in user_ids_text.strip().split('\n') if uid.strip()]


def get_yesterday_partition() -> str:
    """获取昨天的分区日期 (YYYYMMDD 格式)"""
    yesterday = datetime.now() - timedelta(days=3)
    return yesterday.strftime('%Y%m%d')


def query_monika_api(sql: str, api_url: str, db_type: str = 'hive') -> pd.DataFrame:
    """
    通过 Monika SQL API 查询
    """
    print(f"📡 查询中... (数据库类型: {db_type})")
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/plain, */*',
        'trace-context': '{"laneId": "PRT.sqlProxy"}',
    }
    
    payload = {
        'sql': sql,
        'type': db_type
    }
    
    try:
        response = requests.post(
            api_url,
            headers=headers,
            json=payload,
            timeout=300
        )
        
        response.raise_for_status()
        result = response.json()
        
        # 解析响应
        if isinstance(result, dict):
            if 'data' in result:
                data = result['data']
            elif 'result' in result and result.get('result') == 0:
                data = result.get('data', [])
            else:
                error_msg = result.get('message', '未知错误')
                print(f"❌ API 返回错误: {error_msg}")
                return pd.DataFrame()
        else:
            data = result
        
        if not data:
            print("⚠️  查询结果为空")
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        print(f"✅ 成功返回 {len(df)} 条记录")
        return df
        
    except Exception as e:
        print(f"❌ 查询失败: {e}")
        return pd.DataFrame()


def query_table1(user_ids: List[str], partition: str, api_url: str) -> pd.DataFrame:
    """
    查询表1: ks_rc_ad_antispam.hive_ad_rc_data_ad_kuaishou_visitor_device_product_featurev2_di
    """
    print(f"\n{'='*70}")
    print(f"表1: 基础特征表 (featurev2_di)")
    print(f"{'='*70}")
    
    user_ids_quoted = ','.join([f"'{uid}'" for uid in user_ids])
    
    sql = f"""
SELECT 
    visitor_id,
    device_id,
    product,
    is_gid_15d,
    is_new_device,
    ip,
    ipc,
    platform,
    model,
    toString(applist) as applist,
    toString(device_factor) as device_factor,
    country_name,
    province_name,
    city_name,
    device_brand,
    max_global_id,
    user_name,
    is_v,
    is_verified,
    gift_num,
    charge_num,
    fans_user_num,
    friend_user_num,
    toString(keyword_set) as keyword_set,
    reg_date,
    reg_day,
    system_version,
    upload_phone_cnt_7d,
    photo_camera_upload_cnt_7d
FROM ks_rc_ad_antispam.hive_ad_rc_data_ad_kuaishou_visitor_device_product_featurev2_di
WHERE p_date = '{partition}'
    AND visitor_id IN ({user_ids_quoted})
LIMIT 1000
"""
    
    print(f"分区: {partition}")
    print(f"SQL: {sql[:200]}...")
    
    return query_monika_api(sql, api_url, 'ck')


def query_table2(user_ids: List[str], partition: str, api_url: str) -> pd.DataFrame:
    """
    查询表2: ks_ad_antispam.ad_kuaishou_visitor_device_product_feature_di
    """
    print(f"\n{'='*70}")
    print(f"表2: 特征表 (feature_di)")
    print(f"{'='*70}")
    
    user_ids_quoted = ','.join([f"'{uid}'" for uid in user_ids])
    
    # 查询所有字段
    sql = f"""
SELECT *
FROM ks_ad_antispam.ad_kuaishou_visitor_device_product_feature_di
WHERE p_date = '{partition}'
    AND visitor_id IN ({user_ids_quoted})
LIMIT 1000
"""
    
    print(f"分区: {partition}")
    print(f"SQL: {sql[:200]}...")
    
    return query_monika_api(sql, api_url, 'hive')


def query_table3(user_ids: List[str], partition: str, api_url: str) -> pd.DataFrame:
    """
    查询表3: ks_ad_antispam.refund_routine_wide_for_policy_di
    """
    print(f"\n{'='*70}")
    print(f"表3: 退款策略宽表 (refund_routine_wide_for_policy_di)")
    print(f"{'='*70}")
    
    user_ids_quoted = ','.join([f"'{uid}'" for uid in user_ids])
    
    # 查询所有字段
    sql = f"""
SELECT *
FROM ks_ad_antispam.refund_routine_wide_for_policy_di
WHERE p_date = '{partition}'
    AND visitor_id IN ({user_ids_quoted})
LIMIT 1000
"""
    
    print(f"分区: {partition}")
    print(f"SQL: {sql[:200]}...")
    
    return query_monika_api(sql, api_url, 'hive')


def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame) -> pd.DataFrame:
    """
    合并三个 DataFrame，基于 visitor_id
    """
    print(f"\n{'='*70}")
    print(f"合并数据")
    print(f"{'='*70}")
    
    # 打印合并前的信息
    print(f"表1 记录数: {len(df1)}")
    print(f"表2 记录数: {len(df2)}")
    print(f"表3 记录数: {len(df3)}")
    
    # 以表1为基础，左连接表2和表3
    result = df1.copy()
    
    if not df2.empty:
        # 为表2的列添加前缀，避免列名冲突（除了 visitor_id）
        df2_renamed = df2.copy()
        columns_to_rename = [col for col in df2.columns if col != 'visitor_id']
        df2_renamed = df2_renamed.rename(columns={col: f'table2_{col}' for col in columns_to_rename})
        
        result = pd.merge(result, df2_renamed, on='visitor_id', how='left')
        print(f"✅ 已合并表2，新增 {len(df2_renamed.columns)-1} 列")
    else:
        print(f"⚠️  表2 为空，跳过合并")
    
    if not df3.empty:
        # 为表3的列添加前缀，避免列名冲突（除了 visitor_id）
        df3_renamed = df3.copy()
        columns_to_rename = [col for col in df3.columns if col != 'visitor_id']
        df3_renamed = df3_renamed.rename(columns={col: f'table3_{col}' for col in columns_to_rename})
        
        result = pd.merge(result, df3_renamed, on='visitor_id', how='left')
        print(f"✅ 已合并表3，新增 {len(df3_renamed.columns)-1} 列")
    else:
        print(f"⚠️  表3 为空，跳过合并")
    
    print(f"\n最终结果: {len(result)} 行 × {len(result.columns)} 列")
    
    return result


def main():
    """主函数"""
    # 配置
    api_url = 'https://monika.test.gifshow.com/plugin-api/proxy/sql'
    partition = None  # None 表示使用昨天，或指定如 '20260220'
    
    if partition is None:
        partition = get_yesterday_partition()
    
    # 解析用户ID
    user_ids = parse_user_ids(USER_IDS)
    
    print(f"╔{'='*68}╗")
    print(f"║{'多表联合查询 - 用户信息批量查询':^66}║")
    print(f"╚{'='*68}╝")
    print(f"\n📋 用户数量: {len(user_ids)}")
    print(f"📅 分区日期: {partition}")
    print(f"🔗 API地址: {api_url}")
    
    # 查询三个表
    df1 = query_table1(user_ids, partition, api_url)
    df2 = query_table2(user_ids, partition, api_url)
    df3 = query_table3(user_ids, partition, api_url)
    
    # 合并结果
    if df1.empty:
        print("\n❌ 表1（基础表）查询为空，无法继续")
        return None
    
    df_final = merge_dataframes(df1, df2, df3)
    
    # 显示结果
    print(f"\n{'='*70}")
    print(f"查询结果汇总")
    print(f"{'='*70}")
    print(f"\n前 3 条数据:")
    print(df_final.head(3))
    
    print(f"\n列名列表 (共 {len(df_final.columns)} 列):")
    for i, col in enumerate(df_final.columns, 1):
        print(f"  {i:3d}. {col}")
    
    print(f"\n数据类型:")
    print(df_final.dtypes)
    
    # 保存结果
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    csv_file = f'user_info_multi_tables_{timestamp}.csv'
    df_final.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"\n💾 CSV 已保存: {csv_file}")
    
    excel_file = f'user_info_multi_tables_{timestamp}.xlsx'
    df_final.to_excel(excel_file, index=False, engine='openpyxl')
    print(f"💾 Excel 已保存: {excel_file}")
    
    return df_final


if __name__ == '__main__':
    df = main()
