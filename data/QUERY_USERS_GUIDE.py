#!/usr/bin/env python3
"""
使用说明文档 - 批量查询用户信息

脚本: query_users_info.py
"""

print("""
╔══════════════════════════════════════════════════════════════╗
║         用户信息批量查询工具 - 使用说明                     ║
╚══════════════════════════════════════════════════════════════╝

📋 功能说明
----------
批量查询 459 个用户的设备信息等基础信息，支持：
• 自动解析用户ID列表
• 通过 Monika SQL API 查询 ClickHouse
• 可选：直接连接 ClickHouse
• JSON 自动转换为 DataFrame
• 导出为 CSV 和 Excel

🚀 快速开始
----------
1. 安装依赖:
   pip install -r requirements.txt

2. 运行脚本:
   python query_users_info.py

3. 查看结果:
   - user_info_YYYYMMDD_HHMMSS.csv
   - user_info_YYYYMMDD_HHMMSS.xlsx

⚙️ 配置说明
----------
编辑 query_users_info.py 的 main() 函数中的 config:

方式1: 使用 Monika API (推荐)
    config = {
        'method': 'monika',
        'api_url': 'https://monika.test.gifshow.com/plugin-api/proxy/sql',
        'db_type': 'ck',  # 或 'hive'
        'partition': None,  # None=昨天, 或指定 '20260220'
    }

方式2: 直连 ClickHouse
    config = {
        'method': 'clickhouse',
        'host': 'your-clickhouse-host',
        'port': 9000,
        'user': 'default',
        'password': 'your-password',
        'database': 'default',
        'partition': '20260220',
    }

📊 查询字段
----------
基础信息:
  - visitor_id, device_id, product
  - is_gid_15d, is_new_device

设备信息:
  - platform, model, device_brand, system_version
  - applist, device_factor

网络信息:
  - ip, ipc

地域信息:
  - country_name, province_name, city_name

账户信息:
  - max_global_id, user_name
  - is_v, is_verified
  - gift_num, charge_num
  - fans_user_num, friend_user_num
  - reg_date, reg_day

用户行为:
  - keyword_set
  - upload_phone_cnt_7d
  - photo_camera_upload_cnt_7d

🔧 常见问题
----------
Q: 查询结果为空？
A: 检查以下几点:
   1. 分区日期是否正确 (默认昨天)
   2. API 是否需要认证
   3. 用户ID是否存在于该分区

Q: API 认证失败？
A: Monika API 可能需要登录，尝试:
   1. 在浏览器登录 monika 平台
   2. 复制 Cookie 到脚本
   3. 或使用直连 ClickHouse 方式

Q: 如何修改查询字段？
A: 编辑 build_user_info_sql() 函数中的 SELECT 语句

Q: 如何查询不同的分区？
A: 设置 config['partition'] = '20260220'

📝 示例输出
----------
📋 共解析到 459 个用户ID
查询用户信息
============================================================
用户数量: 459
分区日期: 20260220
查询方式: monika

📡 通过 Monika API 查询
✅ 查询成功，返回 459 条记录

查询结果
============================================================
✅ 成功查询 459 条记录

💾 CSV 已保存: user_info_20260221_123456.csv
💾 Excel 已保存: user_info_20260221_123456.xlsx

🔗 相关资源
----------
• Monika 平台: https://monika.test.gifshow.com
• 项目文档: ./README.md
• API 文档: ./src/services/sqlApi.ts

💡 提示
------
• 默认查询昨天的数据
• 支持批量查询，一次最多建议 1000 个用户
• 结果自动导出为 CSV 和 Excel 两种格式
• 如遇到问题，查看终端的详细错误信息
""")
