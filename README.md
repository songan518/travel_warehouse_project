Travel Data Warehouse - 旅游用户行为数仓项目
=============================================

基于Spark on YARN + Hive搭建的四层离线数仓，对旅游用户出行数据进行分层存储与加工，产出用户画像、景点热度排行、城市热度排行等业务指标。

技术栈
------

- Hadoop(HDFS)
- Hive
- Spark(PySpark) on YARN

核心功能点
----------

- 四层数仓架构：ODS -> DWD -> DWS -> ADS，分层清晰，职责明确
- 数据清洗标准化：空值过滤、日期格式统一、标签拆分、景点自动分类（7大类）
- 用户分层与画像：按年均出行频次划分高频/中频/低频用户，结合最近出行时间判断活跃/流失状态
- 环比热度分析：基于Window Lag函数计算景点和城市的季度环比增长率，识别热度上升趋势
- 复杂数据类型应用：使用array存储多值标签，map 存储城市/分类/同伴访问次数分布，减少宽表字段冗余
- ETL优化：使用broadcast join优化小表关联，cache 减少重复计算

数据说明
--------

- 数据来源：Chinese Tourism User Behavior Dataset（Figshare公开数据集）
- 数据范围：清洗后保留2011年-2021年的有效记录
- 核心数据量：用户出行记录约7万条，景区信息约2万个

数仓分层
--------

| 层级 | 说明 |
|------|------|
| ODS | 原始数据层 — 从HDFS加载CSV，保留原始字段直接映射 |
| DWD | 明细数据层 — 数据清洗、空值填充、标签拆分、分类打标、炸裂转行 |
| DWS | 服务数据层 — 按用户/景点/城市三个主题轻度聚合，构建主题宽表 |
| ADS | 应用数据层 — 用户画像、Top50景点排行榜、城市Top10排行榜 |

项目结构
--------

```
travel_warehouse_project/
├── sql/
│   ├── 01_create_databases.sql
│   ├── 02_ods_tables.sql
│   ├── 03_dwd_tables.sql
│   ├── 04_dws_tables.sql
│   └── 05_ads_tables.sql
└── etl/
    ├── 01_ods/
    │   └── 01_ods_load.py
    ├── 02_dwd/
    │   ├── 01_dwd_poi_info.py
    │   └── 02_dwd_user_travel_sequence_info.py
    ├── 03_dws/
    │   ├── 01_dws_user_travel_month.py
    │   ├── 02_dws_attraction_travel_quarter.py
    │   └── 03_dws_city_travel_quarter.py
    └── 04_ads/
        ├── 01_ads_user_portrait.py
        ├── 02_ads_top50_attractions_last_year.py
        └── 03_ads_city_top_10_last_year.py
```

产出结果
--------

| 表名 | 时间范围 | 内容 |
|------|----------|------|
| 用户画像表 | 全量数据 | 涵盖用户活跃等级（高频/中频/低频）、活跃/流失状态、最常去城市、最偏好景点分类、最常用同伴类型 |
| 景点热度Top50| 2021年每季度 |涵盖季度热门景点排名、访问总人次、环比增长率、热度上升标识、最热门出游方式 |
| 城市热度Top10 | 2021年每季度 | 涵盖季度热门城市排名、访问次数、不重复景点数、环比增长率、热度上升标识、最热门出游方式 |
| 用户月度出行宽表 | 2011-2021年 | 涵盖用户每月出行次数、游玩景点数、城市/分类/同伴偏好分布、首末次出行日期 |

待优化方向
----------

- 接入调度工具（如 Azkaban / DolphinScheduler）实现 ETL 自动化
- 引入城市-省份-区域映射表，优化区域划分精度
- 增加数据质量监控（空值率、重复率、异常值告警）
