"""
dwd层景区维度清洗脚本
功能：
 1. 读取ods层数据
 2. 过滤空值
 3. 填充空标签、空字符串
 4. 标签拆分为数组
 5. 景点分类打标
 6. 写入dwd表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("dwd_poi_info").\
        master("yarn").\
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://node1:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        getOrCreate()

#1. 读取ods层数据,初步筛掉与分析无关的各种英文名称
df = spark.table("ods.ods_poi_dim").\
    select(
    "poi_id",
    "name_zh",
    "city_zh",
    "label_zh"
)

""" 验证数据：
print("原始数据前5行")
df.show(5, truncate=False)
print(f"原始行总数：{df.count()}")
"""

#2. 过滤空值
df = df.dropna(
    subset=["poi_id", "name_zh", "city_zh"],
    how="any"
)

""" 验证数据：
print(f"过滤后行总数：{df.count()}")
"""

#3. 填空标签
df = df.withColumn("label_zh",
    F.when(F.trim(F.col("label_zh")) == "", F.lit(None))
    .otherwise(F.col("label_zh"))
)

df = df.fillna(
    value = "无标签", subset=["label_zh"]
)

"""验证数据
print(f"当前总行数: {df.count()}")
print(f"label_zh非空行数: {df.select('label_zh').count()}")
"""

#4. 标签转为数组形式，切割分号
df = df.withColumn(
    "label", F.split(F.col("label_zh"), ";")
)

"""验证数据
print("转换后数据格式")
df.select("poi_id", "label_zh", "label").show(5, truncate=False)
"""

#5. 浅层次划分标签
df = df.withColumn("category",
    F.when(F.col("label_zh").rlike("乐园|游乐场|漂流|拓展|骑行|露营|欢乐谷|方特|主题体验"), "主题游乐")
    .when(F.col("label_zh").rlike("博物馆|美术馆|纪念馆|展览馆|科技馆|影视基地|故居|名人故居"), "文化场馆")
    .when(F.col("label_zh").rlike("古镇|古城|古建|古建筑|寺庙|石窟|园林|古村落|城墙|文物古迹|历史遗址|古战场|老城|文庙|道观"), "历史人文古迹")
    .when(F.col("label_zh").rlike("城市地标|地标建筑|城市标志建筑|高楼|摩天轮|网红打卡点"), "都市地标")
    .when(F.col("label_zh").rlike("公园|商圈|步行街|广场|绿地|城市公园"), "都市休闲")
    .when(F.col("label_zh").rlike("乡村|农庄|农家乐|采摘|田园|民俗村"), "乡村生态")
    .when(F.col("label_zh").rlike("山|河|江|湖|海|瀑布|草原|峡谷|海滩|海岛|红树林|温泉|生态|山水|风景|自然景观|湖泊|江河|溪流|湿地|森林|花海|赏花"), "自然风光")
    .when(F.col("label_zh") == "无标签", "未分类")
    .otherwise("其他景点")
)

"""验证数据
print("查看标签分布")
df.groupBy("category").count().show()
"""

#6. 匹配建表字段准备写入
df_final = df.select(
    "poi_id",
    F.col("name_zh").alias("name"),
    F.col("city_zh").alias("city"),
    "label",
    "category"
)

df_final.write.mode("overwrite").saveAsTable("dwd.dwd_poi_info")

spark.stop()
