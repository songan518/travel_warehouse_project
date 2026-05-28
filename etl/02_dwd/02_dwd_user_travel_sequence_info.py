"""
dwd层用户出行清洗脚本
功能：
 1. 读取ods层数据
 2. 过滤空值
 3. 填补缺失值
 4. 日期转换并提取年月
 5. 去重
 6. 剔除无效年份数据
 7. 生成旅程id
 8. 炸开visit_sequence
 9. 写入dwd表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("dwd_user_travel_sequence_info").\
        master("yarn").\
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://node1:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        getOrCreate()

#1. 读取数据
df = spark.table("ods.ods_user_travel_sequence").\
    select(
    "blog_id",
    "departure_date",
    "travel_partners",
    "visit_sequence"
)

""" 验证数据：
print("原始数据前5行")
df.show(5, truncate=False)
print(f"原始行总数：{df.count()}")
"""

#2. 过滤空值
df = df.dropna(
    subset= ["blog_id", "departure_date", "visit_sequence"]
)

""" 验证数据：
print(f"过滤后行总数：{df.count()}")
"""

#3. 填补缺失值
df = df.fillna(
    value = "Unrecorded", subset=["travel_partners"]
)

""" 验证数据：
print(f"填补缺失值后空值行总数：{df.filter(F.col('travel_partners').isNull()).count()}")
"""

#4. 转换日期数据类型,并提取出年月
df = df.withColumn("travel_date",
    F.coalesce(
        F.to_date(F.col("departure_date"), "yyyy/M/d"),
        F.to_date(F.col("departure_date"), "yyyy-MM-dd"),
        F.to_date(F.col("departure_date"), "yyyyMMdd")
    )
)
df = df.withColumn("travel_year",F.year(F.col("travel_date")))
df = df.withColumn("travel_month",F.month(F.col("travel_date")))

""" 检验数据：
print("判断是否存在转换失败的数据")
df.filter(F.col("travel_date").isNull()).select(F.col("departure_date")).show()
#之后具体内容具体转换
"""

#5. 去重
df = df.dropDuplicates(["blog_id", "travel_date", "visit_sequence"])

""" 检验数据
print("判断年份区间，保留有效数据")
df.groupby("travel_year").count().orderBy("travel_year").show(100)
"""

#6. 剔除无用数据
df = df.filter((F.col("travel_year") >= 2011) & (F.col("travel_year") < 2022))

""" 检验数据
print("查看过滤后当前年份区间")
df.selectExpr(
        "min(travel_year) min_year",
        "max(travel_year) max_year"
).show()
"""

#7. 生成旅程id
df = df.withColumn("trip_id", F.concat_ws("_", F.col("blog_id"), F.col("departure_date")))

#8. 炸开visit_sequence
df = df.withColumn("poi_id", F.explode(F.split(F.col("visit_sequence"), ";")))
df = df.filter(F.trim(F.col("poi_id")) != "")

#9. 匹配建表字段准备写入
df_final = df.select(
    "blog_id",
    "trip_id",
    "travel_date",
    "travel_year",
    "travel_month",
    "travel_partners",
    "poi_id"
)

"""测试数据
print("表前十行")
df_final.show(10, truncate=False)
"""

df_final.write.mode("overwrite").\
    partitionBy("travel_year", "travel_month").\
    saveAsTable("dwd.dwd_user_travel_sequence_info")

spark.stop()
