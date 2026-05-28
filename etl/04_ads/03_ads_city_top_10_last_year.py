"""
ads层-城市热度排行榜
功能：
 1. 读取dws层城市热度宽表（最新一年）
 2. 每季度排名取前10
 3. 计算最热门的出行方式
 4. 写入ads表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("ads_city_top_10_last_year").\
        master("yarn").\
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://node1:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        getOrCreate()

#1. 导入数据
df_city_tmp = spark.table("dws.dws_city_travel_quarter").\
    filter(F.col("travel_year") == 2021).\
    withColumn("year_quarter",
        F.concat("travel_year", F.lit("Q"), "travel_quarter"))

#2. 取出去年每个季度前十
df_city = df_city_tmp.withColumn("rank",
    F.row_number().over(Window.partitionBy("year_quarter").orderBy(F.col("visit_trip_cnt").desc()))
).filter(F.col("rank") <= 10)

#3. top_partner
df_partner_exploded_tmp = df_city.select(
    "year_quarter", "city", F.explode("partner_map")
).toDF("year_quarter", "city", "partner", "cnt")

df_favorite_partner = df_partner_exploded_tmp.withColumn(
    "rank_tmp", F.dense_rank().over(Window.partitionBy(F.col("year_quarter"),F.col("city")).\
                                orderBy(F.col("cnt").desc()))
).filter(F.col("rank_tmp") == 1).\
    groupby("year_quarter", "city").\
    agg(F.collect_list("partner").alias("top_partner")).\
    select("year_quarter", "city", "top_partner")

#4. 汇总
df_final = df_city.\
    join(df_favorite_partner, on = ["year_quarter", "city"], how = "left").\
    select(
    "year_quarter",
    "city",
    "visit_trip_cnt", "unique_attraction_cnt",
    "top_partner", "rank", "is_hot_rising"
).orderBy("year_quarter", "rank")

"""测试数据
print("表前十行")
df_final.show(10, truncate=False)
"""

df_final.write.mode("overwrite").saveAsTable("ads.ads_city_top10_last_year")

spark.stop()
