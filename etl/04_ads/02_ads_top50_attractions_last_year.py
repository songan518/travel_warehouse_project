"""
ads层-热门景点排行榜
功能：
 1. 读取dws层景点热度宽表
 2. 按季度计算景点热度排名
 3. 取每季度前50名
 4. 写入ads表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("ads_top50_attractions_last_year").\
        master("yarn").\
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://node1:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        getOrCreate()

#1. 导入数据
df_attraction = spark.table("dws.dws_attraction_travel_quarter").\
    filter(F.col("travel_year") == 2021).\
    withColumn("year_quarter",
        F.concat("travel_year", F.lit("Q"), "travel_quarter"))

#2. 根据该季度景区的被游览次数排名 取前五十
df_attraction = df_attraction.withColumn("rank",
    F.row_number().over(Window.partitionBy(F.col("year_quarter")).orderBy(F.col("visit_total_cnt").desc()))
).\
    filter(F.col("rank") <= 50)

#3. top_partner
df_partner_exploded_tmp = df_attraction.select(
    "year_quarter", "poi_id", F.explode("partner_map")
).toDF("year_quarter", "poi_id", "partner", "cnt")

df_favorite_partner = df_partner_exploded_tmp.withColumn(
    "rank_tmp", F.dense_rank().over(Window.partitionBy(F.col("year_quarter"),F.col("poi_id")).\
                                orderBy(F.col("cnt").desc()))
).filter(F.col("rank_tmp") == 1).\
    groupby("year_quarter", "poi_id").\
    agg(F.collect_list("partner").alias("top_partner")).\
    select("year_quarter", "poi_id", "top_partner")

#4. 汇总
df_final = df_attraction.\
    join(df_favorite_partner, on=["year_quarter", "poi_id"], how="left").\
    select(
    "year_quarter",
    "poi_id","name", "city", "category",
    "visit_total_cnt", "top_partner",
    "rank", "is_hot_rising", "visit_cnt_growth_rate"
)

"""测试数据
print("表前十行")
df_final.show(10, truncate=False)
"""

df_final.write.mode("overwrite").saveAsTable("ads.ads_top50_attractions_last_year")

spark.stop()
