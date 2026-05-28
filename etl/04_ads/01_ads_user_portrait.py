"""
ads层-用户画像表
功能：
 1. 读取dws层用户月表
 2. 聚合全量基础指标
 3. 计算偏好标签（城市/分类/同伴）
 4. 计算活跃度标签
 5. 写入ads表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("ads_user_portrait").\
        master("yarn").\
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://node1:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        getOrCreate()

#1. 导入数据
df_user = spark.table("dws.dws_user_travel_month").cache()

#2. 基础指标聚合
df_base = df_user.groupby("blog_id").agg(
    F.sum("trip_total_cnt").alias("total_trips"),
    F.sum("total_spot_cnt").alias("total_spots"),
    F.max("last_trip_date").alias("last_trip_date"),

    F.countDistinct("travel_year").alias("active_years")
)

#3. favorite_city
df_city_exploded_tmp = df_user.select(
    "blog_id", F.explode("city_visit_map")
).toDF("blog_id", "city", "cnt")

df_city_total_tmp = df_city_exploded_tmp.groupby("blog_id", "city").agg(
    F.sum("cnt").alias("total_cnt")
)

df_favorite_city = df_city_total_tmp.withColumn(
    "rank", F.dense_rank().over(Window.partitionBy(F.col("blog_id")).orderBy(F.col("total_cnt").desc()))
).filter(F.col("rank") == 1).\
    groupby("blog_id").\
    agg(F.collect_list("city").alias("favorite_city"))

#4. favorite_category
df_cate_exploded_total_tmp = df_user.select(
    "blog_id", F.explode("category_visit_map")
).toDF("blog_id", "cate", "cnt").groupby("blog_id", "cate").agg(
    F.sum("cnt").alias("total_cnt")
)

df_favorite_cate = df_cate_exploded_total_tmp.withColumn(
    "rank", F.dense_rank().over(Window.partitionBy(F.col("blog_id")).orderBy(F.col("total_cnt").desc()))
).filter(F.col("rank") == 1).\
    groupby("blog_id").\
    agg(F.collect_list("cate").alias("favorite_category"))

#5. favorite_partner
df_partner_exploded_total_tmp = df_user.select(
    "blog_id", F.explode("partner_visit_map")
).toDF("blog_id", "partner", "cnt").groupby("blog_id", "partner").agg(
    F.sum("cnt").alias("total_cnt")
)

df_favorite_partner = df_partner_exploded_total_tmp.withColumn(
    "rank", F.dense_rank().over(Window.partitionBy(F.col("blog_id")).orderBy(F.col("total_cnt").desc()))
).filter(F.col("rank") == 1).\
    groupby("blog_id").\
    agg(F.collect_list("partner").alias("favorite_partner"))

#6. user_status
#current_date_str = datetime.now().DATE_FORMAT("%Y-%m-%d")
current_time = "2022-01-01"

df_user_status = df_base.select("blog_id", "last_trip_date").withColumn(
    "days_since_last_trip",
    F.datediff(F.lit(current_time), F.col("last_trip_date"))
).withColumn(
    "user_status",
    F.when(F.col("days_since_last_trip") > 365, "流失").otherwise("活跃")
).select("blog_id", "user_status")

#7. user_level
df_user_level = df_base.select("blog_id", "total_trips", "active_years").\
    withColumn("avg_trip_per_year",
    F.when(F.col("active_years") > 0, F.col("total_trips") / F.col("active_years")).\
    otherwise(0)
).withColumn("user_level",
    F.when(F.col("avg_trip_per_year") >= 3, "高频").\
    when(F.col("avg_trip_per_year") >= 1, "中频").otherwise("低频")
).select("blog_id", "user_level")

#8. 写入表
df_final = df_base.\
    join(df_favorite_city, on = "blog_id", how = "left").\
    join(df_favorite_cate, on = "blog_id", how = "left").\
    join(df_favorite_partner, on = "blog_id", how = "left").\
    join(df_user_status, on = "blog_id", how = "left").\
    join(df_user_level, on = "blog_id", how = "left").\
    select(
    "blog_id",
    "total_trips", "total_spots",
    "favorite_city", "favorite_category", "favorite_partner",
    "user_level", "user_status"
)

"""测试数据
print("表前十行")
df_final.show(10, truncate=False)
"""

df_final.write.mode("overwrite").saveAsTable("ads.ads_user_portrait")

df_user.unpersist()
spark.stop()
