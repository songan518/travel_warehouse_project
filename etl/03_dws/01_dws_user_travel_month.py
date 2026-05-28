"""
dws层用户月度出行主题宽表
功能：
 1. 关联dwd层表
 2. 计算月度基础指标
 3. 计算三个偏好map
 4. 写入dws表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("dws_user_travel_month").\
        master("yarn").\
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://node1:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        getOrCreate()

#1. 关联表
df_travel = spark.table("dwd.dwd_user_travel_sequence_info")
df_poi = spark.table("dwd.dwd_poi_info").select("poi_id", "city", "category")

df_joined = df_travel.join(
    F.broadcast(df_poi), on=["poi_id"], how="left").cache()

#2. 基础计数指标
df_base = df_joined.groupby(
    "blog_id", "travel_year", "travel_month"
).agg(
    F.countDistinct("trip_id").alias("trip_total_cnt"),
    F.count("*").alias("total_spot_cnt"),
    #时间特征
    F.min("travel_date").alias("first_trip_date"),
    F.max("travel_date").alias("last_trip_date")
)

#3. 城市访问次数map
df_city_tmp = df_joined.groupby(
    "blog_id", "travel_year", "travel_month", "city"
).agg(
    F.count("*").alias("city_visit_cnt")
)

df_city_map = df_city_tmp.groupby(
    "blog_id", "travel_year", "travel_month"
).agg(
    F.map_from_entries(
        F.collect_list(F.struct("city", "city_visit_cnt"))
    ).alias("city_visit_map")
)

#4. 景点分类访问次数map
df_cate_tmp = df_joined.groupby(
    "blog_id", "travel_year", "travel_month", "category"
).agg(
    F.count("*").alias("cate_visit_cnt")
)

df_cate_map = df_cate_tmp.groupby(
    "blog_id", "travel_year", "travel_month"
).agg(
    F.map_from_entries(
        F.collect_list(F.struct("category", "cate_visit_cnt"))
    ).alias("category_visit_map")
)

#5. 同伴类型访问次数map
df_partner_tmp = df_joined.groupby(
    "blog_id", "travel_year", "travel_month", "travel_partners"
).agg(
    F.count("*").alias("partner_visit_cnt")
)

df_partners_map = df_partner_tmp.groupby(
    "blog_id", "travel_year", "travel_month"
).agg(
    F.map_from_entries(
        F.collect_list(F.struct("travel_partners", "partner_visit_cnt"))
    ).alias("partner_visit_map")
)

#6. 聚合
df_base = df_base.\
    join(df_city_map, on=["blog_id", "travel_year", "travel_month"], how = "left").\
    join(df_cate_map, on=["blog_id", "travel_year", "travel_month"], how = "left").\
    join(df_partners_map, on=["blog_id", "travel_year", "travel_month"], how = "left")


#7. 插入数据
df_final = df_base.select(
    "blog_id",
    "trip_total_cnt", "total_spot_cnt",
    "city_visit_map","category_visit_map", "partner_visit_map",
    "first_trip_date", "last_trip_date",
    "travel_year", "travel_month"
)

"""测试数据
print("表前十行")
df_final.show(10, truncate=False)
"""

#8. 写入表
df_final.write.mode("overwrite").\
    partitionBy(
    "travel_year", "travel_month"
).saveAsTable("dws.dws_user_travel_month")

df_joined.unpersist()
spark.stop()
