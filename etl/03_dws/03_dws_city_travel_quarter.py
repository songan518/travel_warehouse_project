"""
dws层城市季度热度主题表脚本
功能：
 1. 关联dwd层表
 2. 提取季度
 3. 计算基础热度指标
 4. 计算游客同伴类型map
 5. 计算环比变化指标
 6. 合并写入dws表
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

if __name__ == "__main__":
    spark = SparkSession.builder. \
        appName("dws_city_travel_quarter"). \
        master("yarn"). \
        enableHiveSupport(). \
        config("spark.hadoop.hive.metastore.uris", "thrift://node1:9083"). \
        config("spark.sql.warehouse.dir", "/user/hive/warehouse"). \
        getOrCreate()

#1. 关联表 取出城市字段
df_travel = spark.table("dwd.dwd_user_travel_sequence_info")
df_poi = spark.table("dwd.dwd_poi_info").select(
    "poi_id", "name", "city"
)
df_joined = df_travel.join(
    F.broadcast(df_poi), on = "poi_id", how = "inner"
)

#2. 提取季度
df_joined = df_joined.withColumn(
    "travel_quarter", F.ceil(F.col("travel_month") / 3))

#3. 计算基础指标
df_agg = df_joined.groupby(
    "travel_year", "travel_quarter", "city"
).agg(
    #热度指标
    F.count("trip_id").alias("visit_trip_cnt"),
    F.countDistinct("poi_id").alias("unique_attraction_cnt"),
)

#4. 游客特征
df_partner_tmp = df_joined.groupby(
    "city", "travel_year", "travel_quarter", "travel_partners"
).agg(
    F.count("*").alias("partner_visit_cnt")
)

df_partners_map = df_partner_tmp.groupby(
    "city", "travel_year", "travel_quarter"
).agg(
    F.map_from_entries(
        F.collect_list(F.struct("travel_partners", "partner_visit_cnt"))
    ).alias("partner_map")
)

#5. 环比变化指标
df_base_temp = df_agg.select(
    "city", "travel_year", "travel_quarter", "visit_trip_cnt"
)

window_lag_tmp = Window.partitionBy("city").orderBy("travel_year", "travel_quarter")

df_with_prev = df_base_temp.\
    withColumn(
    "prev_visit_trip_cnt",
    F.lag("visit_trip_cnt",1).over(window_lag_tmp)
).\
    withColumn(
    "visit_cnt_growth_rate",
    F.when(
        (F.col("prev_visit_trip_cnt").isNotNull()) & (F.col("prev_visit_trip_cnt") > 0),
        F.round((F.col("visit_trip_cnt") - F.col("prev_visit_trip_cnt")) / F.col("prev_visit_trip_cnt"), 4)
    ).otherwise(0)
).\
    withColumn(
    "is_hot_rising",
    F.when(F.col("prev_visit_trip_cnt").isNull(), "无上期数据").\
    when(F.col("visit_cnt_growth_rate") > 0.3, "是").\
    otherwise("否")
).select(
    "city", "travel_year", "travel_quarter", "is_hot_rising","visit_cnt_growth_rate"
)

#7. 合并写入
df_final = df_agg.\
    join(df_partners_map, on = ["city", "travel_year", "travel_quarter"], how = "left").\
    join(df_with_prev, on = ["city", "travel_year", "travel_quarter"],how = "left").\
    select(
    "city",
    "visit_trip_cnt", "unique_attraction_cnt",
    "partner_map",
    "visit_cnt_growth_rate", "is_hot_rising",
    "travel_year", "travel_quarter"
)

"""测试数据
print("表前十行")
df_final.show(10, truncate=False)
"""

#"""
df_final.write.mode("overwrite").partitionBy("travel_year", "travel_quarter").\
    saveAsTable("dws.dws_city_travel_quarter")
#"""
spark.stop()
