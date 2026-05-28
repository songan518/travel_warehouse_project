"""
ODS层数据加载脚本
功能：从HDFS加载CSV数据到ODS层
"""

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.\
        appName("ods_load_from_hdfs").\
        enableHiveSupport().\
        master("yarn"). \
        config("spark.hadoop.hive.metastore.uris", "thrift://node1:9083").\
        config("spark.sql.warehouse.dir", "/user/hive/warehouse").\
        config("spark.yarn.jars", "hdfs://node1/spark/jars/*.jar"). \
        getOrCreate()

    poi_path = "hdfs://node1/travel/ods/source/POIs_V2.csv"
    user_path = "hdfs://node1/travel/ods/source/Visit_Sequences_V2.csv"

#1. POI维度表
    df_poi = spark.read.csv(poi_path, header=True, encoding="utf-8").\
        selectExpr(
        "Encrypted_ID AS poi_id",
        "Name_ZH AS name_zh",
        "Name_EN AS name_en",
        "City_ZH AS city_zh",
        "City_EN AS city_en",
        "Latitude_GCJ02 AS latitude",
        "Longitude_GCJ02 AS longitude",
        "Label_ZH AS label_zh",
        "Label_EN AS label_en"
    )

    """
    验证数据
    print("poi表前十行")
    df_poi.show(10, truncate=False)
    print(f"poi表总行数:{df_poi.count()}")
    """

    df_poi.write.mode("overwrite").saveAsTable("ods.ods_poi_dim")

#2. 用户轨迹表
    df_user = spark.read.csv(user_path, header=True, encoding="utf-8"). \
        selectExpr(
        "Anonymized_Blog_ID AS blog_id",
        "Retrieval_Date AS retrieval_date",
        "Departure_Date AS departure_date",
        "Travel_Partners AS travel_partners",
        "Visit_Sequence AS visit_sequence"
    )

    """
    验证数据
    print("user表前十行")
    df_user.show(10, truncate=False)
    print(f"user表总行数:{df_user.count()}")
    """

    df_user.write.mode("overwrite").saveAsTable("ods.ods_user_travel_sequence")
    
    spark.stop()
