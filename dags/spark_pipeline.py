import json
import pandas as pd
import pyspark as ps

from pyspark.sql.functions import col, count, countDistinct, collect_set

spark = (ps.sql.SparkSession
            .builder
            .appName("sandbox")
            .config('spark.driver.extraClassPath', '/home/jovyan/work/driver/sqlite-jdbc-3.32.3.2.jar')
            .getOrCreate()
        )

properties = {
    'driver': 'org.sqlite.JDBC',
    'url': 'jdbc:sqlite:data_warehouse.db',
}

df_dim_course = spark.read\
    .format('jdbc')\
    .option('driver', properties['driver']) \
    .option('url', properties['url']) \
    .option('dbtable', 'dim_course') \
    .load()

df_fact_logged_student = spark.read\
    .format('jdbc')\
    .option('driver', properties['driver']) \
    .option('url', properties['url']) \
    .option('dbtable', 'fact_logged_student') \
    .load()

# reading all events files
df_events = spark.read.json("../data/part-*.json")

# get top courses
df_events.groupBy("custom_2").count().orderBy(col("count").desc()).show(truncate=False)