#!/usr/bin/env python
import glob

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, expr

import typer

# Load a single CSV file
def main(memory_path, busy_path, cpu_path, request_path):
    # Initialize Spark Session
    spark = SparkSession.builder.appName("CSVLoader").getOrCreate()

    busy_df = load_csvs(spark, busy_path)
    busy_df.show(truncate=30)
    print(busy_df.count())

    memory_df = load_csvs(spark, memory_path)
    memory_df.show(truncate=30)
    print(memory_df.count())

    cpu_df = load_csvs(spark, cpu_path)
    cpu_df.show(truncate=30)
    print(cpu_df.count())

    request_df = load_csvs(spark, request_path)
    request_df.show(truncate=30)
    print(request_df.count())

def load_csvs(spark, dir_path):
    paths = glob.glob(dir_path + "/*.csv")
    return spark.read.format("csv") \
        .option("header", "True") \
        .option("inferSchema", "True") \
        .load(paths) \
        .drop("metric_kind") \
        .drop("value_type") \
        .drop("resource_type") \
        .drop("end_time") \
        .drop("metric:top_level_controller_name") \
        .drop("metric:top_level_controller_type") \
        .drop("resource:cluster") \
        .drop("resource:instance") \
        .drop("resource:job") \
        .drop("resource:location") \
        .drop("resource:namespace") \
        .drop("resource:project_id") \
        .withColumn(
            'start_time',
            date_trunc('minute', col('start_time')).cast('timestamp')
        ) \
        .sort('start_time') \
        .distinct() \


if __name__ == "__main__":
    typer.run(main)
