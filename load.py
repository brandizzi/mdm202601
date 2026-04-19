#!/usr/bin/env python
import glob

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, date_trunc, expr, last, regexp_extract

import typer

def main(memory_path, busy_path, cpu_path, request_path):
    spark = SparkSession.builder.appName("CSVLoader").getOrCreate()

    busy_df = load_csvs(spark, busy_path)
    memory_df = load_csvs(spark, memory_path)
    cpu_df = load_csvs(spark, cpu_path)
    request_df = load_csvs(spark, request_path)

    cpu_col = pivot_metric(cpu_df).columns[2]

    combined_df = pivot_metric(busy_df) \
        .join(pivot_metric(memory_df), on=['start_time', 'pod_name'], how='outer') \
        .join(pivot_metric(cpu_df), on=['start_time', 'pod_name'], how='outer') \
        .join(pivot_metric(request_df), on=['start_time', 'pod_name'], how='outer') \
        .sort('start_time', 'pod_name')

    pod_time_window = Window.partitionBy('pod_name').orderBy('start_time').rowsBetween(Window.unboundedPreceding, 0)
    combined_df = combined_df.withColumn(cpu_col, last(col(cpu_col), ignorenulls=True).over(pod_time_window))

    combined_df.show(truncate=40)
    print(combined_df.count())


def pivot_metric(df):
    metric_name = df.select(
        regexp_extract(col('metric_type'), r'/([^/]+)/[^/]+$', 1)
    ).first()[0]
    return df.select(
        col('start_time'),
        col('metric:pod').alias('pod_name'),
        col('value').alias(metric_name)
    )

def load_csvs(spark, dir_path):
    paths = glob.glob(dir_path + "/*.csv")
    return spark.read.format("csv") \
        .option("header", "True") \
        .option("inferSchema", "True") \
        .load(paths) \
        .filter(col('metric_type') != 'metric_type') \
        .drop("metric_kind") \
        .drop("value_type") \
        .drop("resource_type") \
        .drop("end_time") \
        .drop("metric:area") \
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
            (date_trunc('minute', col('start_time')) - expr("(minute(start_time) % 2) * interval 1 minute")).cast('timestamp')
        ) \
        .sort('start_time', ascending=True) \
        .distinct()


if __name__ == "__main__":
    typer.run(main)
