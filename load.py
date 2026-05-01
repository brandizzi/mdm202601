#!/usr/bin/env python
import glob

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, date_trunc, expr, first, lag, last, lead, regexp_extract, when

import typer

def main(memory_path, busy_path, cpu_path, request_path):
    spark = SparkSession.builder.appName("CSVLoader").getOrCreate()

    busy_df = load_csvs(spark, busy_path)
    memory_df = load_csvs(spark, memory_path) \
        .filter(col('metric:area') == 'heap') \
        .drop("metric:area")
    cpu_df = load_csvs(spark, cpu_path)
    request_df = load_csvs(spark, request_path)

    cpu_col = pivot_metric(cpu_df).columns[2]
    req_col = pivot_metric(request_df).columns[2]

    combined_df = pivot_metric(busy_df) \
        .join(pivot_metric(memory_df), on=['start_time', 'pod_name'], how='outer') \
        .join(pivot_metric(cpu_df), on=['start_time', 'pod_name'], how='outer') \
        .join(pivot_metric(request_df), on=['start_time', 'pod_name'], how='outer') \
        .sort('start_time', 'pod_name')

    combined_df = combined_df.repartition('pod_name')

    pod_time_window = Window.partitionBy('pod_name').orderBy('start_time').rowsBetween(Window.unboundedPreceding, 0)
    pod_time_ordered = Window.partitionBy('pod_name').orderBy('start_time')
    pod_time_forward = Window.partitionBy('pod_name').orderBy('start_time').rowsBetween(0, Window.unboundedFollowing)
    cpu_prev_window = Window.partitionBy('pod_name').orderBy('start_time').rowsBetween(Window.unboundedPreceding, -1)
    combined_df = combined_df \
        .withColumn('_cpu_prev', last(col(cpu_col), ignorenulls=True).over(cpu_prev_window)) \
        .withColumn('_cpu_diff', when(col(cpu_col).isNotNull(), col(cpu_col) - col('_cpu_prev'))) \
        .withColumn(cpu_col, first(col('_cpu_diff'), ignorenulls=True).over(pod_time_forward)) \
        .drop('_cpu_prev', '_cpu_diff') \
        .withColumn(req_col, col(req_col) - lag(col(req_col), 1).over(pod_time_ordered)) \
        .withColumn('target_memory', lead("jvm_memory_used_bytes", 5).over(pod_time_ordered)) \
        .withColumn('memory_rate', col('jvm_memory_used_bytes') / (12 * 1024 ** 3)) \
        .withColumn('target_memory_rate', col('target_memory') / (12 * 1024 ** 3))

    combined_df.show(truncate=40)
    print(combined_df.count())
    return combined_df


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
