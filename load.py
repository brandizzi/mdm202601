#!/usr/bin/env python
import glob

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, date_trunc, expr, first, lag, last, lead, max, regexp_extract, when

import typer

def main(memory_path, busy_path, cpu_path, request_path, output_path: str = "output.parquet"):
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
    MAX_SECONDS_PER_K8S_SETTINGS = 12*60*10
    MAX_MEMORY_PER_JVM_SETTINGS = 12 * 1024 ** 3
    combined_df = combined_df \
        .transform(calculate_cpu_delta(cpu_col)) \
        .transform(normalize_cpu(pod_time_ordered, MAX_SECONDS_PER_K8S_SETTINGS)) \
        .withColumn(req_col, col(req_col) - lag(col(req_col), 1).over(pod_time_ordered)) \
        .transform(normalize_memory(pod_time_ordered, MAX_MEMORY_PER_JVM_SETTINGS)) \
        .withColumn('threshold_passed', col('memory_rate') >= 0.8)

    combined_df.show(truncate=40)
    print(combined_df.count())
    print(combined_df.agg(max(col('cpu_rate'))).collect())
    combined_df.filter(col('threshold_passed')).show(truncate=40)
    combined_df.write.mode('overwrite').parquet(output_path)
    return combined_df


def calculate_cpu_delta(col_name):
    forward_window = Window.partitionBy('pod_name').orderBy('start_time').rowsBetween(0, Window.unboundedFollowing)
    prev_window = forward_window.rowsBetween(Window.unboundedPreceding, -1)
    def transform(df):
        return df \
            .withColumn('_prev', last(col(col_name), ignorenulls=True).over(prev_window)) \
            .withColumn('_diff', when(col(col_name).isNotNull(), col(col_name) - col('_prev'))) \
            .withColumn(col_name, first(col('_diff'), ignorenulls=True).over(forward_window)) \
            .drop('_prev', '_diff')
    return transform


def normalize_memory(ordered_window, max_memory_bytes):
    def transform(df):
        return df \
            .withColumn('target_memory', lead("jvm_memory_used_bytes", 5).over(ordered_window)) \
            .withColumn('memory_rate', col('jvm_memory_used_bytes') / max_memory_bytes) \
            .withColumn('target_memory_rate', col('target_memory') / max_memory_bytes)
    return transform

def normalize_cpu(ordered_window, max_cpu_rate):
    def transform(df):
        return df \
            .withColumn('cpu_rate', col('process_cpu_seconds_total') / max_cpu_rate)
    return transform

def pivot_metric(df):
    metric_name = df.select(
        regexp_extract(col('metric_type'), r'/([^/]+)/[^/]+$', 1)
    ).first()[0]
    return df.select(
        col('start_time'),
        col('metric:pod').alias('pod_name'),
        col('value').alias(metric_name)
    )

def load_csvs(spark, dir_path, expand=True):
    if expand:
        paths = glob.glob(dir_path + "/*.csv")
    else:
        paths = [dir_path]
    df = spark.read.format("csv") \
        .option("header", "True") \
        .option("inferSchema", "True") \
        .load(paths)
    # Ignore duplicate headers from merged files
    df = df \
        .filter(col('metric_type') != 'metric_type')
    # Drop unnecessary columns
    df = df \
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
        .drop("resource:project_id")
    # Round down to the nearest even minute
    df = df \
        .withColumn(  
            'start_time',
            (date_trunc('minute', col('start_time')) - expr("(minute(start_time) % 2) * interval 1 minute")).cast('timestamp')
        ) \
        .sort('start_time', ascending=True) \
        .distinct()
    return df


if __name__ == "__main__":
    typer.run(main)
