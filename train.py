import sys

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

def main(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("CSVLoader").getOrCreate()
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    # Shorten column names
    df = shorten_column_names(df)

    # Normalize timestamp and calculate threads rate
    MAX_THREADS_PER_TOMCAT_SETTINGS = 200
    df = (
        df.withColumn("start_time", F.to_timestamp("start_time"))
        .withColumn("threads_rate", F.col("threads_busy") / F.lit(MAX_THREADS_PER_TOMCAT_SETTINGS))
        
    )

    # Sort per pod
    w_order = Window.partitionBy("pod_name").orderBy("start_time")


    # Add lags to rows
    LAGS = 5
    df = add_lags(df, w_order, LAGS)
   

    # Calculate rolling statistics from the lagged values
    w_rolling = Window.partitionBy("pod_name").orderBy("start_time").rowsBetween(-LAGS, 0)

    df = add_stats_features(df, w_rolling)

    # Calculate deltas
    df = add_deltas(df, w_order)

    # Create features per request
    df = (
        df
        .withColumn("mem_per_req", F.col("mem_bytes") / (F.col("request_count") + 1))
        .withColumn("cpu_per_req", F.col("cpu_rate") / (F.col("request_count") + 1))
    )

    # Calculate labels for future values
    
    df = df.withColumn(
        "label",
        (F.col("memory_rate") >= 0.8).cast("int")
    )


    df = df.dropna()
    # Assemble features into a vector
    df = assemble_features(df, LAGS)

    # Sampling by pods
    pods = df.select("pod_name").distinct()
    # Step 1: pod split
    train_pods, test_pods = pods.randomSplit([0.8, 0.2], seed=42)

    train_df = df.join(train_pods, "pod_name")
    test_df  = df.join(test_pods,  "pod_name")
    
    model = GBTClassifier(
        labelCol="label",
        featuresCol="features",
        maxIter=50
    )

    model = model.fit(train_df)

    predictions = model.transform(test_df)

    binary_eval = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    multi_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")

    auc        = binary_eval.setMetricName("areaUnderROC").evaluate(predictions)
    accuracy   = multi_eval.setMetricName("accuracy").evaluate(predictions)
    precision  = multi_eval.setMetricName("weightedPrecision").evaluate(predictions)
    recall     = multi_eval.setMetricName("weightedRecall").evaluate(predictions)
    f1         = multi_eval.setMetricName("f1").evaluate(predictions)

    print(f"AUC-ROC:   {auc:.4f}")
    print(f"Accuracy:  {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print(f"F1:        {f1:.4f}")

    predictions.select("pod_name", "start_time", "label", "prediction", "probability").show(truncate=False)

def assemble_features(df, LAGS):
    FEATURE_PREFIXES = ["mem", "cpu", "req", "thr"]
    feature_cols = [
        "memory_rate", "cpu_rate", "request_count", "threads_rate",
        "mem_mean", "mem_max", "mem_min",
        "cpu_mean", "req_mean", "thr_mean",
        "mem_per_req", "cpu_per_req"
    ] + [
        f"{col}_lag_{i}" for col in FEATURE_PREFIXES for i in range(1, LAGS + 1)
    ] + [
        f"{col}_delta_{i}" for col in FEATURE_PREFIXES for i in range(1, LAGS + 1)
    ]

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    return assembler.transform(df)


def shorten_column_names(df):
    return (
        df.withColumnRenamed("catalina_threadpool_currentthreadsbusy", "threads_busy")
        .withColumnRenamed("jvm_memory_used_bytes", "mem_bytes")
        .withColumnRenamed("process_cpu_seconds_total", "cpu_seconds")
        .withColumnRenamed("catalina_globalrequestprocessor_requestcount", "request_count")
    )

def add_lags(df, w_order, LAGS):
    for i in range(1, LAGS + 1):
        df = (
            df
            .withColumn(f"mem_lag_{i}", F.lag("memory_rate", i).over(w_order))
            .withColumn(f"cpu_lag_{i}", F.lag("cpu_rate", i).over(w_order))
            .withColumn(f"req_lag_{i}", F.lag("request_count", i).over(w_order))
            .withColumn(f"thr_lag_{i}", F.lag("threads_rate", i).over(w_order))
        )
    return df

def add_stats_features(df, w_rolling):
    return (
        df
        # Memory stats
        .withColumn("mem_mean", F.avg("mem_bytes").over(w_rolling))
        .withColumn("mem_max", F.max("mem_bytes").over(w_rolling))
        .withColumn("mem_min", F.min("mem_bytes").over(w_rolling))

        # CPU
        .withColumn("cpu_mean", F.avg("cpu_seconds").over(w_rolling))
        .withColumn("cpu_max", F.max("cpu_seconds").over(w_rolling))
        .withColumn("cpu_min", F.min("cpu_seconds").over(w_rolling))  

        # Requests
        .withColumn("req_mean", F.avg("request_count").over(w_rolling))
        .withColumn("req_max", F.max("request_count").over(w_rolling))
        .withColumn("req_min", F.min("request_count").over(w_rolling))

        # Threads
        .withColumn("thr_mean", F.avg("threads_rate").over(w_rolling))
        .withColumn("thr_max", F.max("threads_rate").over(w_rolling))
        .withColumn("thr_min", F.min("threads_rate").over(w_rolling))
    )

def add_deltas(df, w_order):
    return (
        df
        .withColumn("mem_delta_1", F.col("memory_rate") - F.lag("memory_rate", 1).over(w_order))
        .withColumn("mem_delta_2", F.col("memory_rate") - F.lag("memory_rate", 2).over(w_order))
        .withColumn("mem_delta_3", F.col("memory_rate") - F.lag("memory_rate", 3).over(w_order))
        .withColumn("mem_delta_4", F.col("memory_rate") - F.lag("memory_rate", 4).over(w_order))
        .withColumn("mem_delta_5", F.col("memory_rate") - F.lag("memory_rate", 5).over(w_order))

        .withColumn("cpu_delta_1", F.col("cpu_rate") - F.lag("cpu_rate", 1).over(w_order))
        .withColumn("cpu_delta_2", F.col("cpu_rate") - F.lag("cpu_rate", 2).over(w_order))
        .withColumn("cpu_delta_3", F.col("cpu_rate") - F.lag("cpu_rate", 3).over(w_order))
        .withColumn("cpu_delta_4", F.col("cpu_rate") - F.lag("cpu_rate", 4).over(w_order))
        .withColumn("cpu_delta_5", F.col("cpu_rate") - F.lag("cpu_rate", 5).over(w_order))

        .withColumn("req_delta_1", F.col("request_count") - F.lag("request_count", 1).over(w_order))
        .withColumn("req_delta_2", F.col("request_count") - F.lag("request_count", 2).over(w_order))
        .withColumn("req_delta_3", F.col("request_count") - F.lag("request_count", 3).over(w_order))
        .withColumn("req_delta_4", F.col("request_count") - F.lag("request_count", 4).over(w_order))
        .withColumn("req_delta_5", F.col("request_count") - F.lag("request_count", 5).over(w_order))

        .withColumn("thr_delta_1", F.col("threads_rate") - F.lag("threads_rate", 1).over(w_order))
        .withColumn("thr_delta_2", F.col("threads_rate") - F.lag("threads_rate", 2).over(w_order))
        .withColumn("thr_delta_3", F.col("threads_rate") - F.lag("threads_rate", 3).over(w_order))
        .withColumn("thr_delta_4", F.col("threads_rate") - F.lag("threads_rate", 4).over(w_order))
        .withColumn("thr_delta_5", F.col("threads_rate") - F.lag("threads_rate", 5).over(w_order))
    )
if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])