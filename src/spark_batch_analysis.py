import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True, help="Path to alerts JSON files")
    parser.add_argument("--output_path", required=True, help="Path to write aggregated stats")
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("HeartRateAlertBatchAnalysis")
        .getOrCreate()
    )

    alerts = spark.read.json(args.input_path)

    alert_stats = (
        alerts
        .groupBy("patient_id", "alert_type")
        .agg(
            F.count("*").alias("alert_count"),
            F.round(F.avg("avg_hr"), 2).alias("mean_window_avg_hr"),
            F.min("min_hr").alias("lowest_observed_hr"),
            F.max("max_hr").alias("highest_observed_hr"),
            F.min("window_start").alias("first_window_start"),
            F.max("window_end").alias("last_window_end"),
        )
        .orderBy("patient_id", "alert_type")
    )

    alert_stats.show(truncate=False)

    alert_stats.write.mode("overwrite").parquet(args.output_path)

    spark.stop()


if __name__ == "__main__":
    main()