from pyspark.sql import SparkSession
import argparse
from pyspark.sql.function import *

def spark_transformation_dev(env, bq_project, bq_dataset, transformed_table, route_insights_table, origin_insights_table):
    
    spark = SparkSession.builder.appName("Spark Transformation CI CD")\
                    .config("spark.sql.catalogImplementation", "hive") \
                    .getOrCreate()
    bucket_name = "airflow_project_sankarsh"
    input_path = f"gs://{bucket_name}/airflow-ci-cd/data/flight_booking.csv"
    
    data = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(input_path)

    transformed_data = data.withColumn("is_weekend", when(col("flight_day").isin("Sat","Sun"), lit(1)).otherwise(lit(0)) )\
    .withColumn("lead_time_category", when(col("purchase_lead") > 7 , lit("Last-Minute"))\
                                      .when( (col("purchase_lead") >=7) & (col("purchase_lead") < 30) , lit("Short-Term"))\
                                    .otherwise( lit("Long-Term")))\
    .withColumn(
            "booking_success_rate", expr("booking_complete / num_passengers")
        )
    
            # Aggregations for insights
    route_insights = transformed_data.groupBy("route").agg(
    count("*").alias("total_bookings"),
    avg("flight_duration").alias("avg_flight_duration"),
    avg("length_of_stay").alias("avg_stay_length")
    )
    
    booking_origin_insights = transformed_data.groupBy("booking_origin").agg(
    count("*").alias("total_bookings"),
    avg("booking_success_rate").alias("success_rate"),
    avg("purchase_lead").alias("avg_purchase_lead")
    )
    
    transformed_data.write.format("bigquery").mode("overwrite").option("table", f"{bq_project}:{bq_dataset}.{transformed_table}")\
        .option("writeMethod", "direct").save()
    
    route_insights.write.format("bigquery").mode("overwrite").option("table", f"{bq_project}:{bq_dataset}.{route_insights_table}")\
        .option("writeMethod", "direct").save()
    
    booking_origin_insights.write.format("bigquery").mode("overwrite").option("table", f"{bq_project}:{bq_dataset}.{origin_insights_table}")\
        .option("writeMethod", "direct").save()
    
    print("Writing Done!")
        
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Argments for Spark Job.")
    parser.add_argument("--env", type=str, required=True, help="Environment Name")
    parser.add_argument("--bq_project", type=str, required=True, help="Big Query Project ID Name")    
    parser.add_argument("--bq_dataset", type=str, required=True, help="Big Query DataSet Name")
    parser.add_argument("--transformed_table", type=str, required=True, help="Table Name for transformation")    
    parser.add_argument("--route_insights_table", type=str, required=True, help="Table Name for route Insights")    
    parser.add_argument("--origin_insights_table", type=str, required=True, help="Table Name for origin Insights")
    
    arguments = parser.parse_args()
    
    spark_transformation_dev(
        arguments.env,
        arguments.bq_project,
        arguments.bq_dataset,
        arguments.transformed_table,
        arguments.route_insights_table,
        arguments.origin_insights_table
    )