from pyspark.sql import SparkSession, functions as F

def create_spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("retail-etl") \
        .getOrCreate()

def main():
    spark = create_spark()

    # load CSV - update path if needed
    csv_path = "data/sample_transactions.csv"
    df = (spark.read
          .option("header", True)
          .option("inferSchema", False)
          .csv(csv_path))

    # basic cleaning and derived cols
    df = (df.withColumn("quantity", F.col("quantity").cast("int"))
            .withColumn("unit_price", F.col("unit_price").cast("double"))
            .withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
            .withColumn("total", F.col("quantity") * F.col("unit_price"))
            .withColumn("date", F.to_date("transaction_ts"))
            .na.drop(subset=["invoice_id", "transaction_ts", "product_id"])
         )

    # write full processed table
    df.repartition(1).write.mode("overwrite").parquet("outputs/processed_transactions")

    # daily aggregates (date-level time series)
    daily = (df.groupBy("date")
               .agg(F.sum("total").alias("daily_revenue"),
                    F.sum("quantity").alias("daily_quantity"))
               .orderBy("date"))
    daily.repartition(1).write.mode("overwrite").parquet("outputs/daily_sales")

    # top products by revenue & quantity
    top = (df.groupBy("product_id", "product_name")
             .agg(F.sum("total").alias("revenue"),
                  F.sum("quantity").alias("quantity_sold"))
             .orderBy(F.desc("revenue")))
    top.repartition(1).write.mode("overwrite").parquet("outputs/top_products")

    spark.stop()

if __name__ == "__main__":
    main()
