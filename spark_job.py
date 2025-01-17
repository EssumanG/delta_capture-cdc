from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType


schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("amount", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("merchant_name", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("voucher_code", StringType(), True),
            StructField("affiliated_id", StringType(), True),
            StructField("modified_by", StringType(), True),
            StructField("modified_at", LongType(), True),
            StructField("change_info", StringType(), True)
        ]), True),
        StructField("after", StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("timestamp", LongType(), True),
            StructField("amount", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("merchant_name", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("voucher_code", StringType(), True),
            StructField("affiliated_id", StringType(), True),
            StructField("modified_by", StringType(), True),
            StructField("modified_at", LongType(), True),
            StructField("change_info", StringType(), True)
        ]), True)
    ]), True)
])


spark = SparkSession.builder.appName("Analyse_transaction").getOrCreate()

df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:29092")\
        .option("subscribe", "cdc.public.transactions")\
            .option("startingOffsets", "earliest").load()


df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df_parsed = df.withColumn("json_value", F.from_json(F.col("value"), schema))

df_extracted_diff = df_parsed.select(
    F.col("json_value.payload.before.transaction_id").alias("before_transaction_id"),
    F.col("json_value.payload.after.transaction_id").alias("after_transaction_id"),

    F.col("json_value.payload.before.user_id").alias("before_user_id"),
    F.col("json_value.payload.after.user_id").alias("after_user_id"),
    
    F.col("json_value.payload.before.timestamp").alias("before_timestamp"),
    F.col("json_value.payload.after.timestamp").alias("after_timestamp"),
    
    F.col("json_value.payload.before.amount").alias("before_amount"),
    F.col("json_value.payload.after.amount").alias("after_amount"),
    
    F.col("json_value.payload.before.currency").alias("before_currency"),
    F.col("json_value.payload.after.currency").alias("after_currency"),
    
    F.col("json_value.payload.before.city").alias("before_city"),
    F.col("json_value.payload.after.city").alias("after_city"),
    
    F.col("json_value.payload.before.merchant_name").alias("before_merchant_name"),
    F.col("json_value.payload.after.merchant_name").alias("after_merchant_name"),
    
    F.col("json_value.payload.before.payment_method").alias("before_payment_method"),
    F.col("json_value.payload.after.payment_method").alias("after_payment_method"),
    
    F.col("json_value.payload.before.ip_address").alias("before_ip_address"),
    F.col("json_value.payload.after.ip_address").alias("after_ip_address"),
    
    F.col("json_value.payload.before.voucher_code").alias("before_voucher_code"),
    F.col("json_value.payload.after.voucher_code").alias("after_voucher_code"),
    
    F.col("json_value.payload.before.affiliated_id").alias("before_affiliated_id"),
    F.col("json_value.payload.after.affiliated_id").alias("after_affiliated_id"),
    
    F.col("json_value.payload.before.modified_by").alias("before_modified_by"),
    F.col("json_value.payload.after.modified_by").alias("after_modified_by"),
    
    F.col("json_value.payload.before.modified_at").alias("before_modified_at"),
    F.col("json_value.payload.after.modified_at").alias("after_modified_at"),
    
    F.col("json_value.payload.before.change_info").alias("before_change_info"),
    F.col("json_value.payload.after.change_info").alias("after_change_info"),
)


df_extracted = df_extracted_diff.select(
    F.col("after_transaction_id").alias("transaction_id"),
    F.col("after_user_id").alias("user_id"),
    F.col("after_timestamp").alias("timestamp"),
    F.col("after_amount").alias("amount"),
    F.col("after_currency").alias("currency"),
    F.col("after_city").alias("city"),
    F.col("after_merchant_name").alias("merchant_name"),
    F.col("after_payment_method").alias("payment_method"),
    F.col("after_ip_address").alias("ip_address"),
    F.col("after_voucher_code").alias("voucher_code"),
    F.col("after_affiliated_id").alias("affiliated_id"),
    F.col("after_modified_by").alias("modified_by"),
    F.col("after_modified_at").alias("modified_at"),
    F.col("after_change_info").alias("change_info"),
)

# total number of transactions per user
df_total_number_user_transactions = df_extracted.groupBy(
    F.col("user_id"),
    F.col("currency"),
).agg(
    F.count(F.col("*")).alias("transaction_count"),
    F.sum(F.col("amount")).alias("total_amount"),
    F.avg(F.col("amount")).alias("avg_amount"),
    F.count(F.when(F.col("voucher_code").isNotNull(),1).alias("voucher_count"))
).orderBy(F.col("transaction_count"), F.col("total_amount"))

# merchant with highest transaction per hour
df_trasc_with_hour = df_extracted.withColumn("hour", F.hour((F.col("timestamp") / 1000).cast("timestamp")))

df_other = df_trasc_with_hour.groupBy(
    F.col("merchant_name"),
    ).agg(
        F.count("amount").alias("total_amount"),
        F.count("user_id").alias("no_users"),
        F.count("*").alias("no_transactions"),
        F.first(
        F.col("currency"),
        ignorenulls=True
    ).alias("most_common_currency")
    ).ordeBy(
        F.col("total_amount")
    )


# query = df_total_number_user_transactions.writeStream.outputMode("complete").format("console").start()
query = df_other.writeStream.outputMode("complete").format("console").start(Truncate=True)

query.awaitTermination()