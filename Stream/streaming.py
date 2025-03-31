from pinecone import Pinecone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

from config import MONGO_URI, MONGO_AGG_DATA_DB, PINECONE_API_KEY
from utilities.spark_utility import read_from_mongodb


def consume_interaction_data(spark: SparkSession):
    df = spark\
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "StatsTest") \
            .option("startingOffsets", "earliest") \
            .load() \

    messages = df.selectExpr("CAST(value AS STRING)")

    query = df \
        .selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('topic', 'StatsTestRes') \
        .option('checkpointLocation', '/usr/local/spark/chkpoint/') \
        .start()

    query.awaitTermination()



spark = SparkSession.builder \
    .appName("KafkaStreamProcessing") \
    .getOrCreate()

# Define Kafka source
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "interactionTopic") \
    .option("startingOffsets", "latest") \
    .load()

# Define schema for incoming JSON data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("interaction_type", StringType(), True),
    StructField("details", StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True)
    ]))
])

# Parse Kafka JSON messages
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


def fetch_last_24_hour_agg_data(id, coll_name) -> DataFrame:
    filter = '{"id": {0}}'.format(id)

    agg_df = read_from_mongodb(spark=spark, db_name=MONGO_AGG_DATA_DB, coll_name=coll_name, filter=filter)

    return agg_df


def process_interaction_event(df, epoch_id):
    """
    Processes each micro-batch of interactions efficiently.
    Updates user/item feature tables based on interaction type.
    """
    if df.isEmpty():
        return

    # Apply 3-minute tumbling window before processing
    df = df.withWatermark("timestamp", "3 minutes") \
        .groupBy(window(col("timestamp"), "3 minutes"), col("interaction_type")) \
        .count()

    # Filter interaction types only if they exist in the batch
    if df.filter(col("interaction_type") == "click").count() > 0:
        df_click = df.filter(col("interaction_type") == "click")

        user_agg_df = fetch_last_24_hour_agg_data(df_click.select("user_id"), "user_features")
        item_agg_df = fetch_last_24_hour_agg_data(df_click.select("details.product_id"), "item_features")

        if user_agg_df is not None:
            user_agg_df = user_agg_df.withColumn("browsing_behavior.total_clicks", col("browsing_behavior.total_clicks") + 1)

        if item_agg_df is not None:
            item_agg_df = item_agg_df.withColumn("view_count", col("view_count") + 1)

        if user_agg_df is not None:
            user_agg_df.write.mode("overwrite").parquet("/path/to/user_features.parquet")

        if item_agg_df is not None:
            item_agg_df.write.mode("overwrite").parquet("/path/to/item_features.parquet")

    if df.filter(col("interaction_type") == "checkout").count() > 0:
        df_checkout = df.filter(col("interaction_type") == "checkout")
        user_agg_df = fetch_last_24_hour_agg_data(df_checkout.select("user_id"), "user_features")
        if user_agg_df is not None:
            user_agg_df = user_agg_df.withColumn("purchase_count", col("purchase_count") + 1)
            user_agg_df.write.mode("overwrite").parquet("/path/to/user_features.parquet")

    if df.filter(col("interaction_type") == "add_to_cart").count() > 0:
        df_cart = df.filter(col("interaction_type") == "add_to_cart")
        user_agg_df = fetch_last_24_hour_agg_data(df_cart.select("user_id"), "user_features")
        if user_agg_df is not None:
            user_agg_df = user_agg_df.withColumn("cart_additions", col("cart_additions") + 1)
            user_agg_df.write.mode("overwrite").parquet("/path/to/user_features.parquet")

    if df.filter(col("interaction_type") == "view").count() > 0:
        df_view = df.filter(col("interaction_type") == "view")
        item_agg_df = fetch_last_24_hour_agg_data(df_view.select("details.product_id"), "item_features")
        if item_agg_df is not None:
            item_agg_df = item_agg_df.withColumn("view_count", col("view_count") + 1)
            item_agg_df.write.mode("overwrite").parquet("/path/to/item_features.parquet")

    if df.filter(col("interaction_type") == "search").count() > 0:
        df_search = df.filter(col("interaction_type") == "search")
        user_agg_df = fetch_last_24_hour_agg_data(df_search.select("user_id"), "user_features")
        if user_agg_df is not None:
            user_agg_df = user_agg_df.withColumn("search_queries", col("search_queries") + 1)
            user_agg_df.write.mode("overwrite").parquet("/path/to/user_features.parquet")

    if df.filter(col("interaction_type") == "review").count() > 0:
        df_review = df.filter(col("interaction_type") == "review")
        item_agg_df = fetch_last_24_hour_agg_data(df_review.select("details.product_id"), "item_features")
        if item_agg_df is not None:
            item_agg_df = item_agg_df.withColumn("total_reviews", col("total_reviews") + 1)
            item_agg_df.write.mode("overwrite").parquet("/path/to/item_features.parquet")


query = parsed_stream.writeStream \
    .foreachBatch(process_interaction_event) \
    .start()

query.awaitTermination()




