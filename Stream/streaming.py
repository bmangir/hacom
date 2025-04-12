from datetime import datetime

from kafka import KafkaProducer
from pinecone import Pinecone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

from Batch.ItemBatchProcess.utility import vectorize_item_features
from Batch.UserBatchProcess.utility import vectorize
from config import MONGO_URI, MONGO_AGG_DATA_DB, PINECONE_API_KEY, USER_FEATURES_HOST, ITEM_FEATURES_HOST, client
from utilities.spark_utility import read_from_mongodb, store_df_to_mongodb, read_postgres_table
from utilities.pinecone_utility import store_to_pinecone, find_similar_objects


user_vectorize_udf = udf(vectorize, ArrayType(FloatType()))
item_vectorize_udf = udf(vectorize_item_features, ArrayType(FloatType()))

similar_objects_udf = udf(find_similar_objects, ArrayType(FloatType()))


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

# Read streaming data from Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "interactionTopic") \
    .option("startingOffsets", "latest") \
    .load()

schema = StructType() \
    .add("user_id", StringType()) \
    .add("interaction_type", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("details", StructType().add("product_id", StringType()))

df = df_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# **Apply 3-minute tumbling window**
df_windowed = df \
    .withWatermark("timestamp", "3 minutes") \
    .groupBy(window(col("timestamp"), "3 minutes"), col("interaction_type")) \
    .count()


def fetch_last_24_hour_agg_data(id, coll_name) -> DataFrame:
    filter = '{"id": {0}}'.format(id)

    agg_df = read_from_mongodb(spark=spark, db_name=MONGO_AGG_DATA_DB, coll_name=coll_name, filter=filter)

    return agg_df


def click_event(df_click):
    # Get product and user IDs
    product_id = df_click.select("details.product_id").first()["product_id"]
    user_id = df_click.select("user_id").first()["user_id"]
    
    # Fetch current aggregated data
    user_agg_df = fetch_last_24_hour_agg_data(user_id, "user_features")
    item_agg_df = fetch_last_24_hour_agg_data(product_id, "item_features")

    # Update user features
    user_agg_df = user_agg_df\
        .withColumn("browsing_behavior.total_clicks", col("browsing_behavior.total_clicks") + 1)\
        .withColumn("updated_at", (unix_timestamp(current_timestamp()) * 1000))

    # Update item features
    item_agg_df = item_agg_df\
        .withColumn("view_count", col("view_count") + 1)\
        .withColumn("trending_score", col("trending_score") + 1)\
        .withColumn("updated_at", (unix_timestamp(current_timestamp()) * 1000))

    # Store updated data to MongoDB
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "user_features", user_agg_df, mode="append")
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "item_features", item_agg_df, mode="append")

    # Vectorize and update Pinecone
    user_vectorized_df = user_agg_df.withColumn(
        "values",
        user_vectorize_udf(
            col("ctr").cast("float"), 
            col("user_profile"), 
            col("browsing_behavior"), 
            col("purchase_behavior"), 
            col("product_preferences")
        )
    )

    item_vectorized_df = item_agg_df\
        .withColumn("values", item_vectorize_udf(
            col("purchase_count"), 
            col("view_count"), 
            col("wishlist_count"), 
            col("cart_addition_count"),
            col("conversion_rate"), 
            col("abandonment_rate"), 
            col("avg_rating"), 
            col("trending_score"),
            col("seasonal_sales.Winter.total_sold_unit"), 
            col("seasonal_sales.Spring.total_sold_unit"),
            col("seasonal_sales.Summer.total_sold_unit"), 
            col("seasonal_sales.Fall.total_sold_unit"),
            col("historical_sales_summary.daily_units_sold"), 
            col("historical_sales_summary.7_day_trend"),
            col("historical_sales_summary.30_day_trend"),
            col("latest_sales_summary.last_1_day_sales"), 
            col("latest_sales_summary.last_7_days_sales"),
            col("latest_sales_summary.last_30_days_sales"),
            col("content")
        ))\
        .withColumnRenamed("product_id", "id")

    # Store vectors in Pinecone
    store_to_pinecone(user_vectorized_df.withColumnRenamed("user_id", "id").select("id", "values"), 
                     index_name="", dimension=1, host=USER_FEATURES_HOST)
    store_to_pinecone(item_vectorized_df.select("id", "values"), 
                     index_name="", dimension=1, host=ITEM_FEATURES_HOST)


def checkout_event(user_features: DataFrame, item_features: DataFrame, event):
    current_season = get_current_season()
    purchased_item_id = event["interaction_data"]["product_id"]
    spent_amount = event["interaction_data"]["spent"]

    # Update user features
    user_features = user_features\
        .withColumn("purchase_behavior.total_purchase", col("purchase_behavior.total_purchase") + 1)\
        .withColumn("purchase_behavior.total_spent", col("purchase_behavior.total_spent") + spent_amount)\
        .withColumn(f"purchase_behavior.seasonal_data.{current_season}", 
                   col(f"purchase_behavior.seasonal_data.{current_season}") + 1)\
        .withColumn("updated_at", (unix_timestamp(current_timestamp()) * 1000))

    # Update item features
    item_features = item_features\
        .withColumn("total_purchases", col("total_purchases") + 1)\
        .withColumn("purchase_count", col("purchase_count") + 1)\
        .withColumn("conversion_rate", col("purchase_count") / col("view_count") * 100)\
        .withColumn("abandonment_rate", 
                   (col("cart_addition_count") - col("purchase_count")) / col("cart_addition_count"))\
        .withColumn("trending_score", 
                   col("total_views") * 1 + col("total_purchases") * 2 + col("total_searches") * 0.5)\
        .withColumn("updated_at", (unix_timestamp(current_timestamp()) * 1000))

    # Store to MongoDB
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "user_features", user_features, mode="append")
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "item_features", item_features, mode="append")

    # Vectorize and update Pinecone
    user_vectorized_df = user_features.withColumn(
        "values",
        user_vectorize_udf(
            col("ctr").cast("float"), 
            col("user_profile"), 
            col("browsing_behavior"), 
            col("purchase_behavior"), 
            col("product_preferences")
        )
    )

    item_vectorized_df = item_features\
        .withColumn("values", item_vectorize_udf(
            col("purchase_count"), 
            col("view_count"), 
            col("wishlist_count"), 
            col("cart_addition_count"),
            col("conversion_rate"), 
            col("abandonment_rate"), 
            col("avg_rating"), 
            col("trending_score"),
            col("seasonal_sales.Winter.total_sold_unit"), 
            col("seasonal_sales.Spring.total_sold_unit"),
            col("seasonal_sales.Summer.total_sold_unit"), 
            col("seasonal_sales.Fall.total_sold_unit"),
            col("historical_sales_summary.daily_units_sold"), 
            col("historical_sales_summary.7_day_trend"),
            col("historical_sales_summary.30_day_trend"),
            col("latest_sales_summary.last_1_day_sales"), 
            col("latest_sales_summary.last_7_days_sales"),
            col("latest_sales_summary.last_30_days_sales"),
            col("content")
        ))\
        .withColumnRenamed("product_id", "id")

    # Store vectors in Pinecone
    store_to_pinecone(user_vectorized_df.withColumnRenamed("user_id", "id").select("id", "values"), 
                     index_name="", dimension=1, host=USER_FEATURES_HOST)
    store_to_pinecone(item_vectorized_df.select("id", "values"), 
                     index_name="", dimension=1, host=ITEM_FEATURES_HOST)


def add_to_cart_event(user_features: DataFrame, item_features: DataFrame, event):
    # Update user features
    user_features = user_features\
        .withColumn("browsing_behavior.total_add_to_cart", 
                   col("browsing_behavior.total_add_to_cart") + 1)\
        .withColumn("updated_at", (unix_timestamp(current_timestamp()) * 1000))

    # Update item features
    item_features = item_features\
        .withColumn("cart_addition_count", col("cart_addition_count") + 1)\
        .withColumn("abandonment_rate", 
                   (col("cart_addition_count") - col("purchase_count")) / col("cart_addition_count"))\
        .withColumn("trending_score", 
                   col("total_views") * 1 + col("total_purchases") * 2 + col("total_searches") * 0.5)\
        .withColumn("updated_at", (unix_timestamp(current_timestamp()) * 1000))

    # Store to MongoDB
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "user_features", user_features, mode="append")
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "item_features", item_features, mode="append")

    # Vectorize and update Pinecone
    user_vectorized_df = user_features.withColumn(
        "values",
        user_vectorize_udf(
            col("ctr").cast("float"), 
            col("user_profile"), 
            col("browsing_behavior"), 
            col("purchase_behavior"), 
            col("product_preferences")
        )
    )

    item_vectorized_df = item_features\
        .withColumn("values", item_vectorize_udf(
            col("purchase_count"), 
            col("view_count"), 
            col("wishlist_count"), 
            col("cart_addition_count"),
            col("conversion_rate"), 
            col("abandonment_rate"), 
            col("avg_rating"), 
            col("trending_score"),
            col("seasonal_sales.Winter.total_sold_unit"), 
            col("seasonal_sales.Spring.total_sold_unit"),
            col("seasonal_sales.Summer.total_sold_unit"), 
            col("seasonal_sales.Fall.total_sold_unit"),
            col("historical_sales_summary.daily_units_sold"), 
            col("historical_sales_summary.7_day_trend"),
            col("historical_sales_summary.30_day_trend"),
            col("latest_sales_summary.last_1_day_sales"), 
            col("latest_sales_summary.last_7_days_sales"),
            col("latest_sales_summary.last_30_days_sales"),
            col("content")
        ))\
        .withColumnRenamed("product_id", "id")

    # Store vectors in Pinecone
    store_to_pinecone(user_vectorized_df.withColumnRenamed("user_id", "id").select("id", "values"), 
                     index_name="", dimension=1, host=USER_FEATURES_HOST)
    store_to_pinecone(item_vectorized_df.select("id", "values"), 
                     index_name="", dimension=1, host=ITEM_FEATURES_HOST)


def add_to_wishlist_event(user_features: DataFrame, item_features: DataFrame, event):
    # Update user features
    user_features = user_features\
        .withColumn("browsing_behavior.total_add_to_wishlist", 
                   col("browsing_behavior.total_add_to_wishlist") + 1)\
        .withColumn("updated_at", (unix_timestamp(current_timestamp()) * 1000))

    # Update item features
    item_features = item_features\
        .withColumn("wishlist_count", col("wishlist_count") + 1)\
        .withColumn("trending_score", 
                   col("total_views") * 1 + col("total_purchases") * 2 + col("total_searches") * 0.5)\
        .withColumn("updated_at", (unix_timestamp(current_timestamp()) * 1000))

    # Store to MongoDB
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "user_features", user_features, mode="append")
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "item_features", item_features, mode="append")

    # Vectorize and update Pinecone
    user_vectorized_df = user_features.withColumn(
        "values",
        user_vectorize_udf(
            col("ctr").cast("float"), 
            col("user_profile"), 
            col("browsing_behavior"), 
            col("purchase_behavior"), 
            col("product_preferences")
        )
    )

    item_vectorized_df = item_features\
        .withColumn("values", item_vectorize_udf(
            col("purchase_count"), 
            col("view_count"), 
            col("wishlist_count"), 
            col("cart_addition_count"),
            col("conversion_rate"), 
            col("abandonment_rate"), 
            col("avg_rating"), 
            col("trending_score"),
            col("seasonal_sales.Winter.total_sold_unit"), 
            col("seasonal_sales.Spring.total_sold_unit"),
            col("seasonal_sales.Summer.total_sold_unit"), 
            col("seasonal_sales.Fall.total_sold_unit"),
            col("historical_sales_summary.daily_units_sold"), 
            col("historical_sales_summary.7_day_trend"),
            col("historical_sales_summary.30_day_trend"),
            col("latest_sales_summary.last_1_day_sales"), 
            col("latest_sales_summary.last_7_days_sales"),
            col("latest_sales_summary.last_30_days_sales"),
            col("content")
        ))\
        .withColumnRenamed("product_id", "id")

    # Store vectors in Pinecone
    store_to_pinecone(user_vectorized_df.withColumnRenamed("user_id", "id").select("id", "values"), 
                     index_name="", dimension=1, host=USER_FEATURES_HOST)
    store_to_pinecone(item_vectorized_df.select("id", "values"), 
                     index_name="", dimension=1, host=ITEM_FEATURES_HOST)


def remove_from_cart_event():
    pass


def remove_from_wishlist_event():
    pass


def view_event():
    pass


def search_event(user_features: DataFrame, item_features: DataFrame, event):
    # Update item features
    item_features = item_features\
        .withColumn("total_searches", col("total_searches") + 1)\
        .withColumn("trending_score", 
                   col("total_views") * 1 + col("total_purchases") * 2 + col("total_searches") * 0.5)\
        .withColumn("updated_at", (unix_timestamp(current_timestamp()) * 1000))

    # Store to MongoDB
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "item_features", item_features, mode="append")

    # Vectorize and update Pinecone
    item_vectorized_df = item_features\
        .withColumn("values", item_vectorize_udf(
            col("purchase_count"), 
            col("view_count"), 
            col("wishlist_count"), 
            col("cart_addition_count"),
            col("conversion_rate"), 
            col("abandonment_rate"), 
            col("avg_rating"), 
            col("trending_score"),
            col("seasonal_sales.Winter.total_sold_unit"), 
            col("seasonal_sales.Spring.total_sold_unit"),
            col("seasonal_sales.Summer.total_sold_unit"), 
            col("seasonal_sales.Fall.total_sold_unit"),
            col("historical_sales_summary.daily_units_sold"), 
            col("historical_sales_summary.7_day_trend"),
            col("historical_sales_summary.30_day_trend"),
            col("latest_sales_summary.last_1_day_sales"), 
            col("latest_sales_summary.last_7_days_sales"),
            col("latest_sales_summary.last_30_days_sales"),
            col("content")
        ))\
        .withColumnRenamed("product_id", "id")

    # Store vectors in Pinecone
    store_to_pinecone(item_vectorized_df.select("id", "values"), 
                     index_name="", dimension=1, host=ITEM_FEATURES_HOST)


def review_event(df_review):
    product_id = df_review.select("details.product_id").first()["product_id"]
    user_id = df_review.select("user_id").first()["user_id"]
    rating = df_review.select("details.rating").first()["rating"]
    review_text = df_review.select("details.review_text").first()["review_text"]

    # Fetch current aggregated data
    user_agg_df = fetch_last_24_hour_agg_data(user_id, "user_features")
    item_agg_df = fetch_last_24_hour_agg_data(product_id, "item_features")

    # Update user features
    user_agg_df = user_agg_df\
        .withColumn("product_preferences.review_count", 
                   col("product_preferences.review_count") + 1)\
        .withColumn("updated_at", (unix_timestamp(current_timestamp()) * 1000))

    # Update item features
    item_agg_df = item_agg_df\
        .withColumn("review_count", col("review_count") + 1)\
        .withColumn("avg_rating", 
                   (col("avg_rating") * col("review_count") + rating) / (col("review_count") + 1))\
        .withColumn("updated_at", (unix_timestamp(current_timestamp()) * 1000))

    # Store to MongoDB
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "user_features", user_agg_df, mode="append")
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "item_features", item_agg_df, mode="append")

    # Vectorize and update Pinecone
    user_vectorized_df = user_agg_df.withColumn(
        "values",
        user_vectorize_udf(
            col("ctr").cast("float"), 
            col("user_profile"), 
            col("browsing_behavior"), 
            col("purchase_behavior"), 
            col("product_preferences")
        )
    )

    item_vectorized_df = item_agg_df\
        .withColumn("values", item_vectorize_udf(
            col("purchase_count"), 
            col("view_count"), 
            col("wishlist_count"), 
            col("cart_addition_count"),
            col("conversion_rate"), 
            col("abandonment_rate"), 
            col("avg_rating"), 
            col("trending_score"),
            col("seasonal_sales.Winter.total_sold_unit"), 
            col("seasonal_sales.Spring.total_sold_unit"),
            col("seasonal_sales.Summer.total_sold_unit"), 
            col("seasonal_sales.Fall.total_sold_unit"),
            col("historical_sales_summary.daily_units_sold"), 
            col("historical_sales_summary.7_day_trend"),
            col("historical_sales_summary.30_day_trend"),
            col("latest_sales_summary.last_1_day_sales"), 
            col("latest_sales_summary.last_7_days_sales"),
            col("latest_sales_summary.last_30_days_sales"),
            col("content")
        ))\
        .withColumnRenamed("product_id", "id")

    # Store vectors in Pinecone
    store_to_pinecone(user_vectorized_df.withColumnRenamed("user_id", "id").select("id", "values"), 
                     index_name="", dimension=1, host=USER_FEATURES_HOST)
    store_to_pinecone(item_vectorized_df.select("id", "values"), 
                     index_name="", dimension=1, host=ITEM_FEATURES_HOST)


def process_interaction_event(df):
    """
    Processes each micro-batch of interactions efficiently.
    Updates user/item feature tables based on interaction type.
    """

    if df.isEmpty():
        return

    interaction_types = [row["interaction_type"] for row in df.select("interaction_type").distinct().collect()]

    if "click" in interaction_types:
        df_click = df.filter(col("interaction_type") == "click")
        click_event(df_click)

    if "checkout" in interaction_types:
        df_checkout = df.filter(col("interaction_type") == "checkout")
        user_agg_df = fetch_last_24_hour_agg_data(df_checkout.select("user_id"), "user_features")
        if user_agg_df:
            user_agg_df = user_agg_df.withColumn("purchase_count", col("purchase_count") + 1)

    if "add_to_cart" in interaction_types:
        df_cart = df.filter(col("interaction_type") == "add_to_cart")
        user_agg_df = fetch_last_24_hour_agg_data(df_cart.select("user_id"), "user_features")
        if user_agg_df:
            user_agg_df = user_agg_df.withColumn("cart_additions", col("cart_additions") + 1)

    if "view" in interaction_types:
        df_view = df.filter(col("interaction_type") == "view")
        item_agg_df = fetch_last_24_hour_agg_data(df_view.select("details.product_id"), "item_features")
        if item_agg_df:
            item_agg_df = item_agg_df.withColumn("view_count", col("view_count") + 1)

    if "search" in interaction_types:
        df_search = df.filter(col("interaction_type") == "search")
        user_agg_df = fetch_last_24_hour_agg_data(df_search.select("user_id"), "user_features")
        if user_agg_df:
            user_agg_df = user_agg_df.withColumn("search_queries", col("search_queries") + 1)

    if "review" in interaction_types:
        df_review = df.filter(col("interaction_type") == "review")
        item_agg_df = fetch_last_24_hour_agg_data(df_review.select("details.product_id"), "item_features")
        if item_agg_df:
            item_agg_df = item_agg_df.withColumn("total_reviews", col("total_reviews") + 1)


query = df_windowed.writeStream \
    .foreachBatch(process_interaction_event) \
    .start()

query.awaitTermination()


producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(10):
    producer.send('interactionTopic', value=f'Hello Kafka {i}'.encode('utf-8'))


def get_current_season():
    # get the current day of the year
    doy = datetime.today().timetuple().tm_yday

    # "day of year" ranges for the northern hemisphere
    spring = range(80, 172)
    summer = range(172, 264)
    fall = range(264, 355)

    if doy in spring:
        season = 'spring'
    elif doy in summer:
        season = 'summer'
    elif doy in fall:
        season = 'fall'
    else:
        season = 'winter'

    return season