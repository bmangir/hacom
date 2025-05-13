import os
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

from Stream.recommendation_founder import run_all_recommendations
from config import MONGO_URI, MONGO_AGG_DATA_DB, NEW_USER_FEATURES_HOST as USER_FEATURES_HOST, NEW_ITEM_FEATURES_HOST as ITEM_FEATURES_HOST, \
    MONGO_PRODUCTS_DB, CONFLUENT_BOOTSTRAP_SERVERS, CONFLUENT_INTERACTION_TOPIC, CONFLUENT_API_KEY, CONFLUENT_API_SECRET
from utilities.spark_utility import read_from_mongodb, read_postgres_table, update_doc_in_mongodb
from utilities.pinecone_utility import store_to_pinecone
from Batch.UserBatchProcess.utility import vectorize
from Batch.ItemBatchProcess.utility import vectorize_item_features


import logging
logging.getLogger("org.apache.spark").setLevel(logging.DEBUG)

packages = [
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.postgresql:postgresql:42.6.0",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
]


# Set Spark submit arguments
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {",".join(packages)} pyspark-shell'

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Confluent Kafka Connection") \
    .config("spark.mongodb.input.uri", MONGO_URI) \
    .config("spark.mongodb.output.uri", MONGO_URI) \
    .getOrCreate()

# Create checkpoint directory
checkpoint_dir = "/tmp/spark_streaming_checkpoint"
if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)

# Define schema for incoming events
event_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("details", MapType(StringType(), StringType()), True),
    StructField("timestamp", TimestampType(), True)
])

session_df = read_postgres_table(spark, "sessions")
orders_df = read_postgres_table(spark, "orders")
products_df = read_from_mongodb(spark, MONGO_PRODUCTS_DB, "products")


def update_user_features(user_id, event_type, details):
    """Update user features based on event type"""
    user_agg = read_from_mongodb(spark, MONGO_AGG_DATA_DB, "user_features").filter(col("user_id") == user_id)

    if user_agg.isEmpty():
        print(f"No user features record found for user ID {user_id}, skipping update")
        return

    # Different event types
    if event_type == "login":
        user_agg = user_agg.withColumn(
            "user_profile.session_counts_last_30_days",
            col("user_profile.session_counts_last_30_days") + 1
        )

    # Page view events
    elif event_type == "view":
        duration = details.get("view_duration", "0")
        user_agg = user_agg.withColumn(
            "browsing_behavior",
            struct(
                struct(
                    col("browsing_behavior.freq_views.recently_viewed_products"),
                    array_append(col("browsing_behavior.freq_views.recently_viewed_products"), details.get("product_id")).alias("recently_viewed_products"),
                    array_append(col("browsing_behavior.freq_views.recent_view_durations"), duration).alias("recent_view_durations"),
                    col("browsing_behavior.freq_views.avg_category_view_duration"),
                    col("browsing_behavior.freq_views.avg_product_view_duration"),
                    col("browsing_behavior.freq_views.categories"),
                    col("browsing_behavior.freq_views.overall_avg_category_view_duration"),
                    col("browsing_behavior.freq_views.overall_avg_product_view_duration"),
                    col("browsing_behavior.freq_views.products")
                ).alias("freq_views"),
                (col("browsing_behavior.total_clicks") + 1).alias("total_clicks"),
                (col("browsing_behavior.total_views") + 1).alias("total_views"),
                col("browsing_behavior.total_add_to_cart"),
                col("browsing_behavior.total_add_to_wishlist")
            ).alias("browsing_behavior")
        )

    # Cart events
    elif event_type == "add_to_cart":
        user_agg = user_agg.withColumn(
            "browsing_behavior",
            struct(
                col("browsing_behavior.freq_views"),
                col("browsing_behavior.total_clicks"),
                col("browsing_behavior.total_views"),
                (col("browsing_behavior.total_add_to_cart") + 1).alias("total_add_to_cart"),
                col("browsing_behavior.total_add_to_wishlist")
            ).alias("browsing_behavior")
        )
    elif event_type == "update_cart":
        # No specific update needed for this event type
        pass
    elif event_type == "remove_from_cart":
        # No specific update needed since we don't track cart removal count
        pass
    elif event_type == "cart_cleared":
        # No specific update needed for this event type
        pass

    # Wishlist events
    elif event_type == "add_to_wishlist":
        user_agg = user_agg.withColumn(
            "browsing_behavior",
            struct(
                col("browsing_behavior.freq_views"),
                col("browsing_behavior.total_clicks"),
                col("browsing_behavior.total_views"),
                col("browsing_behavior.total_add_to_cart"),
                (col("browsing_behavior.total_add_to_wishlist") + 1).alias("total_add_to_wishlist")
            ).alias("browsing_behavior")
        )
    elif event_type == "remove_from_wishlist":
        # No specific update needed since we don't track wishlist removal count
        pass
    elif event_type == "wishlist_cleared":
        # No specific update needed for this event type
        pass

    # Order events
    elif event_type == "purchase":
        try:
            amount = details.get("total_amount", "0")
            payment_method = details.get("payment_method", "Unknown")

            user_agg = user_agg.withColumn(
                "purchase_behavior",
                struct(
                    # Updated total_purchase and total_spent
                    (col("purchase_behavior.total_purchase") + 1).alias("total_purchase"),
                    (col("purchase_behavior.total_spent") + float(amount)).alias("total_spent"),
                    # Updated avg_order_value
                    (col("purchase_behavior.total_spent") + float(amount)) /
                    (col("purchase_behavior.total_purchase") + 1).alias("avg_order_value"),
                    # Updated seasonal_data alanı
                    struct(
                        col("purchase_behavior.seasonal_data.winter"),
                        col("purchase_behavior.seasonal_data.spring"),
                        col("purchase_behavior.seasonal_data.summer"),
                        col("purchase_behavior.seasonal_data.fall"),
                        (col("purchase_behavior.seasonal_data." + get_current_season()) + 1).alias(get_current_season())
                    ).alias("seasonal_data"),
                    # Updated payment_methods
                    struct(
                        col("purchase_behavior.payment_methods.Unknown"),
                        col("purchase_behavior.payment_methods.Credit Card"),
                        col("purchase_behavior.payment_methods.Paypal"),
                        (col("purchase_behavior.payment_methods." + payment_method) + 1).alias(payment_method)
                    ).alias("payment_methods"),
                    # Updated recent_purchase_counts and recent_purchase_amounts
                    array_append(col("purchase_behavior.recent_purchase_counts"), 1).alias("recent_purchase_counts"),
                    array_append(col("purchase_behavior.recent_purchase_amounts"), amount).alias("recent_purchase_amounts"),
                    # Updated avg_orders_per_month
                    round(col("purchase_behavior.total_purchase") / 12).alias("avg_orders_per_month")
                ).alias("purchase_behavior")
            )
        except Exception as e:
            print(f"Error updating purchase behavior: {str(e)}")

    elif event_type == "order_completed":
        # This is a summary event after all product purchases, no specific update needed
        pass

    elif event_type == "order_status_updated":
        # Order status updates don't directly affect user features
        pass

    elif event_type == "view_order_confirmation":
        # Order confirmation views don't directly affect user features
        pass

    # Search and other events
    elif event_type == "search":
        pass
        # TODO: Implement search event tracking

    elif event_type == "review":
        try:
            rating = details.get("rating", "0")
            user_agg = user_agg.withColumn(
                "product_preferences",
                struct(
                    (col("product_preferences.review_count") + 1).alias("review_count"),
                    (
                            (col("product_preferences.avg_review_rating") * col("product_preferences.review_count") +
                             float(rating)) /
                            (col("product_preferences.review_count") + 1)
                    ).alias("avg_review_rating"),
                    col("product_preferences.avg_rating_per_category"),
                    col("product_preferences.most_purchased_brands"),
                    col("product_preferences.review_count")
                ).alias("product_preferences")
            )
        except Exception as e:
            print(f"Error updating review data: {str(e)}")

    # Calculate dependent metrics regardless of event type
    unique_categories_df = orders_df.filter(col("user_id") == user_id) \
        .join(products_df.select("product_id", "category"), "product_id") \
        .groupBy("user_id") \
        .agg(coalesce(count_distinct(products_df.category), lit(0)).alias("unique_categories_purchased"))

    # Get the number of unique categories purchased for the specific user
    unique_categories_purchased = unique_categories_df.select("unique_categories_purchased").first()
    unique_categories_purchased = unique_categories_purchased["unique_categories_purchased"] if unique_categories_purchased else 0

    # Calculate dependent features
    user_agg = user_agg.withColumn(
        "loyalty_score",
        (coalesce(col("purchase_behavior.total_purchase"), lit(0)) / 100) * 0.3 +
        (coalesce(col("purchase_behavior.avg_order_value"), lit(0)) / 1000) * 0.3 +
        (coalesce(col("user_profile.account_age_days"), lit(0)) / 365) * 0.2 +
        (coalesce(col("product_preferences.review_count"), lit(0)) / 10) * 0.2
    ).withColumn("engagement_score",
                 (coalesce(col("user_profile.avg_session_duration_sec"), lit(0)) / 3600) * 0.3 +
                 (coalesce(col("user_profile.total_sessions"), lit(0)) / 100) * 0.2 +
                 (coalesce(col("browsing_behavior.total_clicks"), lit(0)) / col("total_impressions")) * 0.2 +
                 (coalesce(col("browsing_behavior.total_views"), lit(0)) / 100) * 0.2 +
                 (coalesce(col("avg_view_duration"), lit(0)) / 60) * 0.1
                 ).withColumn(
        "preference_stability",
        col("preference_stability")
    ).withColumn(
        "price_sensitivity",
        col("price_sensitivity")
    ).withColumn(
        "category_exploration",
        (coalesce(size(array_distinct(col("browsing_behavior.freq_views.categories"))), lit(0)) / 10) * 0.6 +
        (coalesce(lit(unique_categories_purchased), lit(0)) / 5) * 0.4
    ).withColumn(
        "brand_loyalty",
        coalesce(size(array_distinct(col("product_preferences.most_purchased_brands"))) / greatest(col("purchase_behavior.total_purchase"), lit(1)), lit(0))
    ).withColumn(
        "ctr",
        coalesce(col("browsing_behavior.total_clicks") / col("total_impressions") * 100, lit(0.0))
    )

    # Vectorize and update Pinecone
    vectorize_udf = udf(vectorize, ArrayType(FloatType()))

    vectorized_df = user_agg.withColumn(
        "values",
        vectorize_udf(
            col("ctr").cast("float"), col("user_profile"), col("browsing_behavior"), col("purchase_behavior"), col("product_preferences"),
            col("loyalty_score"), col("engagement_score"), col("preference_stability"), col("price_sensitivity"),
            col("category_exploration"), col("brand_loyalty"), col("total_impressions"), col("avg_view_duration")
        )
    )

    try:
        store_to_pinecone(vectorized_df.withColumnRenamed("user_id", "id").select("id", "values"), USER_FEATURES_HOST)
        update_doc_in_mongodb(MONGO_AGG_DATA_DB, "user_features", user_agg, id_col="user_id")
        print(f"Successfully updated user features for {user_id}")
    except Exception as e:
        print(f"Error updating user features in Pinecone/MongoDB: {str(e)}")


def update_item_features(product_id, event_type, details):
    """Update item features based on event type"""
    if not product_id:
        print("No product_id provided, skipping item feature update")
        return

    item_agg = read_from_mongodb(spark, MONGO_AGG_DATA_DB, "item_features").filter(col("product_id") == product_id)

    if item_agg.isEmpty():
        print(f"No item features record found for product ID {product_id}, skipping update")
        return

    # Update based on event type
    if event_type == "view":
        item_agg = item_agg.withColumn(
            "view_count",
            col("view_count") + 1
        ).withColumn(
            "total_views",
            col("total_views") + 1
        )

    elif event_type == "add_to_cart":
        item_agg = item_agg.withColumn(
            "cart_addition_count",
            col("cart_addition_count") + 1
        )

    elif event_type == "add_to_wishlist":
        item_agg = item_agg.withColumn(
            "wishlist_count",
            col("wishlist_count") + 1
        )

    elif event_type == "purchase":
        current_season = get_current_season()
        try:
            item_agg = item_agg.withColumn(
                "seasonal_sales",
                struct(
                    struct(
                        col(f"seasonal_sales.Winter.total_sold_unit") + (lit(1) if current_season == "Winter" else lit(0)),
                        col("seasonal_sales.Winter.percentage_of_total")
                    ).alias("Winter"),
                    struct(
                        col(f"seasonal_sales.Spring.total_sold_unit") + (lit(1) if current_season == "Spring" else lit(0)),
                        col("seasonal_sales.Spring.percentage_of_total")
                    ).alias("Spring"),
                    struct(
                        col(f"seasonal_sales.Summer.total_sold_unit") + (lit(1) if current_season == "Summer" else lit(0)),
                        col("seasonal_sales.Summer.percentage_of_total")
                    ).alias("Summer"),
                    struct(
                        col(f"seasonal_sales.Fall.total_sold_unit") + (lit(1) if current_season == "Fall" else lit(0)),
                        col("seasonal_sales.Fall.percentage_of_total")
                    ).alias("Fall")
                )
            )

            item_agg = item_agg.withColumn(
                "latest_sales_summary",
                struct(
                    (col("latest_sales_summary.last_1_day_sales") + lit(1)).alias("last_1_day_sales"),
                    (col("latest_sales_summary.last_7_days_sales") + lit(1)).alias("last_7_days_sales"),
                    (col("latest_sales_summary.last_30_days_sales") + lit(1)).alias("last_30_days_sales")
                )
            ).withColumn(
                "historical_sales_summary",
                struct(
                    col("historical_sales_summary.daily_units_sold").alias("daily_units_sold"),
                    (
                            (col("latest_sales_summary.last_7_days_sales") -
                             (col("latest_sales_summary.last_30_days_sales") / lit(4))) /
                            greatest(col("latest_sales_summary.last_30_days_sales") / lit(4), lit(1))
                    ).alias("7_day_trend"),
                    (
                            col("latest_sales_summary.last_30_days_sales") /
                            greatest(col("historical_sales_summary.daily_units_sold") * lit(30), lit(1))
                    ).alias("30_day_trend")
                )
            )

            item_agg = item_agg.withColumn("purchase_count", col("purchase_count") + lit(1))
        except Exception as e:
            print(f"Error updating purchase stats: {str(e)}")

    elif event_type == "remove_from_cart":
        # No specific update for removals as we're tracking net impact
        pass

    elif event_type == "remove_from_wishlist":
        # No specific update for removals as we're tracking net impact
        pass

    # Calculate dependent metrics regardless of event type
    weight_views = 1.0
    weight_purchases = 2.0
    weight_searches = 0.5

    # Calculate dependent features
    item_agg = item_agg.withColumn(
        "conversion_rate",
        col("purchase_count") / greatest(col("view_count") * 100, lit(1))
    ).withColumn(
        "abandonment_rate",
        # Abandonment rate based on cart additions vs purchases
        (col("cart_addition_count") - col("purchase_count")) / greatest(col("cart_addition_count"), lit(1))
    ).withColumn(
        "trending_score",
        # Trending score based on multiple factors
        (col("view_count") * weight_views) +
        (col("purchase_count") * weight_purchases) +
        (col("total_searches") * weight_searches)
    ).withColumn(
        "content_quality_score",
        # Content quality based on user engagement
        (coalesce(col("review_length_avg"), lit(0)) / 100) * 0.4 +
        (coalesce(col("review_count"), lit(0)) / 100) * 0.3
    ).withColumn(
        "inventory_turnover",
        # Inventory turnover based on sales velocity
        col("latest_sales_summary.last_30_days_sales") /
        greatest(col("latest_sales_summary.last_1_day_sales"), lit(1))
    )

    # Vectorize and update Pinecone
    vectorize_udf = udf(vectorize_item_features, ArrayType(FloatType()))

    try:
        item_vector = item_agg \
            .withColumn("values", vectorize_udf(
            col("purchase_count"), col("view_count"), col("wishlist_count"), col("cart_addition_count"),
            col("conversion_rate"), col("abandonment_rate"), col("avg_rating"), col("trending_score"),
            col("seasonal_sales.Winter.total_sold_unit"), col("seasonal_sales.Spring.total_sold_unit"),
            col("seasonal_sales.Summer.total_sold_unit"), col("seasonal_sales.Fall.total_sold_unit"),
            col("historical_sales_summary.daily_units_sold"), col("historical_sales_summary.7_day_trend"),
            col("historical_sales_summary.30_day_trend"),
            col("latest_sales_summary.last_1_day_sales"), col("latest_sales_summary.last_7_days_sales"),
            col("latest_sales_summary.last_30_days_sales"),
            col("content"), col("price_elasticity"), col("competitor_analysis"), col("content_quality_score"),
            col("inventory_turnover"), col("return_rate"), col("customer_satisfaction_score"))) \
            .withColumnRenamed("product_id", "id")

        store_to_pinecone(item_vector.select("id", "values"), ITEM_FEATURES_HOST)
        update_doc_in_mongodb(MONGO_AGG_DATA_DB, "item_features", item_agg, id_col="product_id")
        print(f"Successfully updated item features for {product_id}")
    except Exception as e:
        print(f"Error updating item features in Pinecone/MongoDB: {str(e)}")


def process_windowed_events(df, epoch_id):
    """Process events in 3-minute windows"""
    if df.isEmpty():
        print("No events in this window, skipping processing")
        return

    print(f"Processing batch for epoch {epoch_id} with {df.count()} events")

    # Group events by user_id and product_id within the window
    windowed_events = df \
        .withWatermark("timestamp", "3 minutes") \
        .groupBy(
        "user_id",
        "product_id",
        "event_type"
    ).agg(
        count("*").alias("event_count"),
        collect_list(col("details")).alias("details_list")
    )

    # Process each group of events
    for row in windowed_events.collect():
        user_id = row["user_id"]
        product_id = row["product_id"]
        event_type = row["event_type"]
        details = row["details_list"][0] if row["details_list"] and len(row["details_list"]) > 0 else {}

        print(f"Processing event for user: {user_id}, product: {product_id}, event_type: {event_type}, details: {details}")

        try:
            # Update user features first
            if user_id:
                update_user_features(user_id, event_type, details)

            # Update item features if there's a product_id
            if product_id:
                update_item_features(product_id, event_type, details)

        except Exception as e:
            print(f"Error processing event: {str(e)}")

    run_all_recommendations()


def get_current_season():
    """Get current season based on date"""
    month = datetime.now().month
    if month in [12, 1, 2]:
        return "Winter"
    elif month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    else:
        return "Fall"


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", CONFLUENT_BOOTSTRAP_SERVERS) \
    .option("subscribe", CONFLUENT_INTERACTION_TOPIC) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{CONFLUENT_API_KEY}" password="{CONFLUENT_API_SECRET}";') \
    .option("group.id", "python-group-1") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df_json = df.selectExpr("key", "CAST(value AS STRING) as json_value")

parsed_df = df.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select("data.*")
parsed_df.printSchema()

# Process the parsed DataFrame in 3-minute windows
parsed_df = parsed_df.withColumn("timestamp", current_timestamp())
#windowed_df = parsed_df \
#    .withWatermark("timestamp", "3 minutes") \
#    .groupBy(
#        window(col("timestamp"), "3 minutes"),
#        col("user_id"),
#        col("product_id"),
#        col("event_type")
#    ).agg(
#        count("*").alias("event_count"),
#        collect_list(col("details")).alias("details_list")
#    )

# Apply the data to the processing function
query = parsed_df.writeStream \
    .outputMode("update") \
    .foreachBatch(process_windowed_events) \
    .option("truncate", "false") \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime="30 seconds") \
    .start()

#query = parsed_df.writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .option("truncate", "false") \
#    .option("kafka.bootstrap.servers", CONFLUENT_BOOTSTRAP_SERVERS) \
#    .option("subscribe", CONFLUENT_INTERACTION_TOPIC) \
#    .start()

# Add error handling
try:
    query.awaitTermination()
except Exception as e:
    print(f"Streaming query failed: {str(e)}")
    # Log the error to MongoDB or another logging system
    import traceback
    error_details = {
        "error_type": type(e).__name__,
        "error_message": str(e),
        "stack_trace": traceback.format_exc(),
        "timestamp": datetime.now()
    }
    #client[MONGO_LOGS_DB]["streaming_errors"].insert_one(error_details)
    raise  # Re-raise the exception after logging