import json
import os
from _decimal import Decimal
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from config import MONGO_URI, MONGO_AGG_DATA_DB, PINECONE_API_KEY, USER_FEATURES_HOST, ITEM_FEATURES_HOST, client, \
    MONGO_PRODUCTS_DB
from utilities.spark_utility import read_from_mongodb, store_df_to_mongodb, read_postgres_table, update_doc_in_mongodb
from utilities.pinecone_utility import store_to_pinecone, find_similar_objects
from Batch.UserBatchProcess.utility import vectorize
from Batch.ItemBatchProcess.utility import vectorize_item_features, vectorize_item_contents

# Create Kafka topic using the Kafka AdminClient
from kafka.admin import KafkaAdminClient, NewTopic

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.postgresql:postgresql:42.6.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 pyspark-shell --driver-memory 4g pyspark-shell,"
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:2.8.0,"
    "org.apache.kafka:kafka-clients:2.8.0 pyspark-shell"
)


# Kafka configuration
bootstrap_servers = "localhost:9092"
topic_name = "user_interactions"

""""# Create Kafka AdminClient
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Define the topic
topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)

# Create the topic
try:
    admin_client.create_topics([topic])
    print(f"Kafka topic '{topic_name}' created successfully.")
except Exception as e:
    print(f"Error creating Kafka topic: {e}")
finally:
    admin_client.close()"""


# Create Spark session
spark = SparkSession.builder \
    .appName("RealTimeRecommendationEngine") \
    .config("spark.mongodb.input.uri", MONGO_URI) \
    .config("spark.mongodb.output.uri", MONGO_URI) \
    .getOrCreate()

# Define schema for incoming events
event_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("details", MapType(StringType(), StringType()), True)
])

session_df = read_postgres_table(spark, "sessions")
orders_df = read_postgres_table(spark, "orders")
products_df = read_from_mongodb(spark, MONGO_PRODUCTS_DB, "products")

def fetch_agg_data(id, coll_name, id_type):
    """Fetch aggregated data from MongoDB"""
    filter = {f"{id_type}": id} if coll_name == "user_features" else {"product_id": id}
    return read_from_mongodb(spark, MONGO_AGG_DATA_DB, coll_name, filter)


def update_user_features(user_id, event_type, details):
    """Update user features based on event type"""
    user_agg = read_from_mongodb(spark, MONGO_AGG_DATA_DB, "user_features").filter(col("user_id") == user_id)
    #user_agg = fetch_agg_data(user_id, "user_features", "user_id")
    
    if user_agg.isEmpty():
        pass
        # Initialize new user features
        # user_agg = spark.createDataFrame([{
        #             "user_id": user_id,
        #             "browsing_behavior": {
        #                 "total_clicks": 0,
        #                 "total_add_to_cart": 0,
        #                 "total_add_to_wishlist": 0,
        #                 "freq_views": {
        #                     "products": [],
        #                     "avg_product_view_duration": [],
        #                     "overall_avg_product_view_duration": 0,
        #                     "categories": [],
        #                     "avg_category_view_duration": [],
        #                     "overall_avg_category_view_duration": 0,
        #                     "recently_viewed_products": [],
        #                     "recent_view_durations": []
        #                 }
        #             },
        #             "purchase_behavior": {
        #                 "total_purchase": 0,
        #                 "total_spent": 0,
        #                 "avg_order_value": 0,
        #                 "avg_orders_per_month": 0,
        #                 "seasonal_data": {
        #                     "winter": 0,
        #                     "spring": 0,
        #                     "summer": 0,
        #                     "fall": 0
        #                 }
        #             },
        #             "product_preferences": {
        #                 "most_purchased_brands": [],
        #                 "avg_review_rating": 0,
        #                 "review_count": 0
        #             },
        #             "user_profile": {
        #                 "account_age_days": 0,
        #                 "age": 0,
        #                 "avg_session_duration_sec": 0,
        #                 "session_counts_last_30_days": 0,
        #                 "gender": "",
        #                 "device_type": "",
        #                 "payment_method": "",
        #             }
        #         }])

    if event_type == "login":
        user_agg = user_agg.withColumn(
            "user_profile.session_counts_last_30_days",
            col("user_profile.session_counts_last_30_days") + 1
        )
    
    # Update based on event type
    elif event_type == "view":
        #user_agg = user_agg.withColumn(
        #    "browsing_behavior.freq_views.recently_viewed_products",
        #    array_append(col("browsing_behavior.freq_views.recently_viewed_products"), details["product_id"])
        #).withColumn(
        #    "browsing_behavior.freq_views.recent_view_durations",
        #    array_append(col("browsing_behavior.freq_views.recent_view_durations"), details["view_duration"])
        #).withColumn(
        #    "browsing_behavior.total_clicks",
        #    col("browsing_behavior.total_clicks") + 1
        #).withColumn(
        #    "browsing_behavior.total_views",
        #    col("browsing_behavior.total_views") + 1
        #)
        user_agg = user_agg.withColumn(
            "browsing_behavior",
            struct(
                struct(
                    col("browsing_behavior.freq_views.recently_viewed_products"),
                    array_append(col("browsing_behavior.freq_views.recently_viewed_products"), details["product_id"]).alias("recently_viewed_products"),
                    array_append(col("browsing_behavior.freq_views.recent_view_durations"), details["view_duration"]).alias("recent_view_durations"),
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
    
    elif event_type == "search":
        pass
        #    user_agg = user_agg.withColumn(
        #        "browsing_behavior.total_searches",
        #        col("browsing_behavior.total_searches") + 1
        #    ).withColumn(
        #        "browsing_behavior.search_behavior.search_terms",
        #        array_append(col("browsing_behavior.search_behavior.search_terms"), details["search_term"])
        #    ).withColumn(
        #        "browsing_behavior.search_behavior.search_categories",
        #        array_append(col("browsing_behavior.search_behavior.search_categories"), details["category"])
        #    )
    
    elif event_type == "filter":
        pass
        #user_agg = user_agg.withColumn(
        #    "browsing_behavior.total_filters",
        #    col("browsing_behavior.total_filters") + 1
        #).withColumn(
        #    "browsing_behavior.search_behavior.search_filters",
        #    array_append(col("browsing_behavior.search_behavior.search_filters"), details["filter_type"])
        #)
    
    elif event_type == "sort":
        pass
        # user_agg = user_agg.withColumn(
        #     "browsing_behavior.total_sorts",
        #     col("browsing_behavior.total_sorts") + 1
        # ).withColumn(
        #     "browsing_behavior.search_behavior.search_sorts",
        #     array_append(col("browsing_behavior.search_behavior.search_sorts"), details["sort_type"])
        # )
    
    elif event_type == "compare":
        pass
        # user_agg = user_agg.withColumn(
        #     "browsing_behavior.total_comparisons",
        #     col("browsing_behavior.total_comparisons") + 1
        # )
    
    elif event_type == "share":
        pass
        # user_agg = user_agg.withColumn(
        #     "browsing_behavior.total_shares",
        #     col("browsing_behavior.total_shares") + 1
        # )
    
    elif event_type == "review":
        #user_agg = user_agg.withColumn(
        #    "product_preferences.review_count",
        #    col("product_preferences.review_count") + 1
        #).withColumn(
        #    "product_preferences.avg_review_rating",
        #    (col("product_preferences.avg_review_rating") * col("product_preferences.review_count") +
        #     float(details["rating"])) / (col("product_preferences.review_count") + 1)
        #)

        user_agg = user_agg.withColumn(
            "product_preferences",
            struct(
                (col("product_preferences.review_count") + 1).alias("review_count"),
                (
                        (col("product_preferences.avg_review_rating") * col("product_preferences.review_count") +
                         float(details["rating"])) /
                        (col("product_preferences.review_count") + 1)
                ).alias("avg_review_rating"),
                col("product_preferences.avg_rating_per_category"),
                col("product_preferences.most_purchased_brands"),
                col("product_preferences.review_count")
            ).alias("product_preferences")
        )

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
    
    elif event_type == "purchase":
        user_agg = user_agg.withColumn(
            "purchase_behavior",
            struct(
                # Güncellenmiş total_purchase ve total_spent
                (col("purchase_behavior.total_purchase") + 1).alias("total_purchase"),
                (col("purchase_behavior.total_spent") + float(details["amount"])).alias("total_spent"),
                # Güncellenmiş avg_order_value
                (col("purchase_behavior.total_spent") + float(details["amount"])) /
                (col("purchase_behavior.total_purchase") + 1).alias("avg_order_value"),
                # Güncellenmiş seasonal_data alanı
                struct(
                    col("purchase_behavior.seasonal_data.winter"),
                    col("purchase_behavior.seasonal_data.spring"),
                    col("purchase_behavior.seasonal_data.summer"),
                    col("purchase_behavior.seasonal_data.fall"),
                    (col("purchase_behavior.seasonal_data." + get_current_season()) + 1).alias(get_current_season())
                ).alias("seasonal_data"),
                # Güncellenmiş payment_methods alanı
                struct(
                    col("purchase_behavior.payment_methods.Unknown"),
                    col("purchase_behavior.payment_methods.Credit Card"),
                    col("purchase_behavior.payment_methods.Paypal"),
                    col("purchase_behavior.payment_methods." + details["payment_method"]).alias(details["payment_method"])
                ).alias("payment_methods"),
                # Güncellenmiş recent_purchase_counts ve recent_purchase_amounts
                array_append(col("purchase_behavior.recent_purchase_counts"), details["product_count"]).alias("recent_purchase_counts"),
                array_append(col("purchase_behavior.recent_purchase_amounts"), details["amount"]).alias("recent_purchase_amounts"),
                # Güncellenmiş avg_orders_per_month
                round(col("purchase_behavior.total_purchase") / 12).alias("avg_orders_per_month")
            ).alias("purchase_behavior")
        )

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
    # user_vector = user_agg.withColumn(
    #     "values",
    #     vectorize(
    #         col("ctr"),
    #         col("user_profile"),
    #         col("browsing_behavior"),
    #         col("purchase_behavior"),
    #         col("product_preferences"),
    #         col("loyalty_score"),
    #         col("engagement_score"),
    #         col("preference_stability"),
    #         col("price_sensitivity"),
    #         col("category_exploration"),
    #         col("brand_loyalty"),
    #         col("total_impressions"),
    #         col("avg_view_duration")
    #     )
    # )

    vectorize_udf = udf(vectorize, ArrayType(FloatType()))

    vectorized_df = user_agg.withColumn(
        "values",
        vectorize_udf(
            col("ctr").cast("float"), col("user_profile"), col("browsing_behavior"), col("purchase_behavior"), col("product_preferences"),
            col("loyalty_score"), col("engagement_score"), col("preference_stability"), col("price_sensitivity"),
            col("category_exploration"), col("brand_loyalty"), col("total_impressions"), col("avg_view_duration")
        )
    )
    
    store_to_pinecone(vectorized_df.withColumnRenamed("user_id", "id").select("id", "values"), USER_FEATURES_HOST)
    update_doc_in_mongodb(MONGO_AGG_DATA_DB, "user_features", user_agg, id_col="user_id")


def update_item_features(product_id, event_type, details):
    """Update item features based on event type"""
    item_agg = read_from_mongodb(spark, MONGO_AGG_DATA_DB, "item_features").filter(col("product_id") == product_id)
    
    if item_agg.isEmpty():
        pass
        # Initialize new item features
        #item_agg = spark.createDataFrame([{
        #    "product_id": product_id,
        #    "purchase_count": 0,
        #    "view_count": 0,
        #    "wishlist_count": 0,
        #    "cart_addition_count": 0,
        #    "conversion_rate": 0,
        #    "abandonment_rate": 0,
        #    "avg_rating": 0,
        #    "trending_score": 0,
        #    "seasonal_sales": {
        #        "winter": {"total_sold_unit": 0},
        #        "spring": {"total_sold_unit": 0},
        #        "summer": {"total_sold_unit": 0},
        #        "fall": {"total_sold_unit": 0}
        #    },
        #    "historical_sales_summary": {
        #        "daily_units_sold": 0,
        #        "7_day_trend": 0,
        #        "30_day_trend": 0
        #    },
        #    "latest_sales_summary": {
        #        "last_1_day_sales": 0,
        #        "last_7_days_sales": 0,
        #        "last_30_days_sales": 0
        #    },
        #    "content": "",
        #    "price_elasticity": 0,
        #    "competitor_analysis": 0,
        #    "content_quality_score": 0,
        #    "inventory_turnover": 0,
        #    "return_rate": 0,
        #    "customer_satisfaction_score": 0
        #}])
    
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
        item_agg = item_agg.withColumn(
            "seasonal_sales",
            struct(
                struct(
                    col("seasonal_sales.Winter.total_sold_unit") + (lit(1) if current_season == "Winter" else lit(0)),
                    col("seasonal_sales.Winter.percentage_of_total")
                ).alias("Winter"),
                struct(
                    col("seasonal_sales.Spring.total_sold_unit") + (lit(1) if current_season == "Spring" else lit(0)),
                    col("seasonal_sales.Spring.percentage_of_total")
                ).alias("Spring"),
                struct(
                    col("seasonal_sales.Summer.total_sold_unit") + (lit(1) if current_season == "Summer" else lit(0)),
                    col("seasonal_sales.Summer.percentage_of_total")
                ).alias("Summer"),
                struct(
                    col("seasonal_sales.Fall.total_sold_unit") + (lit(1) if current_season == "Fall" else lit(0)),
                    col("seasonal_sales.Fall.percentage_of_total")
                ).alias("Fall")
            )
        )

        item_agg = item_agg.withColumn(
            "latest_sales_summary",
            struct(
                col("latest_sales_summary.last_1_day_sales") + lit(1),
                col("latest_sales_summary.last_7_days_sales") + lit(1),
                col("latest_sales_summary.last_30_days_sales") + lit(1)
            ).withColumn("historical_sales_summary",
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
        )

        item_agg = item_agg.withColumn("purchase_count", col("purchase_count") + lit(1))

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
        "price_elasticity",
        col("price_elasticity")
    ).withColumn(
        "competitor_analysis",
        col("competitor_analysis")
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
    ).withColumn(
        "return_rate",
        col("return_rate")

    ).withColumn(
        "customer_satisfaction_score",
        col("customer_satisfaction_score")
    )
    
    # Vectorize and update Pinecone
    """item_vector = item_agg.withColumn(
        "values",
        vectorize_item_features(
            col("purchase_count"),
            col("view_count"),
            col("wishlist_count"),
            col("cart_addition_count"),
            col("conversion_rate"),
            col("abandonment_rate"),
            col("avg_rating"),
            col("trending_score"),
            col("seasonal_sales.winter.total_sold_unit"),
            col("seasonal_sales.spring.total_sold_unit"),
            col("seasonal_sales.summer.total_sold_unit"),
            col("seasonal_sales.fall.total_sold_unit"),
            col("historical_sales_summary.daily_units_sold"),
            col("historical_sales_summary.7_day_trend"),
            col("historical_sales_summary.30_day_trend"),
            col("latest_sales_summary.last_1_day_sales"),
            col("latest_sales_summary.last_7_days_sales"),
            col("latest_sales_summary.last_30_days_sales"),
            col("content"),
            col("price_elasticity"),
            col("competitor_analysis"),
            col("content_quality_score"),
            col("inventory_turnover"),
            col("return_rate"),
            col("customer_satisfaction_score")
        )
    )"""
    vectorize_udf = udf(vectorize_item_features, ArrayType(FloatType()))

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

def process_windowed_events(df, epoch_id):
    """Process events in 3-minute windows"""
    if df.isEmpty():
        return
    
    # Group events by user_id and product_id within the window
    windowed_events = df.groupBy(
        "user_id",
        "product_id",
        "event_type"
    ).agg(
        count("*").alias("event_count"),
        collect_list("details").alias("details_list")
    )
    
    # Process each group of events
    for row in windowed_events.collect():
        user_id = row["user_id"]
        product_id = row["product_id"]
        event_type = row["event_type"]
        details = row["details_list"][0]  # Take the first details as they should be the same for the same event type

        print(f"Processing event for user: {user_id}, product: {product_id}, event_type: {event_type}, details: {details}")
        
        # Update user features
        #update_user_features(user_id, event_type, details)
        
        # Update item features
        update_item_features(product_id, event_type, details)

def get_current_season():
    """Get current season based on date"""
    month = datetime.now().month
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    else:
        return "fall"

# Read from Kafka
"""df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_interactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON events
parsed_df = df.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select("data.*")

# Apply 3-minute tumbling window
windowed_df = parsed_df \
    .withWatermark("timestamp", "3 minutes") \
    .groupBy(
        window(col("timestamp"), "3 minutes"),
        col("user_id"),
        col("product_id"),
        col("event_type")
    )

# Process windowed events
query = windowed_df.writeStream \
    .foreachBatch(process_windowed_events) \
    .outputMode("update") \
    .start()

query.awaitTermination()"""

test_data = [
    {
        "user_id": "U10",
        "product_id": "P1068738173",
        "event_type": "view",
        "details": {
            "session_id": "S27740",
            "user_id": "U10",
            "product_id": "P1068738173",
            "page_url": "/products/details/P1068738173",
            "view_date": "2025-05-01",
            "view_duration": 204,
            "referrer_url": "/search"
        },
        "timestamp": "2023-10-01T12:34:56Z"
    }
]

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("details", MapType(StringType(), StringType() if isinstance("value", str) else IntegerType()), True),
    StructField("timestamp", StringType(), True)
])

test_df = spark.createDataFrame(test_data, schema=schema)

process_windowed_events(test_df, None)

# Data format which will come
# user_id: "user_123",
# product_id: "product_456",
# event_type: "view",
# timestamp: "2023-10-01T12:34:56Z",
# details: { event data }