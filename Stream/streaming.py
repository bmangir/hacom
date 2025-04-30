from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from config import MONGO_URI, MONGO_AGG_DATA_DB, PINECONE_API_KEY, USER_FEATURES_HOST, ITEM_FEATURES_HOST, client, \
    MONGO_PRODUCTS_DB
from utilities.spark_utility import read_from_mongodb, store_df_to_mongodb, read_postgres_table
from utilities.pinecone_utility import store_to_pinecone, find_similar_objects
from Batch.UserBatchProcess.utility import vectorize
from Batch.ItemBatchProcess.utility import vectorize_item_features, vectorize_item_contents

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
    user_agg = fetch_agg_data(user_id, "user_features", "user_id")
    
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
        user_agg = user_agg.withColumn(
            "browsing_behavior.freq_views.recently_viewed_products",
            array_append(col("browsing_behavior.freq_views.recently_viewed_products"), details["product_id"])
        ).withColumn(
            "browsing_behavior.freq_views.recent_view_durations",
            array_append(col("browsing_behavior.freq_views.recent_view_durations"), details["view_duration"])
        ).withColumn(
            "browsing_behavior.total_clicks",
            col("browsing_behavior.total_clicks") + 1
        ).withColumn(
            "browsing_behavior.total_views",
            col("browsing_behavior.total_views") + 1
        ).withColumn(
            "engagement_score",
            (coalesce(col("user_profile.avg_session_duration"), lit(0)) / 3600) * 0.3 +
            (coalesce(col("user_profile.total_sessions"), lit(0)) / 100) * 0.2 +
            (coalesce(col("browsing_behavior.total_clicks"), lit(0)) / col("total_impressions")) * 0.2 +
            (coalesce(col("browsing_behavior.total_views"), lit(0)) / 100) * 0.2 +
            (coalesce(col("browsing_behavior.overall_avg_product_view_duration"), lit(0)) / 60) * 0.1
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
        user_agg = user_agg.withColumn(
            "product_preferences.review_count",
            col("product_preferences.review_count") + 1
        ).withColumn(
            "product_preferences.avg_review_rating",
            (col("product_preferences.avg_review_rating") * col("product_preferences.review_count") + 
             float(details["rating"])) / (col("product_preferences.review_count") + 1)
        )
    
    elif event_type == "add_to_cart":
        user_agg = user_agg.withColumn(
            "browsing_behavior.total_add_to_cart",
            col("browsing_behavior.total_add_to_cart") + 1
        )
    
    elif event_type == "add_to_wishlist":
        user_agg = user_agg.withColumn(
            "browsing_behavior.total_add_to_wishlist",
            col("browsing_behavior.total_add_to_wishlist") + 1
        )
    
    elif event_type == "purchase":
        user_agg = user_agg.withColumn(
            "purchase_behavior.total_purchase",
            col("purchase_behavior.total_purchase") + 1
        ).withColumn(
            "purchase_behavior.total_spent",
            col("purchase_behavior.total_spent") + float(details["amount"])
        ).withColumn(
            "purchase_behavior.avg_order_value",
            col("purchase_behavior.total_spent") / col("purchase_behavior.total_purchase")
        ).withColumn(
            "purchase_behavior.seasonal_data." + get_current_season(),
            col("purchase_behavior.seasonal_data." + get_current_season()) + 1
        ).withColumn(
            "purchase_behavior.payment_methods." + details["payment_method"],
            col("purchase_behavior.payment_methods." + details["payment_method"]) + 1
        ).withColumn(
            "purchase_behavior.recent_purchase_counts",
            array_append(col("purchase_behavior.recent_purchase_counts"), details["product_count"])
        ).withColumn(
            "purchase_behavior.recent_purchase_amounts",
            array_append(col("purchase_behavior.recent_purchase_amounts"), details["amount"])
        ).withColumn(
            "purchase_behavior.avg_orders_per_month",
            round(col("purchase_behavior.total_purchase") / 12)
        )

    unique_categories_df = orders_df.filter(col("user_id") == user_id) \
        .join(products_df.select("product_id", "category"), "product_id") \
        .groupBy("user_id") \
        .agg(count_distinct(products_df.category).alias("unique_categories_purchased"))

    # Get the number of unique categories purchased for the specific user
    unique_categories_purchased = unique_categories_df.select("unique_categories_purchased").first()["unique_categories_purchased"]

    # Calculate dependent features
    user_agg = user_agg.withColumn(
        "loyalty_score",
        (coalesce(col("purchase_behavior.total_purchase"), lit(0)) / 100) * 0.3 +
        (coalesce(col("purchase_behavior.avg_order_value"), lit(0)) / 1000) * 0.3 +
        (coalesce(col("user_profile.account_age_days"), lit(0)) / 365) * 0.2 +
        (coalesce(col("product_preferences.review_count"), lit(0)) / 10) * 0.2
    ).withColumn("engagement_score",
        (coalesce(col("user_profile.avg_session_duration"), lit(0)) / 3600) * 0.3 +
        (coalesce(col("user_profile.total_sessions"), lit(0)) / 100) * 0.2 +
        (coalesce(col("browsing_behavior.total_clicks"), lit(0)) / col("total_impressions")) * 0.2 +
        (coalesce(col("browsing_behavior.total_views"), lit(0)) / 100) * 0.2 +
        (coalesce(col("avg_view_duration"), lit(0)) / 60) * 0.1
    ).withColumn(
        "preference_stability",
        # Measure of how consistent user preferences are
    ).withColumn(
        "price_sensitivity",
        # Measure of user's sensitivity to price changes
    ).withColumn(
        "category_exploration",
        # Measure of how much user explores different categories
        (coalesce(size(array_distinct(col("browsing_behavior.freq_views.categories"))), lit(0)) / 10) * 0.6 +
        (coalesce(lit(unique_categories_purchased), lit(0)) / 5) * 0.4
    ).withColumn(
        "brand_loyalty",
        # Measure of brand loyalty
        coalesce(size(array_distinct(col("product_preferences.most_purchased_brands"))) / col("purchase_behavior.total_purchase"), lit(0))
    ).withColumn(
        "ctr",
        # Click-through rate
        coalesce(col("browsing_behavior.total_clicks") / col("total_impressions") * 100, lit(0.0))
    )
    
    # Vectorize and update Pinecone
    user_vector = user_agg.withColumn(
        "values",
        vectorize(
            col("ctr"),
            col("user_profile"),
            col("browsing_behavior"),
            col("purchase_behavior"),
            col("product_preferences"),
            col("loyalty_score"),
            col("engagement_score"),
            col("preference_stability"),
            col("price_sensitivity"),
            col("category_exploration"),
            col("brand_loyalty"),
            col("total_impressions"),
            col("avg_view_duration")
        )
    )
    
    store_to_pinecone(user_vector.select("user_id", "values"), USER_FEATURES_HOST)
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "user_features", user_agg)


def update_item_features(product_id, event_type, details):
    """Update item features based on event type"""
    item_agg = fetch_agg_data(product_id, "item_features")
    
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
        item_agg = item_agg.withColumn(
            "purchase_count",
            col("purchase_count") + 1
        ).withColumn(
            "latest_sales_summary.last_1_day_sales",
            col("latest_sales_summary.last_1_day_sales") + 1
        ).withColumn(
            "latest_sales_summary.last_7_days_sales",
            col("latest_sales_summary.last_7_days_sales") + 1
        ).withColumn(
            "latest_sales_summary.last_30_days_sales",
            col("latest_sales_summary.last_30_days_sales") + 1
        ).withColumn(
            "seasonal_sales." + get_current_season() + ".total_sold_unit",
            col("seasonal_sales." + get_current_season() + ".total_sold_unit") + 1
        )
    
    # Calculate dependent features
    item_agg = item_agg.withColumn(
        "conversion_rate",
        # Conversion rate based on purchases vs views
        col("purchase_count") / greatest(col("view_count") * 100, lit(1))
    ).withColumn(
        "abandonment_rate",
        # Abandonment rate based on cart additions vs purchases
        (col("cart_addition_count") - col("purchase_count")) / greatest(col("cart_addition_count"), lit(1))
    ).withColumn(
        "trending_score",
        # Trending score based on multiple factors
        (col("view_count") * 0.2) +  # View popularity
        (col("purchase_count") * 0.3) +  # Purchase popularity
        (col("wishlist_count") * 0.2) +  # Wishlist popularity
        (col("latest_sales_summary.last_7_days_sales") * 0.3)  # Recent sales momentum
    ).withColumn(
        "price_elasticity",
        # Price elasticity based on conversion rate and sales volume
        when(col("conversion_rate") > 0.1, 0.2)  # High conversion = low elasticity
        .when(col("conversion_rate") > 0.05, 0.5)  # Medium conversion
        .otherwise(0.8)  # Low conversion = high elasticity
    ).withColumn(
        "competitor_analysis",
        # Competitor analysis score based on market position
        (col("trending_score") * 0.4) +  # Market position
        (col("conversion_rate") * 0.3) +  # Conversion effectiveness
        (1 - col("abandonment_rate") * 0.3)  # Customer retention
    ).withColumn(
        "content_quality_score",
        # Content quality based on user engagement
        (col("view_count") * 0.3) +  # View engagement
        (col("wishlist_count") * 0.2) +  # Wishlist engagement
        (col("avg_rating") * 0.5)  # Rating quality
    ).withColumn(
        "inventory_turnover",
        # Inventory turnover based on sales velocity
        col("latest_sales_summary.last_30_days_sales") / 
        greatest(col("latest_sales_summary.last_1_day_sales"), lit(1))
    ).withColumn(
        "return_rate",
        # Return rate based on customer satisfaction
        when(col("customer_satisfaction_score") > 0.8, 0.1)  # High satisfaction
        .when(col("customer_satisfaction_score") > 0.5, 0.3)  # Medium satisfaction
        .otherwise(0.5)  # Low satisfaction
    ).withColumn(
        "customer_satisfaction_score",
        # Customer satisfaction based on multiple factors
        (col("avg_rating") * 0.4) +  # Product rating
        (1 - col("abandonment_rate") * 0.3) +  # Cart completion
        (col("conversion_rate") * 0.3)  # Purchase conversion
    ).withColumn(
        "historical_sales_summary.7_day_trend",
        # 7-day trend calculation
        (col("latest_sales_summary.last_7_days_sales") - 
         col("latest_sales_summary.last_30_days_sales") / 4) / 
        greatest(col("latest_sales_summary.last_30_days_sales") / 4, lit(1))
    ).withColumn(
        "historical_sales_summary.30_day_trend",
        # 30-day trend calculation
        col("latest_sales_summary.last_30_days_sales") / 
        greatest(col("historical_sales_summary.daily_units_sold") * 30, lit(1))
    )
    
    # Vectorize and update Pinecone
    item_vector = item_agg.withColumn(
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
    )
    
    store_to_pinecone(item_vector.select("product_id", "values"), ITEM_FEATURES_HOST)
    store_df_to_mongodb(MONGO_AGG_DATA_DB, "item_features", item_agg)

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
        
        # Update user features
        update_user_features(user_id, event_type, details)
        
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
df = spark \
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

query.awaitTermination()


# Data format which will come
# user_id: "user_123",
# product_id: "product_456",
# event_type: "view",
# timestamp: "2023-10-01T12:34:56Z",
# details: { event data }