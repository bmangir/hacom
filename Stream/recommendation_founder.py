import os
from datetime import datetime

from pyspark.sql import Window, SparkSession
from pyspark.sql.functions import *

from Stream.utility import find_similar_objects_udf
from config import MONGO_AGG_DATA_DB, NEW_USER_FEATURES_HOST as USER_FEATURES_HOST, \
    NEW_ITEM_FEATURES_HOST as ITEM_FEATURES_HOST, NEW_ITEM_CONTENTS_HOST as ITEM_CONTENTS_HOST, \
    MONGO_PRODUCTS_DB, client, MONGO_RECOMMENDATION_DB, MONGO_URI
from utilities.spark_utility import read_from_mongodb, store_df_to_mongodb

packages = [
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.postgresql:postgresql:42.6.0",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
]


# Set Spark submit arguments
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages {",".join(packages)} pyspark-shell'

spark = SparkSession.builder \
    .config("spark.mongodb.input.uri", MONGO_URI) \
    .config("spark.mongodb.output.uri", MONGO_URI) \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "200") \
    .getOrCreate()


products_df = read_from_mongodb(
    spark=spark, 
    db_name=MONGO_AGG_DATA_DB, 
    coll_name="item_features", 
    filter={}
).select(
    "product_id",
    "purchase_count",
    "view_count",
    "wishlist_count",
    "cart_addition_count",
    "conversion_rate",
    "abandonment_rate",
    "avg_rating",
    "trending_score",
    "seasonal_sales",
    "historical_sales_summary",
    "latest_sales_summary",
    "bought_together",
    "review_count"
)

raw_product_df = read_from_mongodb(
    spark=spark,
    db_name=MONGO_PRODUCTS_DB,
    coll_name="products",
    filter={}
).select("category", "brand", "date_added", "product_id")

products_df = products_df.join(raw_product_df, "product_id", "left")

users_df = read_from_mongodb(
    spark=spark,
    db_name=MONGO_AGG_DATA_DB,
    coll_name="user_features",
    filter={}
)

products_df.cache()
users_df.cache()
# Trigger caching by forcing an action
products_df.count()
users_df.count()

products_df = products_df.repartition(10)
users_df = users_df.repartition(10)

# Define UDFs
similarity_feature_udf = find_similar_objects_udf(idx_host=ITEM_FEATURES_HOST, top_k=2)
similarity_content_udf = find_similar_objects_udf(idx_host=ITEM_CONTENTS_HOST, top_k=2)
similarity_user_udf = find_similar_objects_udf(idx_host=USER_FEATURES_HOST, top_k=2)

# Filter products before finding similar items
filtered_products = products_df.filter(
    (col("trending_score") > 0.5) | 
    (col("avg_rating") > 3.5)
).select("product_id")

# Group products into batches for batch processing
product_batches = filtered_products.withColumn(
    "batch_id",
    pmod(monotonically_increasing_id(), lit(100))  # Create batches of 100
).groupBy("batch_id").agg(
    collect_list("product_id").alias("product_ids")
)

# Get similar items based on content vectors (metadata-content) using batch processing
content_similar_items = product_batches.withColumn(
    "similar_items_batch",
    similarity_content_udf(col("product_ids"))
).select(
    explode(arrays_zip("product_ids", "similar_items_batch")).alias("similarity_pair")
).select(
    col("similarity_pair.product_ids").alias("product_id"),
    col("similarity_pair.similar_items_batch").alias("content_similar_items")
).cache()

content_similar_items.count()
print()

# Get similar items based on feature vectors (behavioral)
feature_similar_items = products_df.withColumn(
    "feature_similar_items",
    similarity_feature_udf(col("product_id"))
).select("product_id", "feature_similar_items")
feature_similar_items.cache()
feature_similar_items.count()

# Only process users with activity
active_users = users_df.filter(
    (size(col("browsing_behavior.freq_views.products")) > 0) | (col("browsing_behavior.freq_views.products") != "[]")
    | (col("browsing_behavior.freq_views.products").isNotNull())
).select("user_id")

active_users = active_users.repartition(10)
similar_users = active_users.withColumn(
    "similar_users",
    similarity_user_udf(col("user_id"))
).cache()
similar_users.count()


def user_based_category_recommendations_all():
    """
    Optimized user-based collaborative filtering recommendations for categories based on top 3 most visited categories.
    """

    # Take browsing categories and count them for each user
    user_categories = users_df.select(
        "user_id",
        explode("browsing_behavior.freq_views.categories").alias("category")
    ).groupBy("user_id", "category").count().cache()

    # Process similar users' categories for recommendations
    similar_users_expanded = similar_users.select(
        col("user_id").alias("target_user_id"),
        explode(col("similar_users")).alias("similar_user_id")
    )

    # Join with user categories to find the categories of similar users
    similar_users_categories = similar_users_expanded.join(
        user_categories,
        similar_users_expanded.similar_user_id == user_categories.user_id,
        "left"
    ).select(
        "target_user_id",
        "category",
        "count"
    )

    category_counts = similar_users_categories.groupBy(
        "target_user_id", "category"
    ).agg(
        sum("count").alias("total_count")
    )

    category_window = Window.partitionBy("target_user_id").orderBy(desc("total_count"))

    ranked_categories = category_counts.withColumn(
        "rank", row_number().over(category_window)
    ).filter((col("rank") <= 3) & (col("category").isNotNull())).select(
        "target_user_id",
        "category",
        "total_count"
    ).withColumnRenamed("target_user_id", "user_id")

    final_recommendations = ranked_categories.withColumn(
        "source", lit("collaborative")
    ).withColumn(
        "recc_at", unix_timestamp(current_timestamp()) * 1000
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "category_recommendations", final_recommendations, mode="overwrite")

    products_df.unpersist()
    users_df.unpersist()
    similar_users.unpersist()
    user_categories.unpersist()
    category_counts.unpersist()
    ranked_categories.unpersist()


def user_based_recommendations_all():
    """
    Optimized user-based collaborative filtering recommendations
    People who behave like you also liked X
    """

    # Get user items more efficiently - combine viewed and purchased
    user_items = users_df.select(
        "user_id",
        array_union(
            coalesce(col("browsing_behavior.freq_views.products"), array()),
            coalesce(col("purchase_behavior.recently_purchased_products"), array())
        ).alias("user_items"),
        coalesce(col("browsing_behavior.freq_views.avg_product_view_duration"), array()).alias("view_durations"),
        col("ctr"),
        col("purchase_behavior.avg_orders_per_month").alias("purchase_frequency")
    ).cache()

    # Process cold-start cases separately for non-activated users yet
    cold_start_users = users_df.filter(
        size(col("browsing_behavior.freq_views.products")) == 0
    ).select("user_id")

    top_rated_products = products_df.filter(
        col("view_count") > 100
    ).orderBy(
        desc("avg_rating"),
        desc("trending_score")
    ).limit(10).select("product_id")

    # Create cold-start recommendations
    cold_start_recommendations = cold_start_users.crossJoin(
        top_rated_products
    ).select(
        col("user_id"),
        col("product_id").alias("recc_item"),
        lit(1.0).alias("engagement_score"),
        lit(0.5).alias("click_through_rate"),
        lit("cold_start").alias("source")
    )

    # Generate recommendations for active users more efficiently
    similar_users_expanded = similar_users.select(
        col("user_id").alias("target_user_id"),
        explode(col("similar_users")).alias("similar_user_id")
    )
    active_recommendations = similar_users_expanded.join(
        user_items,
        similar_users_expanded.similar_user_id == user_items.user_id,
        "left"
    ).select(
        col("target_user_id").alias("user_id"),
        explode(col("user_items")).alias("recc_item"),
        coalesce(col("purchase_frequency"), col("ctr"), lit(0.5)).alias("engagement_score"),
        coalesce(col("ctr"), lit(0.5)).alias("click_through_rate"),
        lit("collaborative").alias("source")
    )

    # Combine recommendations and add product context efficiently
    all_recommendations = active_recommendations.union(cold_start_recommendations)
    enriched_recommendations = all_recommendations.join(
        broadcast(products_df.select(
            col("product_id"),
            col("trending_score"),
            col("avg_rating"),
            col("conversion_rate")
        )),
        all_recommendations.recc_item == products_df.product_id,
        "left"
    )

    # Calculate final scores and rank efficiently
    final_recommendations = enriched_recommendations.withColumn(
        "recommendation_score",
        when(col("source") == "cold_start",
             (coalesce(col("avg_rating"), lit(0.0)) * 0.4) +
             (coalesce(col("trending_score"), lit(0.0)) * 0.3) +
             (coalesce(col("conversion_rate"), lit(0.0)) * 0.3)
             ).otherwise(
            (col("engagement_score") * 0.3) +
            (col("click_through_rate") * 0.2) +
            (coalesce(col("trending_score"), lit(0.0)) * 0.2) +
            (coalesce(col("avg_rating"), lit(0.0)) * 0.15) +
            (coalesce(col("conversion_rate"), lit(0.0)) * 0.15)
        )
    )

    window_spec = Window.partitionBy("user_id").orderBy(desc("recommendation_score"))

    final_recommendations = final_recommendations.withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(col("rank") <= 10
    ).withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    ).select(
        "user_id",
        "recc_item",
        "recommendation_score",
        "recc_at"
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "ubcf", final_recommendations, mode="overwrite")

    products_df.unpersist()
    users_df.unpersist()
    similar_users.unpersist()
    user_items.unpersist()


def item_based_recommendations_all():
    """
    Customers who bought this also bought Y
    :return:
    """

    frequently_bought = products_df.select(
        "product_id",
        col("bought_together")
    )

    # Process each similarity type separately and combine them
    feature_recommendations = feature_similar_items\
        .withColumn("recc_items", explode(col("feature_similar_items"))) \
        .select(
            col("product_id").alias("feature_product_id"),
            col("recc_items").alias("feature_recc_items"),
            lit("feature_similar").alias("feature_source")
        )

    content_recommendations = content_similar_items\
        .withColumn("recc_items", explode(col("content_similar_items"))) \
        .select(
            col("product_id").alias("content_product_id"),
            col("recc_items").alias("content_recc_items"),
            lit("content_similar").alias("content_source")
        )

    bought_together_recommendations = frequently_bought\
        .withColumn("recc_items", explode(col("bought_together"))) \
        .select(
            col("product_id").alias("bought_product_id"),
            col("recc_items").alias("bought_recc_items"),
            lit("bought_together").alias("bought_source")
        )

    all_recommendations = feature_recommendations\
        .join(content_recommendations,
              (feature_recommendations.feature_product_id == content_recommendations.content_product_id) &
              (feature_recommendations.feature_recc_items == content_recommendations.content_recc_items),
              "full")\
        .join(bought_together_recommendations,
              (feature_recommendations.feature_product_id == bought_together_recommendations.bought_product_id) &
              (feature_recommendations.feature_recc_items == bought_together_recommendations.bought_recc_items),
              "full")\
        .select(
            coalesce(
                feature_recommendations.feature_product_id,
                content_recommendations.content_product_id,
                bought_together_recommendations.bought_product_id
            ).alias("product_id"),
            coalesce(
                feature_recommendations.feature_recc_items,
                content_recommendations.content_recc_items,
                bought_together_recommendations.bought_recc_items
            ).alias("recc_items"),
            coalesce(
                feature_recommendations.feature_source,
                content_recommendations.content_source,
                bought_together_recommendations.bought_source
            ).alias("source")
        )

    # Add product metadata for recommendations
    recommendations = all_recommendations.join(
        products_df.select(
            col("product_id").alias("metadata_product_id"),
            col("category"),
            col("brand"),
            col("trending_score"),
            col("avg_rating")
        ),
        all_recommendations.recc_items == col("metadata_product_id"),
        "left"
    )

    # Weight and rank recommendations
    window_spec = Window.partitionBy("product_id").orderBy(
        desc("trending_score"),
        desc("avg_rating")
    )

    final_recommendations = recommendations.withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(col("rank") <= 10).withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "ibcf", final_recommendations, mode="overwrite")


def content_based_recommendations_all():
    """
    Content-based recommendation combining feature-matching and product similarity
    Optimized to reduce execution time and avoid task kills
    """

    # Select relevant user data and filter active users
    active_users = users_df.filter(
        size(col("browsing_behavior.freq_views.products")) > 0
    ).select("user_id").cache()

    user_preferences = users_df.join(
        broadcast(active_users),
        "user_id",
        "inner"
    ).select(
        "user_id",
        col("product_preferences.most_purchased_brands").alias("preferred_brands"),
        col("browsing_behavior.freq_views.categories").alias("browsed_categories"),
        col("price_sensitivity")
    ).persist()
    user_preferences.count()  # Trigger caching

    # Select product features and apply pre-filtering
    product_features = products_df.select(
        "product_id",
        "category",
        "brand",
        "trending_score",
        "avg_rating",
        "conversion_rate",
        col("latest_sales_summary.last_30_days_sales").alias("monthly_sales"),
        "seasonal_sales"
    ).filter(
        (col("trending_score") > 0.5) |
        (col("avg_rating") > 4.0)
    ).persist()
    product_features.count()  # Trigger caching

    # Calculate category affinity scores
    category_affinity = user_preferences.select(
        "user_id", "browsed_categories"
    ).withColumn(
        "category_scores", explode("browsed_categories")
    ).groupBy("user_id", "category_scores").count()

    # Feature-based recommendations: match on category or brand (no crossJoin)
    feature_based_recommendations = user_preferences.alias("u") \
        .join(
        product_features.alias("p"),
        (array_contains(col("u.preferred_brands"), col("p.brand"))) |
        (expr("array_contains(u.browsed_categories, p.category)"))
    ) \
        .join(broadcast(category_affinity), col("u.user_id") == category_affinity.user_id) \
        .select(
        col("u.user_id").alias("user_id"),
        col("p.product_id").alias("product_id"),
        col("p.trending_score").alias("trending_score"),
        col("p.avg_rating").alias("avg_rating"),
        col("p.conversion_rate").alias("conversion_rate"),
        col("p.brand").alias("brand"),
        col("p.category").alias("category"),
        lit("feature_based").alias("source")
    )

    # Content-based recommendations: join similar items with product metadata
    content_based_recommendations = content_similar_items \
        .select(
        col("product_id").alias("base_product"),
        explode("content_similar_items").alias("product_id")
    ) \
        .join(product_features, "product_id") \
        .select(
        "product_id",
        "trending_score",
        "avg_rating",
        "conversion_rate",
        "brand",
        "category",
        lit("content_similar").alias("source")
    ) \
        .join(broadcast(user_preferences.select("user_id")), "user_id")  # cross join yerine normal join

    # Union both sets of recommendations
    recommendations = feature_based_recommendations.unionByName(content_based_recommendations)

    # Join user preferences again for final scoring
    recommendations = recommendations.join(
        user_preferences.select("user_id", "preferred_brands", "browsed_categories"),
        "user_id",
        "left"
    )

    # Calculate relevance and final score
    recommendations = recommendations.withColumn(
        "relevance_score",
        (when(array_contains(col("preferred_brands"), col("brand")), 1.0).otherwise(0.0) * 0.4) +
        (when(array_contains(col("browsed_categories"), col("category")), 1.0).otherwise(0.0) * 0.6)
    ).withColumn(
        "final_score",
        (col("relevance_score") * 0.4) +
        (col("trending_score") * 0.2) +
        (col("avg_rating") * 0.2) +
        (col("conversion_rate") * 0.2)
    )

    # Rank top 10 per user
    window_spec = Window.partitionBy("user_id").orderBy(desc("final_score"))
    final_recommendations = recommendations.withColumn(
        "rank", row_number().over(window_spec)
    ).filter(col("rank") <= 10).withColumn(
        "recc_at", unix_timestamp(current_timestamp()) * 1000
    ).select(
        "user_id",
        col("product_id").alias("recc_item"),
        "final_score",
        "relevance_score",
        "source",
        "recc_at"
    )

    # Store to MongoDB
    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "content_based", final_recommendations, mode="overwrite")

    # Cleanup
    active_users.unpersist()
    user_preferences.unpersist()
    product_features.unpersist()
    category_affinity.unpersist()
    feature_based_recommendations.unpersist()
    content_based_recommendations.unpersist()
    recommendations.unpersist()
    final_recommendations.unpersist()


def best_sellers_all():
    best_selling_items = products_df\
        .select(
            "product_id",
            col("latest_sales_summary.last_30_days_sales").alias("sales"),
            "trending_score",
            "avg_rating"
        )\
        .orderBy(desc("sales"))

    best_selling_items = best_selling_items.withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "best_sellers", best_selling_items, mode="overwrite")


def new_arrivals_all():
    new_items = products_df\
        .select(
            "product_id",
            "trending_score",
            "avg_rating",
            "date_added"
        )\
        .orderBy(desc("date_added"))

    new_items = new_items.withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "new_arrivals", new_items, mode="overwrite")


def trending_products_all():
    trending_items = products_df\
        .select(
            "product_id",
            "trending_score",
            "latest_sales_summary",
            "avg_rating"
        )\
        .orderBy(desc("trending_score"))

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "trending_products", trending_items, mode="overwrite")


def seasonal_recommendations_all():
    # get current season
    current_month = datetime.now().month

    current_season = "fall"  # default to fall
    if current_month in [12, 1, 2]:
        current_season = "winter"
    elif current_month in [3, 4, 5]:
        current_season = "spring"
    elif current_month in [6, 7, 8]:
        current_season = "summer"

    seasonal_items = products_df\
        .select(
            "product_id",
            col(f"seasonal_sales.{current_season}.total_sold_unit").alias("seasonal_sales"),
            "trending_score",
            "avg_rating"
        )\
        .orderBy(desc("seasonal_sales"))

    seasonal_items = seasonal_items.withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "seasonal", seasonal_items, mode="overwrite")


def review_based_recommendations_all():
    top_rated_items = products_df\
        .select(
            "product_id",
            "avg_rating",
            "review_count",
            "trending_score"
        )\
        .orderBy(desc("avg_rating"))

    top_rated_items = top_rated_items.withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "review_based", top_rated_items, mode="overwrite")


def recently_viewed_based_recommendations():
    """
    Get personalized recommendations based on user's recently viewed products and their view duration.
    If user_id is None, generate recommendations for all users.
    """

    # Get user's browsing behavior with view durations
    browsing_data = users_df.select(
        "user_id",
        col("browsing_behavior.freq_views.recently_viewed_products").alias("products"),
        col("browsing_behavior.freq_views.recent_view_durations").alias("durations")
    )

    browsing_with_duration = browsing_data.select(
        "user_id",
        arrays_zip("products", "durations").alias("product_duration_pairs")
    )

    exploded_data = browsing_with_duration.select(
        "user_id",
        explode("product_duration_pairs").alias("pair")
    ).select(
        "user_id",
        col("pair.products").alias("product_id"),
        col("pair.durations").alias("view_duration")
    )

    # Get top 10 most viewed products for each user
    window_spec = Window.partitionBy("user_id").orderBy(desc("view_duration"))
    top_viewed_products = exploded_data.withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(col("rank") <= 10)

    # Get top 10 similar products for each viewed product
    recommendations = feature_similar_items.select(
        "user_id",
        "product_id",
        "view_duration",
        explode("feature_similar_items").alias("recommended_item")
    ).groupBy("user_id", "recommended_item").agg(
        first("product_id").alias("source_product"),
        max("view_duration").alias("source_view_duration")
    )

    # Get final recommendations
    window_spec_final = Window.partitionBy("user_id").orderBy(desc("source_view_duration"))
    final_recommendations = recommendations.withColumn(
        "rank",
        row_number().over(window_spec_final)
    ).filter(col("rank") <= 10).withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    ).select(
        "user_id",
        col("recommended_item").alias("recc_item"),
        lit("recently_viewed_based").alias("source"),
        "recc_at"
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "recently_viewed_based", final_recommendations, mode="overwrite")


def personalized_trending_products():
    """Personalized trending products based on user preferences and current trends
    Uses pre-calculated features from MongoDB instead of computing them again."""

    trending_base = products_df.select(
        "product_id",
        "category",
        "brand",
        "trending_score",
        "avg_rating",
        "conversion_rate",
        "view_count",
        "purchase_count",
        "wishlist_count",
        "cart_addition_count",
        col("latest_sales_summary.last_7_days_sales").alias("recent_sales"),
        col("latest_sales_summary.last_30_days_sales").alias("monthly_sales"),
        col("historical_sales_summary.7_day_trend").alias("weekly_trend"),
        col("historical_sales_summary.30_day_trend").alias("monthly_trend")
    )

    # Calculate engagement score using pre-calculated metrics
    trending_products = trending_base.withColumn(
        "velocity_score",
        when(col("monthly_sales") > 0,
            col("recent_sales") / (col("monthly_sales") / 4)  # Compare weekly to monthly average
        ).otherwise(0.0)
    ).withColumn(
        "engagement_score",
        (col("view_count") * 0.3 +
         col("purchase_count") * 0.4 +
         col("wishlist_count") * 0.2 +
         col("cart_addition_count") * 0.1) /
        greatest(col("view_count") + col("purchase_count") + col("wishlist_count") + col("cart_addition_count"), lit(1))
    ).withColumn(
        "trend_momentum",
        when(col("monthly_trend") > 0,
            col("weekly_trend") / col("monthly_trend")  # Relative growth rate
        ).otherwise(0.0)
    )

    user_prefs = users_df.select(
        col("user_id"),
        col("product_preferences.most_purchased_brands").alias("preferred_brands"),
        col("browsing_behavior.freq_views.categories").alias("browsed_categories"),
        col("price_sensitivity").alias("price_sensitivity")
    )

    # Personalize trending products
    trending_products = trending_products.crossJoin(user_prefs) \
        .withColumn(
        "personalization_score",
        (when(array_contains(col("preferred_brands"), col("brand")), 1.0).otherwise(0.0) * 0.3) +
        (when(array_contains(col("browsed_categories"), col("category")), 1.0).otherwise(0.0) * 0.7)
    )

    final_trending = trending_products.withColumn(
        "final_trending_score",
        (col("trending_score") * 0.25) +                 # Base trending score
        (col("velocity_score") * 0.20) +                 # Sales velocity
        (col("engagement_score") * 0.15) +               # User engagement
        (col("trend_momentum") * 0.15) +                 # Growth trend
        (col("conversion_rate") * 0.10) +                # Conversion effectiveness
        (col("personalization_score") * 0.10) +          # User relevance
        (col("price_sensitivity") * 0.05)                # Price sensitivity
    )

    # Rank and return top trending products
    window_spec = Window.partitionBy("user_id").orderBy(desc("final_trending_score"))
    final_recommendations = final_trending.withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(col("rank") <= 3).withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    ).select(
        "user_id",
        col("product_id").alias("recc_item"),
        "final_trending_score",
        "recent_sales",
        "avg_rating",
        "trend_momentum",
        "velocity_score",
        "engagement_score",
        "personalization_score",
        "conversion_rate",
        "recc_at"
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "personalized_trending", final_recommendations, mode="overwrite")


def run_all_recommendations():
    global users_df, products_df

    user_based_recommendations_all()
    item_based_recommendations_all()
    best_sellers_all()
    new_arrivals_all()
    trending_products_all()
    seasonal_recommendations_all()
    review_based_recommendations_all()
    recently_viewed_based_recommendations()
    personalized_trending_products()
    user_based_category_recommendations_all()
    users_df = users_df.repartition("user_id")
    products_df = products_df.repartition("category")
    content_based_recommendations_all()
