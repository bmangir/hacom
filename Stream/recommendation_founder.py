from datetime import datetime

from pinecone import Pinecone
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, StructType, StructField, StringType, LongType
import numpy as np

from Batch.UserBatchProcess.utility import distribution_of_df_by_range
from config import MONGO_AGG_DATA_DB, USER_FEATURES_HOST, ITEM_FEATURES_HOST, ITEM_CONTENTS_HOST, MONGO_URI, \
    MONGO_PRODUCTS_DB, PINECONE_API_KEY, client, MONGO_PRECOMPUTED_DB, MONGO_RECOMMENDATION_DB
from utilities.pinecone_utility import find_similar_objects
from utilities.spark_utility import read_from_mongodb, read_postgres_table, create_spark_session, store_df_to_mongodb


spark = create_spark_session("reader")

# Read aggregated data from MongoDB
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

def find_similar_objects_udf(idx_host, top_k):
    """Creates a UDF for finding similar objects."""
    def inner_udf(id):
        result = find_similar_objects(id, idx_host, top_k)
        if result is None:
            return None
        else:
            return result

    return udf(inner_udf, ArrayType(StringType()))


# UBCF: People who behave like you also liked X
def user_based_recommendations_all():
    """
    Optimized user-based collaborative filtering recommendations
    """
    # First delete old recommendations in collection
    client[MONGO_RECOMMENDATION_DB]["ubcf"].delete_many({})

    # Cache frequently used DataFrames
    products_df.cache()
    users_df.cache()

    # 1. Find similar users more efficiently
    similarity_udf = find_similar_objects_udf(idx_host=USER_FEATURES_HOST, top_k=2)

    # Only process users with activity
    active_users = users_df.filter(
        size(col("browsing_behavior.freq_views.products")) > 0
    ).select("user_id")

    # Repartition the data for better parallel processing
    active_users = active_users.repartition(10)  # Adjust number based on your cluster size

    # Process in parallel
    similar_users = active_users.withColumn(
        "similar_users",
        similarity_udf(col("user_id"))
    ).cache()

    #similar_users = get_similar_objects_from_mongodb(coll_name="similar_users", db_name=MONGO_PRECOMPUTED_DB).select(
    #    col("user_id").alias("target_user_id"),
    #    col("similar_users")
    #).cache()

    # 2. Get user items more efficiently - combine viewed and purchased in one go
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

    # 3. Process cold-start cases separately
    cold_start_users = users_df.filter(
        size(col("browsing_behavior.freq_views.products")) == 0
    ).select("user_id")

    # Get top-rated products efficiently
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

    # 4. Generate recommendations for active users more efficiently
    similar_users_expanded = similar_users.select(
        col("user_id").alias("target_user_id"),
        explode(col("similar_users")).alias("similar_user_id")
    )

    # Join with user items in one step
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

    # 5. Combine recommendations and add product context efficiently
    all_recommendations = active_recommendations.union(cold_start_recommendations)

    # Add product context with optimized join
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

    # 6. Calculate final scores and rank efficiently
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

    # Use single window operation for both deduplication and ranking
    window_spec = Window.partitionBy("user_id").orderBy(desc("recommendation_score"))

    final_recommendations = final_recommendations.withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(col("rank") <= 2)  # Get top 10 recommendations per user

    # Add recommendation timestamp
    final_recommendations = final_recommendations.withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    )

    # Optimize the final DataFrame for MongoDB write
    final_recommendations = final_recommendations.select(
        "user_id",
        "recc_item",
        "recommendation_score",
        "recc_at"
    )

    # Store in MongoDB with optimized settings
    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "ubcf", final_recommendations)

    # Uncache DataFrames
    products_df.unpersist()
    users_df.unpersist()
    similar_users.unpersist()
    user_items.unpersist()


# IBCF: Customers who bought this also bought Y
def item_based_recommendations_all(products_df):
    # First delete old recommendations in collection
    client[MONGO_RECOMMENDATION_DB]["ibcf"].delete_many({})

    # Cache the products dataframe
    products_df.cache()

    # Repartition the data for better parallel processing
    products_df = products_df.repartition(10)  # Adjust number based on your cluster size

    # Find similar items based on item features and contents
    similarity_feature_udf = find_similar_objects_udf(idx_host=ITEM_FEATURES_HOST, top_k=2)
    similarity_content_udf = find_similar_objects_udf(idx_host=ITEM_CONTENTS_HOST, top_k=2)

    # Get similar items based on feature vectors (behavioral)
    feature_similar_items = products_df.withColumn(
        "feature_similar_items",
        similarity_feature_udf(col("product_id"))
    ).select("product_id", "feature_similar_items")

    # Get similar items based on content vectors (metadata)
    content_similar_items = products_df.withColumn(
        "content_similar_items",
        similarity_content_udf(col("product_id"))
    ).select("product_id", "content_similar_items")

    # Get frequently bought together items
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

    # Combine all recommendations using a more efficient approach
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
    ).filter(col("rank") <= 10)

    # Add recommendation timestamp
    final_recommendations = final_recommendations.withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "ibcf", final_recommendations)

# Content-Based: Since you liked sneakers, here are similar styles
def content_based_recommendations_all(products_df, users_df):
    # First delete old recommendations in collection
    client[MONGO_RECOMMENDATION_DB]["content_based"].delete_many({})

    # Cache frequently used DataFrames
    products_df.cache()
    users_df.cache()

    # Repartition the data for better parallel processing
    products_df = products_df.repartition(10)
    users_df = users_df.repartition(10)

    # Get user preferences with available features
    user_preferences = users_df.select(
        "user_id",
        col("product_preferences.most_purchased_brands").alias("preferred_brands"),
        col("browsing_behavior.freq_views.categories").alias("browsed_categories"),
        col("price_sensitivity")
    )

    # Get product metadata and features with available attributes
    product_features = products_df.select(
        "product_id",
        "category",
        "brand",
        "trending_score",
        "avg_rating",
        "conversion_rate",
        col("latest_sales_summary.last_30_days_sales").alias("monthly_sales"),
        "seasonal_sales"
    )

    # Find similar items based on content vectors using parallel processing
    similarity_udf = find_similar_objects_udf(idx_host=ITEM_CONTENTS_HOST, top_k=15)
    similar_items = products_df.withColumn(
        "similar_items",
        similarity_udf(col("product_id"))
    )

    # Calculate category affinity scores using actual browsing data
    category_affinity = user_preferences.select(
        "user_id",
        "browsed_categories"
    ).withColumn(
        "category_scores",
        explode("browsed_categories")
    ).groupBy("user_id", "category_scores").count()

    # Feature-based recommendations
    feature_based_recommendations = user_preferences\
        .crossJoin(product_features)\
        .join(category_affinity, "user_id")\
        .where(
            (array_contains(col("preferred_brands"), col("brand"))) |
            (col("category") == col("category_scores"))
        )\
        .select(
            col("user_id").alias("feature_user_id"),
            col("product_id").alias("feature_product_id"),
            col("trending_score").alias("feature_trending_score"),
            col("avg_rating").alias("feature_avg_rating"),
            col("conversion_rate").alias("feature_conversion_rate"),
            col("brand").alias("feature_brand"),
            col("category").alias("feature_category"),
            lit("feature_based").alias("feature_source")
        )

    # Content-based recommendations (similar items approach)
    content_based_recommendations = similar_items\
        .select(
            col("product_id").alias("content_product_id"),
            explode("similar_items").alias("similar_product_id")
        )\
        .join(product_features, col("content_product_id") == product_features.product_id)\
        .select(
            col("similar_product_id").alias("content_product_id"),
            col("trending_score").alias("content_trending_score"),
            col("avg_rating").alias("content_avg_rating"),
            col("conversion_rate").alias("content_conversion_rate"),
            col("brand").alias("content_brand"),
            col("category").alias("content_category"),
            lit("content_similar").alias("content_source")
        )\
        .crossJoin(user_preferences.select(col("user_id").alias("content_user_id")))

    # Combine both approaches using join instead of union
    recommendations = feature_based_recommendations\
        .join(content_based_recommendations,
              (feature_based_recommendations.feature_user_id == content_based_recommendations.content_user_id) &
              (feature_based_recommendations.feature_product_id == content_based_recommendations.content_product_id),
              "full")\
        .select(
            coalesce(
                feature_based_recommendations.feature_user_id,
                content_based_recommendations.content_user_id
            ).alias("user_id"),
            coalesce(
                feature_based_recommendations.feature_product_id,
                content_based_recommendations.content_product_id
            ).alias("product_id"),
            coalesce(
                feature_based_recommendations.feature_trending_score,
                content_based_recommendations.content_trending_score
            ).alias("trending_score"),
            coalesce(
                feature_based_recommendations.feature_avg_rating,
                content_based_recommendations.content_avg_rating
            ).alias("avg_rating"),
            coalesce(
                feature_based_recommendations.feature_conversion_rate,
                content_based_recommendations.content_conversion_rate
            ).alias("conversion_rate"),
            coalesce(
                feature_based_recommendations.feature_brand,
                content_based_recommendations.content_brand
            ).alias("brand"),
            coalesce(
                feature_based_recommendations.feature_category,
                content_based_recommendations.content_category
            ).alias("category"),
            coalesce(
                feature_based_recommendations.feature_source,
                content_based_recommendations.content_source
            ).alias("source")
        )

    # Join back with user preferences to get preferred_brands and browsed_categories
    recommendations = recommendations.join(
        user_preferences.select(
            "user_id",
            "preferred_brands",
            "browsed_categories"
        ),
        "user_id",
        "left"
    )

    # Calculate recommendation score based on available factors
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

    # Rank and filter recommendations
    window_spec = Window.partitionBy("user_id").orderBy(desc("final_score"))
    final_recommendations = recommendations.withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(col("rank") <= 10)\
    .select(
        "user_id",
        col("product_id").alias("recc_item"),
        "final_score",
        "relevance_score",
        lit("content_based").alias("source")
    )

    # Add recommendation timestamp
    final_recommendations = final_recommendations.withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "content_based", final_recommendations)

# Best Sellers: "En Çok Satanlar"
def best_sellers_all():
    # First delete old recommendations in collection
    client[MONGO_RECOMMENDATION_DB]["best_sellers"].delete_many({})

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

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "best_sellers", best_selling_items)

# New Arrivals: "Yeni Gelenler"
def new_arrivals_all():
    # First delete old recommendations in collection
    client[MONGO_RECOMMENDATION_DB]["new_arrivals"].delete_many({})

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

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "new_arrivals", new_items)

# Trending Products: "Trend Ürünler"
def trending_products_all():
    # First delete old recommendations in collection
    client[MONGO_RECOMMENDATION_DB]["trending_products"].delete_many({})

    trending_items = products_df\
        .select(
            "product_id",
            "trending_score",
            "latest_sales_summary",
            "avg_rating"
        )\
        .orderBy(desc("trending_score"))

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "trending_products", trending_items)

# Seasonal Recommendations: "Mevsimsel Öneriler"
def seasonal_recommendations_all():
    # First delete old recommendations in collection
    client[MONGO_RECOMMENDATION_DB]["seasonal"].delete_many({})

    # get current season
    current_month = datetime.now().month

    current_season = "fall"  # default to fall
    if current_month in [12, 1, 2]:
        current_season = "winter"
    elif current_month in [3, 4, 5]:
        current_season = "spring"
    elif current_month in [6, 7, 8]:
        current_season = "summer"

    # Get seasonal recommendations
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

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "seasonal", seasonal_items)

# Review-Based Recommendations: "Yorumlara Göre Öneriler"
def review_based_recommendations_all():
    # First delete old recommendations in collection
    client[MONGO_RECOMMENDATION_DB]["review_based"].delete_many({})

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

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "review_based", top_rated_items)

# Recently Viewed Based Recommendations: "Son Gezilen Ürünlere Benzer Ürünler"
def recently_viewed_based_recommendations(user_id=None):
    """
    Get personalized recommendations based on user's recently viewed products and their view duration.
    If user_id is None, generate recommendations for all users.
    """
    # First delete old recommendations in collection
    client[MONGO_RECOMMENDATION_DB]["recently_viewed_based"].delete_many({})

    # Get user's browsing behavior with view durations
    browsing_data = users_df.select(
        "user_id",
        col("browsing_behavior.freq_views.recently_viewed_products").alias("products"),
        col("browsing_behavior.freq_views.recent_view_durations").alias("durations")
    )

    # Create arrays of products and their view durations
    browsing_with_duration = browsing_data.select(
        "user_id",
        arrays_zip("products", "durations").alias("product_duration_pairs")
    )

    # Explode the arrays and create separate rows for each product-duration pair
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

    # Find similar products for each viewed product
    similarity_udf = find_similar_objects_udf(idx_host=ITEM_FEATURES_HOST, top_k=15)
    similar_items = top_viewed_products.withColumn(
        "similar_items",
        similarity_udf(col("product_id"))
    )

    # Get top 5 similar products for each viewed product
    recommendations = similar_items.select(
        "user_id",
        "product_id",
        "view_duration",
        explode("similar_items").alias("recommended_item")
    ).groupBy("user_id", "recommended_item").agg(
        first("product_id").alias("source_product"),
        max("view_duration").alias("source_view_duration")
    )

    # Get final recommendations
    window_spec_final = Window.partitionBy("user_id").orderBy(desc("source_view_duration"))
    final_recommendations = recommendations.withColumn(
        "rank",
        row_number().over(window_spec_final)
    ).filter(col("rank") <= 5).select(
        "user_id",
        col("recommended_item").alias("recc_item"),
        lit("recently_viewed_based").alias("source")
    )

    # If user_id is provided, filter for that specific user
    if user_id:
        final_recommendations = final_recommendations.filter(col("user_id") == user_id)

    # Add recommendation timestamp
    final_recommendations = final_recommendations.withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "recently_viewed_based", final_recommendations)


def personalized_trending_products(user_id=None):
    """Personalized trending products based on user preferences and current trends
    Uses pre-calculated features from MongoDB instead of computing them again."""
    # First delete old recommendations in collection
    client[MONGO_RECOMMENDATION_DB]["personalized_trending"].delete_many({})

    # Get pre-calculated features from products_df
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

    if user_id:
        # Get user preferences
        user_prefs = users_df.filter(col("user_id") == user_id).select(
            col("product_preferences.most_purchased_brands").alias("preferred_brands"),
            col("product_preferences.most_viewed_categories").alias("preferred_categories"),
            col("browsing_behavior.freq_views.categories").alias("browsed_categories"),
            col("product_preferences.price_sensitivity").alias("price_sensitivity")
        )

        # Personalize trending products
        trending_products = trending_products.crossJoin(user_prefs)\
            .withColumn(
                "personalization_score",
                (when(array_contains(col("preferred_brands"), col("brand")), 1.0).otherwise(0.0) * 0.4) +
                (when(array_contains(col("preferred_categories"), col("category")), 1.0).otherwise(0.0) * 0.3) +
                (when(array_contains(col("browsed_categories"), col("category")), 1.0).otherwise(0.0) * 0.3)
            )
    else:
        # For non-personalized trending, use general popularity
        trending_products = trending_products.withColumn(
            "personalization_score",
            lit(1.0)
        )

    # Calculate final trending score using pre-calculated metrics
    final_trending = trending_products.withColumn(
        "final_trending_score",
        (col("trending_score") * 0.25) +                 # Base trending score
        (col("velocity_score") * 0.20) +                 # Sales velocity
        (col("engagement_score") * 0.15) +               # User engagement
        (col("trend_momentum") * 0.15) +                 # Growth trend
        (col("conversion_rate") * 0.15) +                # Conversion effectiveness
        (col("personalization_score") * 0.10)            # User relevance
    )

    # Rank and return top trending products
    window_spec = Window.orderBy(desc("final_trending_score"))
    final_recommendations = final_trending.withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(col("rank") <= 20)\
    .select(
        "product_id",
        "category",
        "brand",
        "final_trending_score",
        "recent_sales",
        "avg_rating",
        "trend_momentum",
        "velocity_score"
    )

    # Add recommendation timestamp
    final_recommendations = final_recommendations.withColumn(
        "recc_at",
        unix_timestamp(current_timestamp()) * 1000
    )

    store_df_to_mongodb(MONGO_RECOMMENDATION_DB, "personalized_trending", final_recommendations)

# Main function to run all recommendations
def run_all_recommendations():
    start = datetime.now()
    print("User-Based Recommendations:")
    user_based_recommendations_all()

    print("Item-Based Recommendations:")
    item_based_recommendations_all(products_df)

    print("Content-Based Recommendations:")
    content_based_recommendations_all(products_df, users_df)

    print("Best Sellers Recommendations:")
    best_sellers_all()
    print("New Arrivals Recommendations:")
    new_arrivals_all()
    print("Trending Products Recommendations:")
    trending_products_all()
    print("Seasonal Recommendations:")
    seasonal_recommendations_all()
    print("Review-Based Recommendations:")
    review_based_recommendations_all()
    print("Recently Viewed Based Recommendations:")
    recently_viewed_based_recommendations()
    print("Personalized Trending Products:")
    personalized_trending_products()

    end = datetime.now()
    print(f"All recommendations completed in {end - start} seconds.")
    print("done")

if __name__ == "__main__":
    run_all_recommendations()