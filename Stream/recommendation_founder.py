from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, StructType, StructField, StringType

from config import MONGO_AGG_DATA_DB, USER_FEATURES_HOST, ITEM_FEATURES_HOST
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


)

users_df = read_from_mongodb(
    spark=spark,
    db_name=MONGO_AGG_DATA_DB,
    coll_name="user_features",
    filter={}
).select(
    "user_id",
    "user_profile",
    "browsing_behavior",
    "purchase_behavior",
    "product_preferences",
    "ctr"
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

#similar_objects_udf = udf(find_similar_objects, ArrayType(FloatType()))

# UBCF: People who behave like you also liked X
def user_based_recommendations_all():
    # Find similar users based on user features
    similarity_udf = find_similar_objects_udf(idx_host=USER_FEATURES_HOST, top_k=15)
    similar_users = users_df.withColumn(
        "similar_users",
        similarity_udf(col("user_id"))
    )

    similar_users.show(5)
    print(similar_users.count())

    # Get browsing and purchase history of similar users
    similar_users_items = users_df.select(
        "user_id",
        col("browsing_behavior.freq_views.products").alias("viewed_products"),
        col("browsing_behavior.freq_views.categories").alias("viewed_categories"),
        col("purchase_behavior.recently_purchased_products").alias("purchased_products"),
        col("product_preferences.most_purchased_brands").alias("preferred_brands")
    )

    similar_users_items.show(5)
    print(similar_users_items.count())

    # Join and get recommendations
    recommendations = similar_users\
        .join(similar_users_items, "user_id") \
        .select(
            col("user_id").alias("user_id"),
            col("viewed_products").alias("recc_item"),
            lit("viewed").alias("source")
        ).union(
            similar_users\
                .join(similar_users_items, "user_id")
                .select(
                    col("user_id").alias("user_id"),
                    col("purchased_products").alias("recc_item"),
                    lit("purchased").alias("source")
                )
        )

    store_df_to_mongodb("mongo_recommendations", "ubcf", recommendations)

    return recommendations

# IBCF: Customers who bought this also bought Y
def item_based_recommendations_all():
    # Find similar items based on item features
    similarity_udf = find_similar_objects_udf(idx_host=ITEM_FEATURES_HOST, top_k=3)
    similar_items = products_df.withColumn(
        "similar_items",
        similarity_udf(col("product_id"))
    ).select("product_id", "similar_items")
    similar_items.show(5)

    # Get frequently bought together items
    frequently_bought = products_df.select(
        "product_id",
        col("bought_together")
    )
    frequently_bought.show(5)

    # Combine similar items and frequently bought together
    recommendations = similar_items\
        .withColumn("recc_items", explode(col("similar_items"))) \
        .select(
            col("product_id"),
            col("recc_items"),
            lit("similar").alias("source")
        ).union(
            frequently_bought\
                .withColumn("recc_items", explode(col("bought_together"))) \
                .select(
                    col("product_id"),
                    col("recc_items"),
                    lit("bought_together").alias("source")
                )
        )

    store_df_to_mongodb("mongo_recommendations", "ibcf", recommendations)

    return recommendations

# Content-Based: Since you liked sneakers, here are similar styles
def content_based_recommendations_all():
    # Get user preferences
    user_preferences = users_df.select(
        "user_id",
        col("product_preferences.most_purchased_brands").alias("preferred_brands"),
        col("browsing_behavior.freq_views.categories").alias("preferred_categories")
    )

    # Get product metadata and features
    product_features = products_df.select(
        "product_id",
        "category",
        "brand",
        "trending_score",
        "avg_rating"
    )

    # Find similar items based on content
    similarity_udf = find_similar_objects_udf(idx_host=ITEM_FEATURES_HOST, top_k=15)
    similar_items = products_df.withColumn(
        "similar_items",
        similarity_udf(col("product_id"))
    )

    # Join user preferences with product features
    recommendations = user_preferences\
        .join(product_features, 
            (array_contains(col("preferred_brands"), col("brand"))) | 
            (array_contains(col("preferred_categories"), col("category")))
        )\
        .select(
            col("user_id"),
            col("product_id").alias("recc_item"),
            lit("content_based").alias("source")
        )

    store_df_to_mongodb("mongo_recommendations", "content_based", recommendations)
    return recommendations

# Best Sellers: "En Çok Satanlar"
def best_sellers_all():
    best_selling_items = products_df\
        .select(
            "product_id",
            col("latest_sales_summary.last_30_days_sales").alias("sales"),
            "trending_score",
            "avg_rating"
        )\
        .orderBy(desc("sales"))

    store_df_to_mongodb("mongo_recommendations", "best_sellers", best_selling_items)
    return best_selling_items

# New Arrivals: "Yeni Gelenler"
def new_arrivals_all():
    new_items = products_df\
        .select(
            "product_id",
            "trending_score",
            "avg_rating",
            "date_added"
        )\
        .orderBy(desc("date_added"))

    store_df_to_mongodb("mongo_recommendations", "new_arrivals", new_items)
    return new_items

# Trending Products: "Trend Ürünler"
def trending_products_all():
    trending_items = products_df\
        .select(
            "product_id",
            "trending_score",
            "latest_sales_summary",
            "avg_rating"
        )\
        .orderBy(desc("trending_score"))

    store_df_to_mongodb("mongo_recommendations", "trending_products", trending_items)
    return trending_items

# Seasonal Recommendations: "Mevsimsel Öneriler"
def seasonal_recommendations_all():
    # Get current season
    current_season = when(
        (month(current_date()).isin(12, 1, 2)), "Winter"
    ).when(
        (month(current_date()).isin(3, 4, 5)), "Spring"
    ).when(
        (month(current_date()).isin(6, 7, 8)), "Summer"
    ).otherwise("Fall")

    # Get seasonal recommendations
    seasonal_items = products_df\
        .select(
            "product_id",
            col(f"seasonal_sales.{current_season}.total_sold_unit").alias("seasonal_sales"),
            "trending_score",
            "avg_rating"
        )\
        .orderBy(desc("seasonal_sales"))

    store_df_to_mongodb("mongo_recommendations", "seasonal", seasonal_items)
    return seasonal_items

# Review-Based Recommendations: "Yorumlara Göre Öneriler"
def review_based_recommendations_all():
    top_rated_items = products_df\
        .select(
            "product_id",
            "avg_rating",
            col("historical_sales_summary.review_count").alias("review_count"),
            "trending_score"
        )\
        .orderBy(desc("avg_rating"))

    store_df_to_mongodb("mongo_recommendations", "review_based", top_rated_items)
    return top_rated_items

# Recently Viewed Based Recommendations: "Son Gezilen Ürünlere Benzer Ürünler"
def recently_viewed_based_recommendations(user_id=None):
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

    store_df_to_mongodb("mongo_recommendations", "recently_viewed_based", final_recommendations)
    return final_recommendations

# Main function to run all recommendations
def run_all_recommendations():
    # show recommendations
    a = user_based_recommendations_all()
    print("User-Based Recommendations:")
    a.show(5, truncate=False)
    print(a.count())
    a.filter(col("user_id") == "U10013").show(truncate=False)

    b = item_based_recommendations_all()
    print("Item-Based Recommendations:")
    b.show(5, truncate=False)

    c = content_based_recommendations_all()
    print("Content-Based Recommendations:")
    c.show(5, truncate=False)

    d = best_sellers_all()
    print("Best Sellers:")
    d.show(5, truncate=False)

    e = new_arrivals_all()
    print("New Arrivals:")
    e.show(5, truncate=False)

    f = trending_products_all()
    print("Trending Products:")
    f.show(5, truncate=False)

    g = seasonal_recommendations_all()
    print("Seasonal Recommendations:")
    g.show(5, truncate=False)

    h = review_based_recommendations_all()
    print("Review-Based Recommendations:")
    h.show(5, truncate=False)

    i = recently_viewed_based_recommendations()
    print("Recently Viewed Based Recommendations:")
    i.show(5, truncate=False)

    print("done")

if __name__ == "__main__":
    run_all_recommendations()