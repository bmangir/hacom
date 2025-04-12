from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, ArrayType, StructType, StructField, StringType

from config import MONGO_AGG_DATA_DB, ITEM_FEATURES_HOST, USER_FEATURES_HOST
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
    "metadata",
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
    "latest_sales_summary"
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
    host = "https://user-features-demo-8dq5u7b.svc.aped-4627-b74a.pinecone.io"
    similarity_udf = find_similar_objects_udf(idx_host=host, top_k=15)
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
    similar_items = products_df.withColumn(
        "similar_items",
        similar_objects_udf(col("product_id"), ITEM_FEATURES_HOST)
    )

    # Get frequently bought together items
    frequently_bought = products_df.select(
        "product_id",
        col("historical_sales_summary.frequently_bought_together").alias("bought_together")
    )

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
        "metadata",
        "category",
        "brand",
        "trending_score",
        "avg_rating"
    )

    # Find similar items based on content
    similar_items = products_df.withColumn(
        "similar_items", 
        similar_objects_udf(col("product_id"), ITEM_FEATURES_HOST)
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
            "metadata",
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

# Main function to run all recommendations
def run_all_recommendations():
    # show recommendations
    a = user_based_recommendations_all()
    print("User-Based Recommendations:")
    a.show(5, truncate=False)
    print(a.count())
    a.filter(col("user_id") == "U10013").show(truncate=False)

    item_based_recommendations_all()
    print("Item-Based Recommendations:")
    item_based_recommendations_all().show(5, truncate=False)

    content_based_recommendations_all()
    print("Content-Based Recommendations:")
    content_based_recommendations_all().show(5, truncate=False)

    best_sellers_all()
    print("Best Sellers:")
    best_sellers_all().show(5, truncate=False)

    new_arrivals_all()
    print("New Arrivals:")
    new_arrivals_all().show(5, truncate=False)

    trending_products_all()
    print("Trending Products:")
    trending_products_all().show(5, truncate=False)

    seasonal_recommendations_all()
    print("Seasonal Recommendations:")
    seasonal_recommendations_all().show(5, truncate=False)

    review_based_recommendations_all()
    print("Review-Based Recommendations:")
    review_based_recommendations_all().show(5, truncate=False)

if __name__ == "__main__":
    run_all_recommendations()