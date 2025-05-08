import json

from flask import Flask, request
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, DoubleType

from Batch.UserBatchProcess.utility import vectorize
from config import USER_FEATURES_HOST, MONGO_AGG_DATA_DB
from utilities.spark_utility import create_spark_session, store_df_to_mongodb
from utilities.pinecone_utility import store_to_pinecone

app = Flask(__name__)

spark = create_spark_session("computer_registration")


@app.route("/api/v1/stream/new_registration_computer", methods=["POST"])
def generate_agg_data_and_vector_of_new_user():
    user_json = request.get_json()
    user_info = spark.read.json(spark.sparkContext.parallelize([json.dumps(user_json)]))

    user_info = user_info \
        .withColumn("winter", lit(0)) \
        .withColumn("spring", lit(0)) \
        .withColumn("summer", lit(0)) \
        .withColumn("fall", lit(0))

    map_from_seasons = create_map(
        lit("winter"), coalesce(col("winter"), lit(0)),
        lit("spring"), coalesce(col("spring"), lit(0)),
        lit("summer"), coalesce(col("summer"), lit(0)),
        lit("fall"), coalesce(col("fall"), lit(0))
    )

    user_info = user_info.withColumn("account_age_days", lit(0)) \
        .withColumn("device_type", lit("Unknown")) \
        .withColumn("payment_method", lit("Unknown")) \
        .withColumn("session_counts_last_30_days", lit(0)) \
        .withColumn("avg_session_duration_sec", lit(0.0)) \
        .withColumn("total_sessions", lit(0)) \
        .withColumn("recently_viewed_products", lit([])) \
        .withColumn("categories", lit([])) \
        .withColumn("products", lit([])) \
        .withColumn("recently_view_durations", lit([])) \
        .withColumn("avg_product_view_duration", lit([])) \
        .withColumn("avg_category_view_duration", lit([])) \
        .withColumn("overall_avg_category_view_duration", lit(0.0)) \
        .withColumn("overall_avg_product_view_duration", lit(0.0)) \
        .withColumn("total_views", lit(0)) \
        .withColumn("total_clicks", lit(0)) \
        .withColumn("total_add_to_cart", lit(0)) \
        .withColumn("total_add_to_wishlist", lit(0)) \
        .withColumn("total_purchase", lit(0)) \
        .withColumn("total_spent", lit(0.0)) \
        .withColumn("avg_order_value", lit(0.0)) \
        .withColumn("avg_orders_per_month", lit(0)) \
        .withColumn("recently_purchased_products", lit([])) \
        .withColumn("recent_purchase_counts", lit([])) \
        .withColumn("recent_purchase_amounts", lit([])) \
        .withColumn("seasonal_data", map_from_seasons) \
        .withColumn("most_purchased_brands", lit([])) \
        .withColumn("avg_review_rating", lit(0.0)) \
        .withColumn("review_count", lit(0)) \
        .withColumn("avg_rating_per_category", lit([])) \
        .withColumn("avg_view_duration", lit(0)) \
        .withColumn("total_impressions", lit(0))

    user_info = user_info \
        .withColumn("user_profile",
                    struct(
                        coalesce(col("gender"), lit("Other")).alias("gender"),
                        col("age").alias("age"),
                        col("location").alias("location"),
                        coalesce("account_age_days", lit(0)).alias("account_age_days"),
                        coalesce("device_type", lit("Unknown")).alias("device_type"),
                        coalesce("payment_method", lit("Unknown")).alias("payment_method"),
                        coalesce("session_counts_last_30_days", lit(0)).alias("session_counts_last_30_days"),
                        coalesce("avg_session_duration_sec", lit(0.0)).alias("avg_session_duration_sec"),
                        coalesce("total_sessions", lit(0)).alias("total_sessions"))
                    ).withColumn(
        "browsing_behavior",
        struct(
            struct(
                coalesce("recently_viewed_products", lit([])).cast(ArrayType(StringType())).alias("recently_viewed_products"),
                coalesce("categories", lit([])).cast(ArrayType(StringType())).alias("categories"),
                coalesce("products", lit([])).cast(ArrayType(StringType())).alias("products"),
                coalesce("recently_view_durations", lit([])).cast(ArrayType(FloatType())).alias("recently_view_durations"),
                coalesce("avg_product_view_duration", lit([])).cast(ArrayType(FloatType())).alias("avg_product_view_duration"),
                coalesce("avg_category_view_duration", lit([])).cast(ArrayType(FloatType())).alias("avg_category_view_duration"),
                coalesce("overall_avg_category_view_duration", lit(0.0)).alias("overall_avg_category_view_duration"),
                coalesce("overall_avg_product_view_duration", lit(0.0)).alias("overall_avg_product_view_duration"),
            ).alias("freq_views"),
            coalesce("total_views", lit(0)).alias("total_views"),
            coalesce("total_clicks", lit(0)).alias("total_clicks"),
            coalesce("total_add_to_cart", lit(0)).alias("total_add_to_cart"),
            coalesce("total_add_to_wishlist", lit(0)).alias("total_add_to_wishlist"),
        )
    ).withColumn(
        "purchase_behavior",
        struct(
            coalesce(col("total_purchase").cast(IntegerType()), lit(0)).alias("total_purchase"),
            coalesce(col("total_spent").cast(DoubleType()), lit(0.0)).alias("total_spent"),
            coalesce(col("avg_order_value").cast(DoubleType()), lit(0.0)).alias("avg_order_value"),
            coalesce(col("avg_orders_per_month").cast(IntegerType()), lit(0)).alias("avg_orders_per_month"),
            coalesce(col("recently_purchased_products").cast(ArrayType(StringType())), lit([])).alias("recently_purchased_products"),
            coalesce(col("recent_purchase_counts").cast(ArrayType(IntegerType())), lit([])).alias("recent_purchase_counts"),
            coalesce(col("recent_purchase_amounts").cast(ArrayType(DoubleType())), lit([])).alias("recent_purchase_amounts"),
            col("seasonal_data")
        )
    ).withColumn("product_preferences",
                 struct(
                     coalesce("most_purchased_brands", lit([])).cast(ArrayType(StringType())).alias("most_purchased_brands"),
                     coalesce("avg_review_rating", lit(0.0)).alias("avg_review_rating"),
                     coalesce("review_count", lit(0)).alias("review_count"),
                     coalesce("avg_rating_per_category", lit([])).cast(ArrayType(FloatType())).alias("avg_rating_per_category"),
                 )
                 ).withColumn("ctr", lit(0.0)
                              ).withColumn("loyalty_score", lit(0.0)
                                           ).withColumn("engagement_score", lit(0.0)
                                                        ).withColumn("preference_stability", lit(0.0)
                                                                     ).withColumn("price_sensitivity", lit(0.0)
                                                                                  ).withColumn("category_exploration", lit(0.0)
                                                                                               ).withColumn("brand_loyalty", lit(0.0)) \
        .select("user_id", "user_profile", "browsing_behavior", "purchase_behavior", "product_preferences", "ctr", "loyalty_score",
                "engagement_score", "preference_stability", "price_sensitivity", "category_exploration", "brand_loyalty", "total_impressions",
                "avg_view_duration")


    vectorize_udf = udf(vectorize, ArrayType(FloatType()))

    vectorized_df = user_info.withColumn(
        "values",
        vectorize_udf(
            col("ctr").cast("float"), col("user_profile"), col("browsing_behavior"), col("purchase_behavior"), col("product_preferences"),
            col("loyalty_score"), col("engagement_score"), col("preference_stability"), col("price_sensitivity"),
            col("category_exploration"), col("brand_loyalty"), col("total_impressions"), col("avg_view_duration")
        )
    )

    store_df_to_mongodb(
        df=user_info,
        collection_name="user_features",
        db_name=MONGO_AGG_DATA_DB,
        mode="append")

    # store to pinecone
    store_to_pinecone(vectors_df=vectorized_df.withColumnRenamed("user_id", "id").select("id", "values"),
                      host=USER_FEATURES_HOST, batch_size=400)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8081, debug=True)