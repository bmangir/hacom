import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract

from backend.app.models.user_models import UBCF, ContentBased, PersonalizedTrending, RecentlyViewed
from backend.app.services.utils import _get_product_details
from backend.config import client, MONGO_BROWSING_DB, MONGO_URI

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.6.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 pyspark-shell --driver-memory 4g pyspark-shell'

spark = SparkSession.builder \
    .appName("MongoDBBrowsingHistory") \
    .master("local[*]") \
    .config("spark.mongodb.read.connection.uri", MONGO_URI) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .getOrCreate()

class UserRecommendationService:
    def __init__(self):
        self.user_browsing_history = spark.read \
            .format("mongodb") \
            .option("database", MONGO_BROWSING_DB) \
            .option("collection", "browsing_history") \
            .load()

    def get_ubcf_recommendations(self, user_id, num_recommendations=10):
        items = UBCF.objects(user_id=user_id).limit(num_recommendations)

        recc_items = []
        for item in items:
            recc_items.append(item.recc_item)

        return _get_product_details(recc_items)

    def get_content_based_recommendations(self, user_id, num_recommendations=10):
        items = ContentBased.objects(user_id=user_id).limit(num_recommendations)
        recc_items = []

        for item in items:
            recc_items.append(item.recc_item)

        return _get_product_details(recc_items)

    def get_personalized_trendings(self, user_id, num_recommendations=10):
        items = PersonalizedTrending.objects(user_id=user_id).order_by('-final_trending_score').limit(num_recommendations)
        recc_items = []

        for item in items:
            recc_items.append(item.recc_item)

        return _get_product_details(recc_items)

    def get_most_visited_categories(self, user_id, num_categories=5):
        # TODO: get this from user_agg data as browsing_behavior.freq_views.categories
        # Example query to get most visited categories

        filtered_history = self.user_browsing_history.filter(col("user_id") == user_id)

        browsed_df = filtered_history \
            .withColumn("product_id",
                        when(
                            col("page_url").rlike(r"^/product/[\w\d]+"),
                            regexp_extract(col("page_url"), r"/product/([\w\d]+)", 1)
                        ).when(
                            col("page_url").rlike(r"^/products/details/[\w\d]+"),
                            regexp_extract(col("page_url"), r"/products/details/([\w\d]+)", 1)
                        ).otherwise(None)) \
            .withColumn("category",
                        when(
                            col("page_url").rlike("^/category/"),
                            regexp_extract(col("page_url"), r"/category/([A-Za-z0-9_/]+)", 1)
                        ).otherwise(None)) \
            .select("user_id", "category")

        most_visited_categories = [row['category'] for row in browsed_df
                        .filter(col("category").isNotNull())
                        .groupBy("category")
                        .count()
                        .orderBy("count", ascending=False)
                        .limit(num_categories)
                        .collect()]

        return most_visited_categories


    def get_recently_viewed_based_recommendations(self, user_id, num_recommendations=10):
        items = RecentlyViewed.objects(user_id=user_id).order_by('-recc_at').limit(num_recommendations)
        recc_items = []

        for item in items:
            recc_items.append(item.recc_item)

        return _get_product_details(recc_items)
