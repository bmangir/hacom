import time
from datetime import datetime, timedelta

from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType

from Batch.models import user_model as UserModel
from utility import extract_place_name, transform_data, distribution_of_df_by_range, vectorize
from utilities.spark_utility import create_spark_session

user_feature_ext_sj = create_spark_session(app_name="user-features-extraction-job")
user_feature_vectorization_sj = create_spark_session(app_name="user-features-vectorization-job")
reader_sj = create_spark_session(app_name="user-reader-job")


class UserBatchService:
    def __init__(self):
        (self.users_df,
         self.orders_df,
         self.reviews_df,
         self.cart_df,
         self.wishlist_df,
         self.browsing_df,
         self.clickstream_df,
         self.session_df,
         self.products_df,
         self.users_ids_df, _) = UserModel.fetch_data(spark=user_feature_ext_sj)

        self.preprocess_data()

    def preprocess_data(self):
        extract_place_name_udf = udf(extract_place_name, StringType())
        self.users_df = self.users_df \
            .withColumn("age", (datediff(current_date(), col("birth_date")) / 365).cast("int")) \
            .withColumn("location", extract_place_name_udf(col("address"))) \
            .withColumn("gender",
                        when(col("gender") == "F", "Female")
                        .when(col("gender") == "M", "Male")
                        .when(col("gender") == "U", "Unisex")
                        .otherwise("P")) \
            .withColumn("account_age_days", datediff(current_date(), col("join_date").cast("date"))) \
            .drop("address", "birth_date", "join_date")

        self.clickstream_df = self.clickstream_df \
            .withColumn("is_click", when((col("action") == "view") & (col("duration_seconds") > 0), 1).otherwise(0)) \
            .withColumn("is_impression", when(col("action") == "view", 1).otherwise(0))

        self.session_df = self.session_df.withColumn("session_duration", (col("end_time") - col("start_time")) / 1000)

        # Assign interaction weights
        self.orders_df = self.orders_df.withColumn("interaction_weight", lit(1.0))  # Purchases are strongest
        self.reviews_df = self.reviews_df.withColumn("interaction_weight", col("rating") / 5)  # Normalize ratings
        self.browsing_df = self.browsing_df.withColumn("interaction_weight", lit(0.2))  # Browsing is weaker

    def _get_user_profile(self, ):
        # Get Device Preferences
        device_preference_df = self.session_df.groupBy("user_id", "device_type") \
            .agg(sum("session_duration").alias("total_duration"), count("*").alias("session_count"))

        window_spec_for_device_type = Window.partitionBy("user_id").orderBy(desc("total_duration"))
        preferred_device = device_preference_df \
            .withColumn("rank", row_number().over(window_spec_for_device_type)) \
            .filter(col("rank") == 1) \
            .join(self.users_ids_df, "user_id", "right")

        # Get Payment Method Preferences
        payment_method_preference_df = self.orders_df \
            .groupBy("user_id", "payment_method") \
            .agg(count("*").alias("used_method_count"))

        window_spec_for_payment_method = Window.partitionBy("user_id").orderBy(desc("used_method_count"))
        preferred_payment_method = payment_method_preference_df \
            .withColumn("rank", row_number().over(window_spec_for_payment_method)) \
            .filter(col("rank") == 1) \
            .join(self.users_ids_df, "user_id", "right")

        # Get Session Stats
        session_count_last_30_days = self.session_df \
            .filter(col("start_time") >= int(time.time()) - (30 * 24 * 60 * 60)) \
            .groupBy("user_id") \
            .agg(count("session_id").alias("session_counts_last_30_days")) \
            .join(self.users_ids_df, "user_id", "right") \
            .fillna({"session_counts_last_30_days": 0})
        #session_count_last_30_days = self.users_ids_df.join(session_count_last_30_days, "user_id", "left").fillna({"session_counts_last_30_days": 0})

        avg_session_duration_all_history = self.session_df \
            .groupBy("user_id") \
            .agg(round(avg("session_duration"), 2).alias("avg_session_duration_sec")) \
            .join(self.users_ids_df, "user_id", "right") \
            .fillna({"avg_session_duration_sec": 0.0})
        #avg_session_duration_all_history = self.users_ids_df.join(avg_session_duration_all_history, "user_id", "left").fillna({"avg_session_duration_sec": 0.0})

        #user_session_stats = session_count_last_30_days.join(avg_session_duration_all_history, "user_id", "left")

        # Convert session duration to seconds for readability
        user_demographics_df = self.users_df \
            .join(preferred_device, "user_id", "left") \
            .join(preferred_payment_method, "user_id", "left") \
            .join(session_count_last_30_days, "user_id", "left") \
            .join(avg_session_duration_all_history, "user_id", "left") \
            .fillna({
            "device_type": "Unknown",
            "payment_method": "Unknown",
            "session_counts_last_30_days": 0,
            "avg_session_duration_sec": 0.0
        })

        return user_demographics_df.select("user_id", "gender", "age", "location", "account_age_days", "device_type",
                                           "payment_method", "session_counts_last_30_days", "avg_session_duration_sec")

    def _get_browsing_behavior(self, ):
        # Get last 30 days viewed products
        last_30_days_views = self.browsing_df.filter(
            col("browsing_timestamp") >= current_timestamp() - expr("INTERVAL 30 DAYS")
        ).groupBy("user_id", "product_id").agg(
            sum("view_duration").alias("view_duration"),
            count("*").alias("view_count")
        ).groupBy("user_id").agg(
            collect_list("product_id").alias("recently_viewed_products"),
            collect_list("view_duration").alias("recent_view_durations")
        )

        # Get total clicks
        total_clicks = self.browsing_df.filter(col("browsing_timestamp") >= current_timestamp() - expr("INTERVAL 30 DAYS")) \
            .groupBy("user_id") \
            .agg(count("*").alias("total_clicks"))

        # Perform left join to keep all users, and fill missing values with 0
        final_clicks_df = self.users_ids_df.join(total_clicks, "user_id", "left") \
            .fillna({"total_clicks": 0})

        # Get Top 5 Views Products and Categories
        avg_view_dur_by_product = self.browsing_df.groupBy("user_id", "product_id") \
            .agg(
            sum("view_duration").alias("view_duration"),
            count("*").alias("view_count"),
            avg("view_duration").alias("avg_view_duration"),
        ) \
            .filter((col("product_id").isNotNull()) & (col("product_id") != "nan")) \
            .withColumn("type", lit("product")) \
            .withColumn("type_id", col("product_id")) \
            .withColumn("category", lit(None))

        avg_view_dur_by_category = self.browsing_df.groupBy("user_id", "category") \
            .agg(
            sum("view_duration").alias("view_duration"),
            count("*").alias("view_count"),
            avg("view_duration").alias("avg_view_duration")
        ) \
            .filter(col("category").isNotNull()) \
            .withColumn("type", lit("category")) \
            .withColumn("type_id", col("category")) \
            .withColumn("product_id", lit(None))

        # Get most 15 products by avg view
        window_spec_for_product = Window.partitionBy("user_id").orderBy(desc("avg_view_duration"))
        avg_view_dur_by_product = avg_view_dur_by_product.withColumn("rank", row_number().over(window_spec_for_product)).filter(col("rank") <= 15).drop(col("rank"))

        # Get most 2 category by avg viewed
        window_spec_for_category = Window.partitionBy("user_id").orderBy(desc("avg_view_duration"))
        avg_view_dur_by_category = avg_view_dur_by_category.withColumn("rank", row_number().over(window_spec_for_category)).filter(col("rank") <= 2).drop(col("rank"))

        merged_df = avg_view_dur_by_product.select("user_id", "view_duration", "view_count", "avg_view_duration", "type", "type_id") \
            .unionByName(avg_view_dur_by_category.select("user_id", "view_duration", "view_count", "avg_view_duration", "type", "type_id"))

        products_df = merged_df.filter(col("type") == "product") \
            .groupBy("user_id") \
            .agg(
            collect_list("type_id").alias("products"),
            collect_list("avg_view_duration").alias("avg_product_view_duration")
        )

        # Aggregate categories (list of categories and their avg_view_duration)
        categories_df = merged_df.filter(col("type") == "category") \
            .groupBy("user_id") \
            .agg(
            collect_list("type_id").alias("categories"),
            collect_list("avg_view_duration").alias("avg_category_view_duration")
        )
        categories_df = self.users_ids_df.join(categories_df, "user_id", "left")

        # Compute overall average product view duration (across all products for a user)
        overall_product_avg_df = merged_df.filter(col("type") == "product") \
            .groupBy("user_id") \
            .agg(avg("avg_view_duration").alias("overall_avg_product_view_duration"))
        overall_product_avg_df = self.users_ids_df.join(overall_product_avg_df, "user_id", "left")

        # Compute overall average category view duration (across all categories for a user)
        overall_category_avg_df = merged_df.filter(col("type") == "category") \
            .groupBy("user_id") \
            .agg(avg("avg_view_duration").alias("overall_avg_category_view_duration"))
        overall_category_avg_df = self.users_ids_df.join(overall_category_avg_df, "user_id", "left")

        # Join all dataframes together
        final_df = products_df.join(categories_df, "user_id", "outer") \
            .join(overall_product_avg_df, "user_id", "outer") \
            .join(overall_category_avg_df, "user_id", "outer") \
            .join(last_30_days_views, "user_id", "left") \
            .withColumn("products", coalesce(col("products"), array())) \
            .withColumn("avg_product_view_duration", coalesce(col("avg_product_view_duration"), array())) \
            .withColumn("overall_avg_product_view_duration", coalesce(col("overall_avg_product_view_duration"), lit(0.0))) \
            .withColumn("categories", coalesce(col("categories"), array())) \
            .withColumn("avg_category_view_duration", coalesce(col("avg_category_view_duration"), array())) \
            .withColumn("overall_avg_category_view_duration", coalesce(col("overall_avg_category_view_duration"), lit(0.0))) \
            .withColumn("recently_viewed_products", coalesce(col("recently_viewed_products"), array())) \
            .withColumn("recent_view_durations", coalesce(col("recent_view_durations"), array())) \
            .withColumn(
            "freq_views",
            struct(
                col("products"),
                col("avg_product_view_duration"),
                col("overall_avg_product_view_duration"),
                col("categories"),
                col("avg_category_view_duration"),
                col("overall_avg_category_view_duration"),
                col("recently_viewed_products"),
                col("recent_view_durations")
            )
        ).select("user_id", "freq_views")

        browsing_df = final_df.join(final_clicks_df, "user_id", "left")

        added_count_cart = self.clickstream_df.filter(col("action") == "add_to_cart") \
            .groupBy("user_id") \
            .agg(count("*").alias("total_add_to_cart"))
        added_count_cart = added_count_cart.join(self.users_ids_df, "user_id", "right").fillna({"total_add_to_cart": 0})

        added_count_wishlist = self.clickstream_df.filter(col("action") == "add_to_wishlist") \
            .groupBy("user_id") \
            .agg(count("*").alias("total_add_to_wishlist"))
        added_count_wishlist = added_count_wishlist.join(self.users_ids_df, "user_id", "right").fillna({"total_add_to_wishlist": 0})
        added_cart_wishlist_df = added_count_cart.join(added_count_wishlist, "user_id", "left")

        browsing_df = browsing_df.join(added_cart_wishlist_df, "user_id", "left")
        browsing_df = self.users_ids_df.join(browsing_df, "user_id", "left")

        return browsing_df

    def _get_purchase_behavior(self, ):
        # Get last 30 days purchased products
        last_30_days_purchases = self.orders_df.filter(
            col("order_date") >= int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
        ).groupBy("user_id", "order_product_id").agg(
            count("*").alias("purchase_count"),
            sum("total_amount").alias("total_amount")
        ).groupBy("user_id").agg(
            collect_list("order_product_id").alias("recently_purchased_products"),
            collect_list("purchase_count").alias("recent_purchase_counts"),
            collect_list("total_amount").alias("recent_purchase_amounts")
        )
        last_30_days_purchases = self.users_ids_df.join(last_30_days_purchases, "user_id", "left")

        total_purchases = self.orders_df.groupBy("user_id").agg(count_distinct("order_id").alias("total_purchase"))
        total_purchases = self.users_ids_df.join(total_purchases, "user_id", "left").fillna({"total_purchase": 0})

        order_totals = self.orders_df.groupBy("user_id", "order_id") \
            .agg(sum("total_amount").alias("order_total"))
        avg_order_value = order_totals.groupBy("user_id") \
            .agg(round(avg("order_total"), 2).alias("avg_order_value"))
        avg_order_value = self.users_ids_df.join(avg_order_value, "user_id", "left").fillna({"avg_order_value": 0})

        total_spent = self.orders_df.filter(col("order_date") >= int((datetime.now() - timedelta(days=30)).timestamp() * 1000)) \
            .groupBy("user_id").agg(sum("total_amount").alias("total_spent"))
        total_spent = self.users_ids_df.join(total_spent, "user_id", "left").fillna({"total_spent": 0})

        temp_orders_df = self.orders_df \
            .withColumn("year_month", date_format(col("order_timestamp"), "yyyy-MM")) \
            .withColumn("month", month("order_timestamp")) \
            .withColumn(
            "season",
            when(col("month").isin(12, 1, 2), "winter")
            .when(col("month").isin(3, 4, 5), "spring")
            .when(col("month").isin(6, 7, 8), "summer")
            .when(col("month").isin(9, 10, 11), "fall"))

        # Count total orders per user per month
        monthly_orders = temp_orders_df.groupBy("user_id", "year_month") \
            .agg(count("*").alias("orders_per_month"))

        avg_order_freq_monthly = monthly_orders.groupBy("user_id") \
            .agg(round(avg("orders_per_month"), 2).alias("avg_orders_per_month"))
        avg_order_freq_monthly = self.users_ids_df.join(avg_order_freq_monthly, "user_id", "left").fillna({"avg_orders_per_month": 0.0})

        # Calculcate Seasonal data
        seasonal_orders = temp_orders_df.groupBy("user_id", "season") \
            .agg(count("*").alias("orders_per_season"))

        # Calculate the average order frequency per season per user
        avg_order_freq_seasonal = seasonal_orders.groupBy("user_id") \
            .pivot("season", ["winter", "spring", "summer", "fall"]) \
            .agg(avg("orders_per_season").alias("avg_orders_per_season"))
        avg_order_freq_seasonal = avg_order_freq_seasonal.fillna(0)
        print(avg_order_freq_seasonal.count())

        avg_order_freq_seasonal = avg_order_freq_seasonal.withColumn(
            "seasonal_data",
            struct(
                when(col("winter").isNotNull(), col("winter")).otherwise(lit(0.0)).alias("winter"),
                when(col("spring").isNotNull(), col("spring")).otherwise(lit(0.0)).alias("spring"),
                when(col("summer").isNotNull(), col("summer")).otherwise(lit(0.0)).alias("summer"),
                when(col("fall").isNotNull(), col("fall")).otherwise(lit(0.0)).alias("fall")
            )
        ).select("user_id", "seasonal_data")
        print(avg_order_freq_seasonal.count())

        avg_order_freq_seasonal = self.users_ids_df.join(avg_order_freq_seasonal, "user_id", "left")
        default_seasonal = struct(
            lit(0.0).alias("winter"),
            lit(0.0).alias("spring"),
            lit(0.0).alias("summer"),
            lit(0.0).alias("fall")
        )
        avg_order_freq_seasonal = avg_order_freq_seasonal.withColumn(
            "seasonal_data",
            coalesce(col("seasonal_data"), default_seasonal)
        )

        avg_order_freq_seasonal.groupBy("user_id").count().filter("count > 1").show()
        self.users_ids_df.groupBy("user_id").count().filter("count > 1").show()


        print(avg_order_value.count())
        print(total_spent.count())
        print(avg_order_freq_monthly.count())
        print(avg_order_freq_seasonal.count())
        print(last_30_days_purchases.count())

        final_df = total_purchases \
            .join(avg_order_value, "user_id", "right") \
            .join(total_spent, "user_id", "right") \
            .join(avg_order_freq_monthly, "user_id", "right") \
            .join(avg_order_freq_seasonal, "user_id", "right") \
            .join(last_30_days_purchases, "user_id", "right") \
            .withColumn("recently_purchased_products", coalesce(col("recently_purchased_products"), array())) \
            .withColumn("recent_purchase_counts", coalesce(col("recent_purchase_counts"), array())) \
            .withColumn("recent_purchase_amounts", coalesce(col("recent_purchase_amounts"), array()))

        return final_df

    def _get_product_preferences(self, ):
        # Get Review Count
        review_count_per_user = self.reviews_df.filter(col("review_text").isNotNull()) \
            .groupBy("user_id") \
            .agg(count("*").alias("review_count"))
        review_count_per_user = self.users_ids_df.join(review_count_per_user, "user_id", "left").fillna({"review_count": 0})

        # Get average rating
        avg_rating_per_user = self.reviews_df.groupBy("user_id").agg(round(avg("rating"), 2).alias("avg_review_rating"))
        avg_rating_per_user = self.users_ids_df.join(avg_rating_per_user, "user_id", "left").fillna({"avg_review_rating": 0.0})

        # Get average rating per category
        product_and_reviews_df = self.reviews_df \
            .join(self.products_df, self.products_df.product_id == self.reviews_df.review_product_id, "inner") \
            .select("user_id", "product_id", "category", "brand", self.reviews_df.rating)
        product_and_reviews_df = self.users_ids_df.join(product_and_reviews_df, "user_id", "left").fillna({"rating": 0.0})

        avg_rating_per_category = product_and_reviews_df.groupBy("user_id", "category") \
            .agg(round(avg("rating"), 2).alias("avg_review_rating_per_category"))
        avg_rating_per_category = avg_rating_per_category.filter(col("category").isNotNull())

        avg_rating_per_category = avg_rating_per_category.groupBy("user_id") \
            .agg(collect_list(struct(col("category"), col("avg_review_rating_per_category"))).alias("category_ratings"))

        avg_rating_per_category = avg_rating_per_category.withColumn(
            "avg_rating_per_category", map_from_entries(col("category_ratings"))
        ).drop("category_ratings")

        avg_rating_per_category = self.users_ids_df.join(avg_rating_per_category, "user_id", "left")
        avg_rating_per_category = avg_rating_per_category.withColumn(
            "avg_rating_per_category",
            coalesce(col("avg_rating_per_category"), create_map())
        )

        # Get most purchased brand
        most_purchased_brands = product_and_reviews_df.filter(col("brand").isNotNull()) \
            .groupBy("user_id", "brand") \
            .agg(count("*").alias("purchase_count"))
        most_purchased_brands = self.users_ids_df.join(most_purchased_brands, "user_id", "left") \
            .fillna({"purchase_count": 0})
        window_spec = Window.partitionBy("user_id").orderBy(col("purchase_count").desc())
        top_5_brands_per_user = most_purchased_brands.withColumn("rank", dense_rank().over(window_spec)) \
            .filter(col("rank") <= 5).drop("rank")
        top_5_brands_per_user = top_5_brands_per_user.groupBy("user_id") \
            .agg(collect_list("brand").alias("most_purchased_brands"))

        final_df = review_count_per_user \
            .join(avg_rating_per_user, "user_id", "right") \
            .join(avg_rating_per_category, "user_id", "right") \
            .join(top_5_brands_per_user, "user_id", "right")

        final_df = final_df.withColumn(
            "most_purchased_brands",
            coalesce(col("most_purchased_brands"), array())
        )
        final_df = final_df.fillna(
            0, subset=["review_count", "avg_review_rating"]
        )
        return final_df

    def _get_ctr(self, ):
        ctr_df = self.clickstream_df.groupBy("user_id").agg(
            sum("is_impression").alias("total_impressions"),
            sum("is_click").alias("total_clicks")
        )

        ctr_df = ctr_df.withColumn(
            "ctr",
            when(col("total_impressions") > 0, (col("total_clicks") / col("total_impressions")) * 100)
            .otherwise(0)
        )

        ctr_df = self.users_ids_df.join(ctr_df, "user_id", "left")
        ctr_df = ctr_df.withColumn("ctr", coalesce(col("ctr"), lit(0.0)))

        return ctr_df.select("user_id", "ctr")

    def define_user_features(self, ):
        user_profile = self._get_user_profile()
        browsing_behavior = self._get_browsing_behavior()
        purchase_behavior = self._get_purchase_behavior()
        product_preferences = self._get_product_preferences()
        ctr_df = self._get_ctr()

        print(user_profile.count())
        print(browsing_behavior.count())
        print(purchase_behavior.count())
        print(product_preferences.count())
        print(ctr_df.count())

        transformed_df = transform_data(user_profile, browsing_behavior, purchase_behavior, product_preferences, ctr_df)
        distributed_df = distribution_of_df_by_range(transformed_df, 1)
        print(distributed_df.count())

        return distributed_df

    def vectorize(self, agg_df: DataFrame) -> DataFrame | None:
        vectorize_udf = udf(vectorize, ArrayType(FloatType()))

        if agg_df is None:
            return None

        vectorized_df = agg_df.withColumn(
            "values",
            vectorize_udf(
                col("ctr").cast("float"), col("user_profile"), col("browsing_behavior"), col("purchase_behavior"), col("product_preferences")
            )
        )

        return vectorized_df.withColumnRenamed("user_id", "id").select("id", "values")

    def start_vectorization(self, agg_df):
        vectorized_agg_df = self.vectorize(agg_df)  # Vectorize df
        UserModel.store_vectors(vectorized_agg_df)

    def start(self):
        distributed_df = self.define_user_features().cache()
        print(distributed_df.count())

        # Store data in Mongo and HDFS for further usages
        UserModel.store_agg_data_to_mongodb(distributed_df.drop("user_num", "user_range"))
        #UserModel.store_df_to_hdfs(distributed_df, "user_range")  # No need to write to hdfs if in same job

        self.start_vectorization(distributed_df)

        # TODO: find recc item from similar users
        # TODO: store them in mongodb
        # TODO: for start the recommendation
        self.stop()

    def stop(self):
        print("IT'S DONE")

start = datetime.now()
ubs = UserBatchService()
ubs.start()
print(datetime.now() - start)
print("Bitti")

