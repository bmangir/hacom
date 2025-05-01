import json
from datetime import datetime, timedelta

from pymongo import UpdateOne
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, MapType, Row

from config import MONGO_PRODUCTS_DB, client, ITEM_FEATURES_HOST, ITEM_CONTENTS_HOST
from Batch.models import item_model as ItemModel
from utility import analyze_sentiment, _create_product_contents, vectorize_item_features, vectorize_item_contents, \
    distribution_of_df_by_range, _extract_details_column
from utilities.spark_utility import create_spark_session


class ItemBatchService:

    def __init__(self):
        self.item_feature_ext_sj = create_spark_session(app_name="item-features-extraction-job", num_of_partition="20")

        (self.users_df,
         self.orders_df,
         self.reviews_df,
         self.cart_df,
         self.wishlist_df,
         self.browsing_df,
         self.clickstream_df,
         self.session_df,
         self.products_df,
         _, self.product_ids_df) = ItemModel.fetch_data(spark=self.item_feature_ext_sj)

        self.preprocess_data()

    def preprocess_data(self, range=10):
        sentiment_udf = udf(analyze_sentiment, FloatType())
        self.reviews_df = self.reviews_df.withColumn("sentiment_score", sentiment_udf(col("review_text")))
        self.orders_df = self.orders_df.withColumn("order_date_as_day",
                                                   date_format(from_unixtime(col("order_date") / 1000), "yyyy-MM-dd")) \
                                        .withColumn("price", coalesce(col("order_quantity") * col("unit_price"), lit(0.0)))

    def update_avg_ratings(self):

        rating_df = self.reviews_df.groupBy("review_product_id").agg(avg("rating").alias("rating")).select(
            "review_product_id", "rating")
        rating_updates = rating_df.withColumnRenamed("review_product_id", "product_id")

        def update_partition(iterator):
            db = client[MONGO_PRODUCTS_DB]
            collection = db["products"]

            bulk_operations = [
                UpdateOne({"product_id": row.product_id}, {"$set": {"rating": row.rating}})
                for row in iterator
            ]

            if bulk_operations:
                collection.bulk_write(bulk_operations)

        rating_updates.foreachPartition(update_partition)

    def _get_popularity_metrics(self):
        """ Get metrics last 30 days """

        # Purchase count
        last_30_days_purchases_df = self.orders_df.filter(
            col("order_date") >= int((datetime.now() - timedelta(days=30)).timestamp() * 1000))
        purchase_counts = last_30_days_purchases_df.groupBy("order_product_id") \
            .agg(count("*").alias("purchase_count")) \
            .withColumnRenamed("order_product_id", "product_id")
        purchase_counts = purchase_counts.fillna({"purchase_count": 0})

        # Avg Rating, Review Count
        # last_30_days_reviews_dy = self.reviews_df.filter(col("review_date") >= date_sub(current_date(), 30))
        avg_rating_and_review_count = self.reviews_df.groupBy("review_product_id") \
            .agg(avg("rating").alias("avg_rating"),
                 count("*").alias("review_count")) \
            .withColumnRenamed("review_product_id", "product_id")
        avg_rating_and_review_count = avg_rating_and_review_count.fillna({"avg_rating": 0, "review_count": 0})

        final_df = purchase_counts.join(avg_rating_and_review_count, "product_id", "left")
        return final_df

    def _get_basic_item_information(self):
        """Product Identification"""

        basic_item_information = self.products_df.select(
            "product_id", "product_name", "category", "brand", "price",
            "stock_quantity", "color", "material", "gender", "size",
            "tags", "details", "format", "language"
        )
        basic_item_information = basic_item_information.withColumn("price_bucket",
                                                                   when(col("price") <= 200, "Low")
                                                                   .when((col("price") > 200) & (col("price") <= 1000), "Medium")
                                                                   .otherwise("High")) \
            .withColumn("stock_status",
                        when(col("stock_quantity") > 0, "in-stock").otherwise("out-stock"))

        def build_metadata(
                product_name, category, brand, price_bucket, stock_status,
                color, material, gender, size, tags, details,
                format, language
        ):
            print(f"Details Değeri: {details}, Tipi: {type(details)}")
            metadata = {}

            for key, val in {
                "product_name": product_name,
                "category": category,
                "brand": brand,
                "price_bucket": price_bucket,
                "stock_status": stock_status,
                "material": material,
                "gender": gender,
                "format": format,
                "language": language
            }.items():
                if val:
                    metadata[key] = str(val).lower()

            for key, val in {
                "color": color,
                "size": size,
                "tags": tags
            }.items():
                if val:
                    if isinstance(val, list):
                        metadata[key] = [str(v).lower() for v in val if v]
                    else:
                        metadata[key] = [str(val).lower()]

            if details:
                if isinstance(details, Row):
                    for field in details.__fields__:
                        value = details[field]
                        if value:
                            metadata[field.lower()] = str(value).lower() if isinstance(value, str) else value
                elif isinstance(details, str): # JSON string kontrolü (opsiyonel, struct ise gerekmeyebilir)
                    try:
                        details_dict = json.loads(details)
                        if isinstance(details_dict, dict):
                            for k, v in details_dict.items():
                                if v:
                                    metadata[k.lower()] = str(v).lower() if isinstance(v, str) else v
                    except json.JSONDecodeError:
                        print(f"Uyarı: 'details' sütunu geçerli bir JSON stringi değil: {details}")

            return metadata

        metadata_udf = udf(build_metadata, MapType(StringType(), StringType()))

        # metadata kolonu ekle
        basic_item_information = basic_item_information.withColumn(
            "metadata",
            metadata_udf(
                col("product_name"),
                col("category"),
                col("brand"),
                col("price_bucket"),
                col("stock_status"),
                col("color"),
                col("material"),
                col("gender"),
                col("size"),
                col("tags"),
                col("details"),
                col("format"),
                col("language")
            )
        )

        a = basic_item_information.limit(1).toJSON().collect()
        b = [json.loads(row) for row in a]
        print(b[0])
        return basic_item_information.select("product_id", "product_name", "category", "brand", "price",
                                             "stock_quantity", "price_bucket", "stock_status", "metadata")

    def _get_content_based_attributes(self):
        """Textual and Visual Representations"""

        contents_of_products = self.products_df.drop("stock_quantity", "rating", "image_url", "author")
        detailed_products_df = _extract_details_column(contents_of_products.select("details", "product_id")).select(
            "details", "product_id")
        contents_of_products = contents_of_products \
            .withColumn("colors",
                        concat_ws(",", when(col("color").isNull(), lit([]))
                                  .otherwise(array_sort(col("color"))))) \
            .withColumn("tag",
                        concat_ws(",", when(col("tags").isNull(), lit([]))
                                  .otherwise(array_sort(col("tags"))))) \
            .withColumn("sizes",
                        concat_ws(",", when(col("size").isNull(), lit([]))
                                  .otherwise(array_sort(col("size")))))

        df_flattened = contents_of_products.join(detailed_products_df, "product_id", "left").drop(
            contents_of_products.details)
        df_flattened = df_flattened.select("product_id", "gender", "material", "colors", "tag", "sizes", "details",
                                           "language", "format")

        return df_flattened

    def _get_sales_and_popularity_features(self):
        """Engagement & Demand Metrics"""

        # Purchase count
        last_30_days_purchases_df = self.orders_df.filter(
            col("order_date") >= int((datetime.now() - timedelta(days=30)).timestamp() * 1000))
        purchase_counts = self.orders_df.groupBy("order_product_id") \
            .agg(count("*").alias("purchase_count")) \
            .withColumnRenamed("order_product_id", "product_id")

        # View Count
        view_counts = self.browsing_df \
            .groupBy("product_id") \
            .agg(count("*").alias("view_count"))

        # Wishlist Count
        wishlist_counts = self.wishlist_df \
            .filter(col("wishlist_action_type") == "added") \
            .groupBy("wishlist_product_id") \
            .agg(count("*").alias("wishlist_count")) \
            .withColumnRenamed("wishlist_product_id", "product_id")

        # Cart Count - Number of times this product was added to the cart
        cart_counts = self.cart_df \
            .filter(col("cart_action_type") == "added") \
            .groupBy("cart_product_id").agg(count("*").alias("cart_addition_count")) \
            .withColumnRenamed("cart_product_id", "product_id")

        engagement_df = purchase_counts \
            .join(view_counts, "product_id", "left") \
            .join(wishlist_counts, "product_id", "left") \
            .join(cart_counts, "product_id", "left")

        engagement_df = self.product_ids_df.join(engagement_df, "product_id", "left")

        engagement_df = engagement_df \
            .withColumn("purchase_count", coalesce(col("purchase_count"), lit(0))) \
            .withColumn("view_count", coalesce(col("view_count"), lit(0))) \
            .withColumn("cart_addition_count", coalesce(col("cart_addition_count"), lit(0))) \
            .withColumn("wishlist_count", coalesce(col("wishlist_count"), lit(0))) \
            .withColumn("conversion_rate", col("purchase_count") / col("view_count") * 100) \
            .withColumn("abandonment_rate",
                        coalesce((col("cart_addition_count") - col("purchase_count")) / col("cart_addition_count"),
                                 lit(0.0)))

        return engagement_df

    def _get_user_engagements_and_ratings(self):
        """Social Proof & Quality Perception"""

        avg_rating_and_review_count = self.reviews_df.groupBy("review_product_id") \
            .agg(avg("rating").alias("avg_rating"),
                 count("rating").alias("rating_count"),
                 count(when(col("review_text").isNotNull(), True)).alias("review_count"),
                 avg(when(col("review_text").isNotNull(), length(col("review_text")))).alias("review_length_avg"),
                 avg("sentiment_score").alias("avg_sentiment_score")) \
            .withColumnRenamed("review_product_id", "product_id")

        avg_rating_and_review_count = self.product_ids_df.join(avg_rating_and_review_count, "product_id", "left")

        avg_rating_and_review_count = avg_rating_and_review_count \
            .withColumn("rating_count", coalesce(col("rating_count"), lit(0))) \
            .withColumn("review_count", coalesce(col("review_count"), lit(0))) \
            .withColumn("review_length_avg", coalesce(col("review_length_avg"), lit(0.0))) \
            .withColumn("avg_sentiment_score", coalesce(col("avg_sentiment_score"), lit(0.0)))

        return avg_rating_and_review_count

    def _get_category_based_products(self, top_n=20):
        products_browsing_df = self.products_df.join(self.browsing_df, "product_id", "left").drop(
            self.browsing_df.category)
        products_browsing_df = products_browsing_df.select("product_id", "category", "view_duration")
        products_avg_view_duration_df = products_browsing_df.groupBy("product_id", "category").agg(
            avg("view_duration").alias("avg_view_duration")
        )

        window_spec = Window.partitionBy("category").orderBy(col("avg_view_duration").desc())

        top_n_products_df = products_avg_view_duration_df.withColumn(
            "rank", row_number().over(window_spec)
        ).filter(col("rank") <= top_n)

        top_n_products_by_category_df = top_n_products_df.groupBy("category").agg(
            collect_list("product_id").alias("top_20_product_ids")
        )

        return top_n_products_by_category_df

    def _get_user_item_interaction_features(self):
        """Collaborative Filtering Factors"""

        # Frequently bought together
        co_purchased = self.orders_df.alias("a").join(
            self.orders_df.alias("b"),
            on="order_id",
            how="inner"
        ).filter(col("a.order_product_id") != col("b.order_product_id"))

        co_purchased_count = co_purchased.groupBy("a.order_product_id", "b.order_product_id") \
            .count() \
            .withColumnRenamed("count", "co_purchase_count")

        frequently_bought_together = co_purchased_count.groupBy("a.order_product_id") \
            .agg(collect_list(struct("b.order_product_id", "co_purchase_count")).alias("frequently_bought_together"))
        frequently_bought_together = frequently_bought_together.withColumnRenamed("a.order_product_id", "product_id")

        frequently_bought_together = frequently_bought_together.select("order_product_id",
                                                                       col("frequently_bought_together.order_product_id").alias(
                                                                           "bought_together"))
        frequently_bought_together = frequently_bought_together \
            .withColumnRenamed("order_product_id", "product_id")
        frequently_bought_together = self.product_ids_df.join(frequently_bought_together, "product_id", "left")

        final_df = frequently_bought_together.withColumn("bought_together", coalesce(col("bought_together"), lit([])))

        return final_df

    def _get_temporal_dynamics(self, daily_sales_df: DataFrame):
        seven_day_window_spec = Window.partitionBy("product_id").orderBy("order_date").rowsBetween(-6,
                                                                                                   0)  # 7-day window
        thirty_day_window_spec = Window.partitionBy("product_id").orderBy("order_date").rowsBetween(-29,
                                                                                                    0)  # 30-day window

        daily_sales_trend_df = daily_sales_df \
            .withColumn("7_day_trend", sum("daily_units_sold").over(seven_day_window_spec)) \
            .withColumn("30_day_trend", sum("daily_units_sold").over(thirty_day_window_spec))

        latest_window = Window.partitionBy("product_id").orderBy(col("order_date").desc())

        daily_sales_trend_df = daily_sales_trend_df.withColumn("row_num", row_number().over(latest_window)) \
            .filter(col("row_num") == 1).drop("row_num", "order_date")

        daily_sales_trend_df = self.product_ids_df.join(daily_sales_trend_df, "product_id", "left")

        daily_sales_trend_df = daily_sales_trend_df.withColumn(
            "historical_sales_summary",
            struct(
                col("daily_units_sold"),
                col("7_day_trend"),
                col("30_day_trend")
            )
        ).select("product_id", "historical_sales_summary")

        return daily_sales_trend_df

    def _get_seasonal_pop(self):
        seasonal_df = self.orders_df.withColumn("season",
                                                when((month(col("order_date_as_day")) >= 12) | (
                                                            month(col("order_date_as_day")) <= 2), "Winter")
                                                .when((month(col("order_date_as_day")) >= 3) & (
                                                            month(col("order_date_as_day")) <= 5), "Spring")
                                                .when((month(col("order_date_as_day")) >= 6) & (
                                                            month(col("order_date_as_day")) <= 8), "Summer")
                                                .otherwise("Fall")
                                                )

        seasons_df = self.item_feature_ext_sj.createDataFrame([
            ("Winter",),
            ("Spring",),
            ("Summer",),
            ("Fall",)
        ], ["season"])

        products_df = seasonal_df.select("order_product_id").distinct()
        full_product_seasons_df = products_df.crossJoin(seasons_df)
        seasonal_sales_df = full_product_seasons_df \
            .join(seasonal_df.groupBy("order_product_id", "season").agg(count("*").alias("total_units_sold")),
                  on=["order_product_id", "season"], how="left")
        seasonal_sales_df = seasonal_sales_df.withColumn("total_units_sold", coalesce(col("total_units_sold"), lit(0)))
        total_sales = seasonal_df.agg(count("*")).first()[0]
        seasonal_sales_df = seasonal_sales_df \
            .withColumn("percentage_of_total", (col("total_units_sold") / total_sales) * 100) \
            .withColumnRenamed("order_product_id", "product_id")

        default_seasonal_sales = create_map(
            lit("Winter"), struct(lit(0).alias("total_sold_unit"), lit(0.0).alias("percentage_of_total")),
            lit("Spring"), struct(lit(0).alias("total_sold_unit"), lit(0.0).alias("percentage_of_total")),
            lit("Summer"), struct(lit(0).alias("total_sold_unit"), lit(0.0).alias("percentage_of_total")),
            lit("Fall"), struct(lit(0).alias("total_sold_unit"), lit(0.0).alias("percentage_of_total"))
        )

        seasonal_sales_df = (
            seasonal_sales_df
            .groupBy("product_id")
            .agg(
                coalesce(
                    map_from_entries(
                        collect_list(
                            struct(
                                col("season"),
                                struct(
                                    col("total_units_sold").alias("total_sold_unit"),
                                    col("percentage_of_total")
                                )
                            )
                        )
                    ),
                    default_seasonal_sales
                ).alias("seasonal_sales")
            )
        )

        return seasonal_sales_df

    def _get_trending_scores(self):
        weight_views = 1.0
        weight_purchases = 2.0
        weight_searches = 0.5

        filtered_df = self.clickstream_df.filter(
            (col("referrer").contains("/search")) &
            ((col("page_url").startswith("/product/")) | (col("page_url").startswith("/products/details/")))
        )
        product_id_regex = r"/(?:product|products/details)/(\w+)"
        filtered_df = filtered_df.withColumn("product_id", regexp_extract(col("page_url"), product_id_regex, 1))

        searches_agg = filtered_df.groupBy("product_id").count().orderBy(col("count").desc())
        searches_agg = self.product_ids_df.join(searches_agg, "product_id", "left").withColumn("count",
                                                                                               coalesce(col("count"),
                                                                                                        lit(0)))
        searches_agg = searches_agg.withColumnRenamed("count", "total_searches")

        purchases_agg = self.orders_df.groupBy("order_product_id").agg(count("*").alias("total_purchases"))
        purchases_agg = purchases_agg.withColumn("total_purchases",
                                                 coalesce(col("total_purchases"), lit(0))).withColumnRenamed(
            "order_product_id", "product_id")

        views_agg = self.browsing_df \
            .groupBy("product_id") \
            .agg(count("*").alias("total_views"))
        views_agg = self.product_ids_df.join(views_agg, "product_id", "left").withColumn("total_views",
                                                                                         coalesce(col("total_views"),
                                                                                                  lit(0)))

        trending_df = views_agg.join(purchases_agg, on="product_id", how="outer") \
            .join(searches_agg, on="product_id", how="outer")

        trending_df = trending_df.withColumn("trending_score", coalesce(
            (col("total_views") * weight_views) +
            (col("total_purchases") * weight_purchases) +
            (col("total_searches") * weight_searches), lit(0)))

        return trending_df

    def _get_latest_sales_summary(self, daily_sales_df: DataFrame):
        # Get latest bases from current dat
        today = current_date()
        last_1_day = date_sub(today, 1)
        last_7_days = date_sub(today, 7)
        last_30_days = date_sub(today, 30)

        # Last 1 day
        last_1_day_sales_df = daily_sales_df.filter(col("order_date") >= last_1_day)
        last_1_day_sales = last_1_day_sales_df.groupBy("product_id").agg(
            sum("daily_units_sold").alias("last_1_day_sales"))

        # Last 7 days
        last_7_days_sales_df = daily_sales_df.filter(col("order_date") >= last_7_days)
        last_7_days_sales = last_7_days_sales_df.groupBy("product_id").agg(
            sum("daily_units_sold").alias("last_7_days_sales"))

        # Last 30 days
        last_30_days_sales_df = daily_sales_df.filter(col("order_date") >= last_30_days)
        last_30_days_sales = last_30_days_sales_df.groupBy("product_id").agg(
            sum("daily_units_sold").alias("last_30_days_sales"))

        final_sales_df = last_1_day_sales \
            .join(last_7_days_sales, "product_id", "outer") \
            .join(last_30_days_sales, "product_id", "outer")

        final_sales_df = self.product_ids_df.join(final_sales_df, "product_id", "left") \
            .withColumn("last_7_days_sales", coalesce(col("last_7_days_sales"), lit(0))) \
            .withColumn("last_30_days_sales", coalesce(col("last_30_days_sales"), lit(0))) \
            .withColumn("last_1_day_sales", coalesce(col("last_1_day_sales"), lit(0)))

        final_sales_df = final_sales_df.withColumn(
            "latest_sales_summary",
            struct(
                col("last_1_day_sales"),
                col("last_7_days_sales"),
                col("last_30_days_sales")
            )
        ).select("product_id", "latest_sales_summary")

        return final_sales_df

    def _get_seasonal_and_trend_based_features(self):
        daily_sales_df = self.orders_df.groupBy("order_product_id", "order_date_as_day") \
            .agg(count("*").alias("daily_units_sold")) \
            .withColumnRenamed("order_date_as_day", "order_date") \
            .withColumn("order_date", col("order_date").cast("date")) \
            .withColumnRenamed("order_product_id", "product_id")

        temporal_dynamics_df = self._get_temporal_dynamics(daily_sales_df)
        latest_sales_summary_df = self._get_latest_sales_summary(daily_sales_df)
        seasonal_sales_pop_df = self._get_seasonal_pop()
        trending_scores_df = self._get_trending_scores()

        final_df = seasonal_sales_pop_df \
            .join(trending_scores_df, "product_id", "left") \
            .join(temporal_dynamics_df, "product_id", "left") \
            .join(latest_sales_summary_df, "product_id", "left")

        return final_df

    def define_item_contents(self, basic_item_info_df, content_df):
        item_content_df = basic_item_info_df.join(content_df, "product_id", "left")

        item_content_df = _create_product_contents(item_content_df)

        return item_content_df.select("product_id", "content", "metadata")

    def _get_price_elasticity(self):
        """Calculate price elasticity based on price changes and sales volume"""
        price_changes = self.orders_df.groupBy("order_product_id") \
            .agg(
                stddev("price").alias("price_std"),
                avg("price").alias("avg_price"),
                count("*").alias("total_orders")
            )
        
        sales_volume = self.orders_df.groupBy("order_product_id") \
            .agg(count("*").alias("total_sales"))
        
        elasticity = price_changes.join(sales_volume, "order_product_id") \
            .withColumn("price_elasticity", 
                when(col("price_std") > 0, 
                    (col("total_sales") / col("total_orders")) / (col("price_std") / col("avg_price"))
                ).otherwise(0)
            )

        elasticity = self.product_ids_df.join(elasticity, self.product_ids_df.product_id == elasticity.order_product_id, "left") \
            .withColumn("price_elasticity", coalesce(col("price_elasticity"), lit(0.0)))
        
        return elasticity.select("product_id", "price_elasticity")

    def _get_competitor_analysis(self):
        """Analyze product's position relative to competitors"""
        category_avg_price = self.products_df.groupBy("category") \
            .agg(avg("price").alias("category_avg_price"))
        
        category_avg_rating = self.reviews_df.groupBy("review_product_id") \
            .agg(avg("rating").alias("avg_rating")) \
            .join(self.products_df.select("product_id", "category"), 
                  col("review_product_id") == col("product_id")) \
            .groupBy("category") \
            .agg(avg("avg_rating").alias("category_avg_rating"))
        
        competitor_analysis = self.products_df \
            .join(category_avg_price, "category") \
            .join(category_avg_rating, "category") \
            .withColumn("price_competitiveness", 
                when(col("price") <= col("category_avg_price"), 1.0)
                .otherwise(0.5)
            ) \
            .withColumn("rating_competitiveness",
                when(col("rating") >= col("category_avg_rating"), 1.0)
                .otherwise(0.5)
            ) \
            .withColumn("competitor_analysis",
                (col("price_competitiveness") + col("rating_competitiveness")) / 2
            )

        competitor_analysis = self.product_ids_df.join(competitor_analysis, "product_id", "left") \
            .withColumn("competitor_analysis", coalesce(col("competitor_analysis"), lit(0.0)))
        
        return competitor_analysis.select("product_id", "competitor_analysis")

    def _get_content_quality_score(self):
        """Calculate content quality score based on various content metrics"""
        review_quality = self.reviews_df \
            .withColumn("review_length", length(col("review_text"))) \
            .groupBy("review_product_id") \
            .agg(
                avg("review_length").alias("avg_review_length"),
                count("*").alias("review_count")
            )
        
        content_quality = self.products_df \
            .join(review_quality, col("product_id") == col("review_product_id"), "left") \
            .withColumn("content_quality_score",
                (coalesce(col("avg_review_length"), lit(0)) / 100) * 0.4 +
                (coalesce(col("review_count"), lit(0)) / 100) * 0.3
            )

        content_quality = self.product_ids_df.join(content_quality, "product_id", "left") \
            .withColumn("content_quality_score", coalesce(col("content_quality_score"), lit(0.0)))

        return content_quality.select("product_id", "content_quality_score")

    def _get_inventory_turnover(self):
        """Calculate inventory turnover rate"""
        sales_period = 30  # days
        sales_volume = self.orders_df \
            .filter(col("order_timestamp") >= current_timestamp() - expr(f"INTERVAL {sales_period} DAYS")) \
            .groupBy("order_product_id") \
            .agg(count("*").alias("sales_volume"))
        
        inventory_turnover = self.products_df \
            .join(sales_volume, col("product_id") == col("order_product_id"), "left") \
            .withColumn("inventory_turnover",
                when(col("stock_quantity") > 0,
                    coalesce(col("sales_volume"), lit(0)) / col("stock_quantity")
                ).otherwise(0)
            )

        inventory_turnover = self.product_ids_df.join(inventory_turnover, "product_id", "left") \
            .withColumn("inventory_turnover", coalesce(col("inventory_turnover"), lit(0.0)))

        return inventory_turnover.select("product_id", "inventory_turnover")

    def _get_return_rate(self):
        """Calculate product return rate"""
        total_orders = self.orders_df.groupBy("order_product_id") \
            .agg(count("*").alias("total_orders"))
        
        returns = self.orders_df.filter(col("status") == "returned") \
            .groupBy("order_product_id") \
            .agg(count("*").alias("return_count"))
        
        return_rate = total_orders.join(returns, "order_product_id", "left") \
            .withColumn("return_rate",
                when(col("total_orders") > 0,
                    coalesce(col("return_count"), lit(0)) / col("total_orders")
                ).otherwise(0)
            )

        return_rate = self.product_ids_df.join(return_rate, self.product_ids_df.product_id == return_rate.order_product_id, "left") \
            .withColumn("return_rate", coalesce(col("return_rate"), lit(0.0)))
        
        return return_rate.select("product_id", "return_rate")

    def _get_customer_satisfaction_score(self):
        """Calculate customer satisfaction score based on reviews and ratings"""
        review_sentiment = self.reviews_df \
            .withColumn("sentiment_score", 
                when(col("rating") >= 4, 1.0)
                .when(col("rating") >= 3, 0.7)
                .when(col("rating") >= 2, 0.4)
                .otherwise(0.1)
            ) \
            .groupBy("review_product_id") \
            .agg(avg("sentiment_score").alias("customer_satisfaction_score"))

        review_sentiment = self.product_ids_df.join(review_sentiment, self.product_ids_df.product_id == review_sentiment.review_product_id, "left") \
            .withColumn("customer_satisfaction_score", coalesce(col("customer_satisfaction_score"), lit(0.0)))
        
        return review_sentiment.select("product_id", "customer_satisfaction_score")

    def define_item_profile(self):
        basic_item_information = self._get_basic_item_information()
        content_df = self._get_content_based_attributes()
        sales_and_pop_metrics_df = self._get_sales_and_popularity_features()
        user_eng_n_ratings_df = self._get_user_engagements_and_ratings()
        user_item_interaction_df = self._get_user_item_interaction_features()
        top_n_products_by_cat = self._get_category_based_products(top_n=10)
        seasonal_and_trend_based_df = self._get_seasonal_and_trend_based_features()
        
        # New features
        price_elasticity_df = self._get_price_elasticity()
        competitor_analysis_df = self._get_competitor_analysis()
        content_quality_score_df = self._get_content_quality_score()
        inventory_turnover_df = self._get_inventory_turnover()
        return_rate_df = self._get_return_rate()
        customer_satisfaction_score_df = self._get_customer_satisfaction_score()

        basic_item_information.cache()
        content_df.cache()

        item_content_df = self.define_item_contents(basic_item_information, content_df)
        item_content_df = item_content_df.withColumn("agg_time", (unix_timestamp(current_timestamp()) * 1000))
        distributed_content_df = distribution_of_df_by_range(item_content_df, range=1)

        top_n_products_by_cat = top_n_products_by_cat.withColumn("agg_time", (unix_timestamp(current_timestamp()) * 1000))

        transformed_features_df = item_content_df \
            .join(sales_and_pop_metrics_df, "product_id", "left") \
            .join(user_eng_n_ratings_df, "product_id", "left") \
            .join(user_item_interaction_df, "product_id", "left") \
            .join(seasonal_and_trend_based_df, "product_id", "left") \
            .join(price_elasticity_df, "product_id", "left") \
            .join(competitor_analysis_df, "product_id", "left") \
            .join(content_quality_score_df, "product_id", "left") \
            .join(inventory_turnover_df, "product_id", "left") \
            .join(return_rate_df, "product_id", "left") \
            .join(customer_satisfaction_score_df, "product_id", "left") \
            .withColumn("agg_time", (unix_timestamp(current_timestamp()) * 1000))

        distributed_features_df = distribution_of_df_by_range(transformed_features_df, range=1)

        return distributed_content_df, top_n_products_by_cat, distributed_features_df

    def start_vectorization(self, agg_features_df, agg_contents_df):
        # Calculate vectors of contents and features
        feature_vector_df = self.vectorize_features(agg_features_df)
        content_vector_df = self.vectorize_contents(agg_contents_df)

        # Store the vectors in Pinecone, vector database
        start = datetime.now()
        ItemModel.store_vectors(feature_vector_df, index_host=ITEM_FEATURES_HOST)
        ItemModel.store_vectors(content_vector_df, index_host=ITEM_CONTENTS_HOST)
        end = datetime.now()
        print(end - end)

    def vectorize_contents(self, agg_contents_df: DataFrame) -> DataFrame | None:
        if agg_contents_df is None:
            return None

        vectorize_udf = udf(
            lambda content: vectorize_item_contents({"content": content}) if content else [],
            ArrayType(FloatType())
        )

        # Apply the UDF to add a new column with embeddings
        vectorized_df = agg_contents_df \
            .withColumn("values", vectorize_udf(col("content"))) \
            .withColumnRenamed("product_id", "id")

        return vectorized_df.select("id", "values", "metadata")

    def vectorize_features(self, agg_features_df: DataFrame) -> DataFrame | None:
        if agg_features_df is None:
            return None

        vectorize_udf = udf(vectorize_item_features, ArrayType(FloatType()))

        vectorized_df = agg_features_df \
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

        return vectorized_df.select("id", "values")

    def start(self):
        distributed_content_df, top_products_based_cat_df, distributed_features_df = self.define_item_profile()

        # Store them in HDFS, store top_products to MongoDB
        ItemModel.store_agg_data_to_mongodb(top_products_based_cat_df, "top_n_products_by_category")
        ItemModel.store_agg_data_to_mongodb(distributed_content_df.drop("product_num", "product_range", "metadata"), "item_contents")
        ItemModel.store_agg_data_to_mongodb(distributed_features_df.drop("product_num", "product_range", "metadata"), "item_features")

        self.start_vectorization(distributed_features_df, distributed_content_df)

        self.stop()

    def stop(self):
        print("IT'S DONE")


ibs = ItemBatchService()
ibs.start()
