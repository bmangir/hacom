from pyspark.sql.functions import *
import numpy as np

from config import sentence_transformer_model


def vectorize(ctr, user_profile, browsing_behavior, purchase_behavior, product_preferences,
              loyalty_score, engagement_score, preference_stability, price_sensitivity, category_exploration, brand_loyalty,
              total_impressions, avg_view_duration):
    weights = {
        "num_features": 1.2,
        "categories": 1.5,
        "products": 2.0,
        "brands": 1.3,
        "user_profile": 1.1,
        "seasonal": 0.8,
        "recent_views": 1.7,
        "recent_purchases": 2.2,
        "loyalty_score": 1.4,
        "engagement_score": 1.3,
        "preference_stability": 1.2,
        "price_sensitivity": 1.1,
        "category_exploration": 1.0,
        "brand_loyalty": 1.5,
        "default": 1.0
    }

    # Extract Numeric Features Using Dot Notation
    num_features = [
        float(ctr) if ctr is not None else 0,
        browsing_behavior["freq_views"]["overall_avg_category_view_duration"] if "freq_views" in browsing_behavior else 0,
        browsing_behavior["freq_views"]["overall_avg_product_view_duration"] if "freq_views" in browsing_behavior else 0,
        browsing_behavior["total_add_to_cart"] if "total_add_to_cart" in browsing_behavior else 0,
        browsing_behavior["total_add_to_wishlist"] if "total_add_to_wishlist" in browsing_behavior else 0,
        browsing_behavior["total_clicks"] if "total_clicks" in browsing_behavior else 0,
        browsing_behavior["total_views"] if "total_views" in browsing_behavior else 0,
        product_preferences["avg_review_rating"] if "avg_review_rating" in product_preferences else 0,
        product_preferences["review_count"] if "review_count" in product_preferences else 0,
        purchase_behavior["avg_order_value"] if "avg_order_value" in purchase_behavior else 0,
        purchase_behavior["avg_orders_per_month"] if "avg_orders_per_month" in purchase_behavior else 0,
        purchase_behavior["total_purchase"] if "total_purchase" in purchase_behavior else 0,
        purchase_behavior["total_spent"] if "total_spent" in purchase_behavior else 0,
        user_profile["account_age_days"] if "account_age_days" in user_profile else 0,
        user_profile["age"] if "age" in user_profile else 0,
        user_profile["avg_session_duration_sec"] if "avg_session_duration_sec" in user_profile else 0,
        user_profile["session_counts_last_30_days"],
        user_profile["total_sessions"],
        loyalty_score * weights["loyalty_score"] if loyalty_score else 0,
        engagement_score * weights["engagement_score"] if engagement_score else 0,
        preference_stability * weights["preference_stability"] if preference_stability else 0,
        price_sensitivity * weights["price_sensitivity"] if price_sensitivity else 0,
        category_exploration * weights["category_exploration"] if category_exploration else 0,
        brand_loyalty * weights["brand_loyalty"] if brand_loyalty else 0,
        total_impressions * weights["default"] if total_impressions else 0,
        avg_view_duration * weights["default"] if avg_view_duration else 0
    ]

    num_features = [float(x) for x in num_features]

    # Convert List-Based Features into Embeddings
    categories_vector = sentence_transformer_model.encode(
        " ".join(browsing_behavior["freq_views"]["categories"])) if "freq_views" in browsing_behavior else np.zeros(384)
    products_vector = sentence_transformer_model.encode(
        " ".join(browsing_behavior["freq_views"]["products"])) if "freq_views" in browsing_behavior else np.zeros(384)
    brands_vector = sentence_transformer_model.encode(" ".join(product_preferences["most_purchased_brands"])) if product_preferences[
        "most_purchased_brands"] else np.zeros(384)

    # Convert Categorical Features into Embeddings
    user_profile_str = f"{user_profile['gender']} {user_profile['device_type']} {user_profile['payment_method']}"
    user_profile_vector = sentence_transformer_model.encode(user_profile_str)

    # Extract Seasonal Data
    seasonal_data = purchase_behavior["seasonal_data"]
    seasonal_vector = np.array([seasonal_data[season] for season in ["winter", "spring", "summer", "fall"]])

    # Extract Recent Views and Purchases
    try:
        recent_views_vector = sentence_transformer_model.encode(
            " ".join(browsing_behavior["freq_views"]["recently_viewed_products"])) if "freq_views" in browsing_behavior else np.zeros(384)
    except:
        recent_views_vector = np.zeros(384)
    try:
        recent_purchases_vector = sentence_transformer_model.encode(
            " ".join(purchase_behavior["recently_purchased_products"])) if "recently_purchased_products" in purchase_behavior else np.zeros(384)
    except:
        recent_purchases_vector = np.zeros(384)

    # Apply Weights and Concatenate
    weighted_vector = np.concatenate([
        num_features,
        categories_vector * weights["categories"],
        products_vector * weights["products"],
        brands_vector * weights["brands"],
        user_profile_vector * weights["user_profile"],
        seasonal_vector * weights["seasonal"],
        recent_views_vector * weights["recent_views"],
        recent_purchases_vector * weights["recent_purchases"]
    ])

    return weighted_vector.tolist()


def extract_place_name(address):
    # TODO: search for this to find the address
    return "NaN"


def transform_data(user_profile: DataFrame, browsing_behavior: DataFrame,
                   purchase_behavior: DataFrame, product_preferences: DataFrame, ctr_df: DataFrame) -> DataFrame:
    joined_df = user_profile \
        .join(browsing_behavior, "user_id", "left") \
        .join(purchase_behavior, "user_id", "left") \
        .join(product_preferences, "user_id", "left")

    user_profile_columns = user_profile.columns
    user_profile_struct = struct(*[col(c) for c in user_profile_columns if c != 'user_id'])

    default_freq_views = struct(
        coalesce(col("freq_views.products"), array().cast("array<string>")).alias("products"),
        coalesce(col("freq_views.avg_product_view_duration"), array().cast("array<double>")).alias("avg_product_view_duration"),
        coalesce(col("freq_views.overall_avg_product_view_duration"), lit(0.0)).alias("overall_avg_product_view_duration"),
        coalesce(col("freq_views.categories"), array().cast("array<string>")).alias("categories"),
        coalesce(col("freq_views.avg_category_view_duration"), array().cast("array<double>")).alias("avg_category_view_duration"),
        coalesce(col("freq_views.overall_avg_category_view_duration"), lit(0.0)).alias("overall_avg_category_view_duration"),
        coalesce(col("freq_views.recently_viewed_products"), array().cast("array<string>")).alias("recently_viewed_products"),
        coalesce(col("freq_views.recent_view_durations"), array().cast("array<bigint>")).alias("recent_view_durations")
    )

    browsing_behavior_struct = struct(
        coalesce(col("total_clicks"), lit(0)).alias("total_clicks"),
        coalesce(col("total_views"), lit(0)).alias("total_views"),
        coalesce(col("total_add_to_cart"), lit(0)).alias("total_add_to_cart"),
        coalesce(col("total_add_to_wishlist"), lit(0)).alias("total_add_to_wishlist"),
        default_freq_views.alias("freq_views")
    )

    purchase_behavior_columns = [
        col(c) for c in purchase_behavior.columns if c not in ["user_id", "seasonal_data"]
    ]
    purchase_behavior_struct = struct(
        *purchase_behavior_columns,
        struct(
            col("seasonal_data.winter").alias("winter"),
            col("seasonal_data.spring").alias("spring"),
            col("seasonal_data.summer").alias("summer"),
            col("seasonal_data.fall").alias("fall")
        ).alias("seasonal_data")
    )

    product_preferences_columns = product_preferences.columns
    product_preferences_struct = struct(*[col(c) for c in product_preferences_columns if c != 'user_id'])

    final_df = joined_df.select(
        "user_id",
        user_profile_struct.alias("user_profile"),
        browsing_behavior_struct.alias("browsing_behavior"),
        purchase_behavior_struct.alias("purchase_behavior"),
        product_preferences_struct.alias("product_preferences"),
    )

    final_df = final_df.join(ctr_df, "user_id", "left")
    final_df = final_df.withColumn("agg_time", (unix_timestamp(current_timestamp()) * 1000))

    return final_df


def distribution_of_df_by_range(df: DataFrame, range: int = 10):
    """
    :param df:
    :param range: Distribute the df by mode (max 5 distributed files)
    :return:
    """

    df = df.withColumn("user_num", regexp_replace(col("user_id"), "[^0-9]", "").cast("int"))
    df = df.withColumn("user_range", (col("user_num") % range).cast("int"))

    return df.repartitionByRange(range, "user_range")
