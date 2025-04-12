from pyspark.sql.functions import *
from pyspark.sql.types import FloatType
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import MinMaxScaler
import numpy as np

model = SentenceTransformer("all-MiniLM-L6-v2")
scaler = MinMaxScaler()


def vectorize(ctr, user_profile, browsing_behavior, purchase_behavior, product_preferences):
    weights = {
        "num_features": 1.2,
        "categories": 1.5,
        "products": 2.0,
        "brands": 1.3,
        "user_profile": 1.1,
        "seasonal": 0.8,
        "recent_views": 1.7,
        "recent_purchases": 2.2,
        "category_affinity": 1.4,
        "price_sensitivity": 1.3,
        "brand_loyalty": 1.6,
        "search_patterns": 1.2,
        "engagement_score": 1.5
    }

    # Extract Numeric Features Using Dot Notation
    num_features = [
        float(ctr) if ctr is not None else 0,
        browsing_behavior["freq_views"]["overall_avg_category_view_duration"] if "freq_views" in browsing_behavior else 0,
        browsing_behavior["freq_views"]["overall_avg_product_view_duration"] if "freq_views" in browsing_behavior else 0,
        browsing_behavior["total_add_to_cart"],
        browsing_behavior["total_add_to_wishlist"],
        browsing_behavior["total_clicks"],
        product_preferences["avg_review_rating"],
        product_preferences["review_count"],
        purchase_behavior["avg_order_value"],
        purchase_behavior["avg_orders_per_month"],
        purchase_behavior["total_purchase"],
        purchase_behavior["total_spent"],
        user_profile["account_age_days"],
        user_profile["age"],
        user_profile["avg_session_duration_sec"],
        user_profile["session_counts_last_30_days"],
        # New features
        user_profile["category_affinity_score"],
        user_profile["price_sensitivity_score"],
        user_profile["brand_loyalty_score"],
        user_profile["search_pattern_score"],
        user_profile["engagement_score"]
    ]

    # Normalize Numeric Features
    norm_num_features = scaler.fit_transform(np.array(num_features).reshape(1, -1)).flatten()

    # Convert List-Based Features into Embeddings
    categories_vector = model.encode(
        " ".join(browsing_behavior["freq_views"]["categories"])) if "freq_views" in browsing_behavior else np.zeros(384)
    products_vector = model.encode(
        " ".join(browsing_behavior["freq_views"]["products"])) if "freq_views" in browsing_behavior else np.zeros(384)
    brands_vector = model.encode(" ".join(product_preferences["most_purchased_brands"])) if product_preferences[
        "most_purchased_brands"] else np.zeros(384)

    # Convert Categorical Features into Embeddings
    user_profile_str = f"{user_profile['gender']} {user_profile['device_type']} {user_profile['payment_method']}"
    user_profile_vector = model.encode(user_profile_str)

    # Extract Seasonal Data
    seasonal_data = purchase_behavior["seasonal_data"]
    seasonal_vector = np.array([seasonal_data[season] for season in ["winter", "spring", "summer", "fall"]])

    # Extract Recent Views and Purchases
    recent_views_vector = model.encode(
        " ".join(browsing_behavior["freq_views"]["recently_viewed_products"])) if "freq_views" in browsing_behavior else np.zeros(384)
    recent_purchases_vector = model.encode(
        " ".join(purchase_behavior["recently_purchased_products"])) if "recently_purchased_products" in purchase_behavior else np.zeros(384)

    # Apply Weights and Concatenate
    weighted_vector = np.concatenate([
        norm_num_features * weights["num_features"],
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
    return "NaN"
    # try:
    #    data = usaddress.tag(address)
    #    for item in data[0]:  # Iterate through the tagged components
    #        if item[1] == "PlaceName":
    #            return item[0]
    #    return "NaN"
    # except Exception as e:
    #    print(f"Error parsing address: {address}. Error: {e}")
    #    return "NaN"  # Return "NaN" on error


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
        # Top-level fields (total_clicks, etc.)
        coalesce(col("total_clicks"), lit(0)).alias("total_clicks"),
        coalesce(col("total_add_to_cart"), lit(0)).alias("total_add_to_cart"),
        coalesce(col("total_add_to_wishlist"), lit(0)).alias("total_add_to_wishlist"),

        # Nested freq_views with defaults
        default_freq_views.alias("freq_views")
    )

    purchase_behavior_columns = [
        col(c) for c in purchase_behavior.columns if c not in ["user_id", "seasonal_data"]
    ]
    purchase_behavior_struct = struct(
        *purchase_behavior_columns,  # Other purchase behavior columns
        struct(
            col("seasonal_data.winter").alias("winter"),
            col("seasonal_data.spring").alias("spring"),
            col("seasonal_data.summer").alias("summer"),
            col("seasonal_data.fall").alias("fall")
        ).alias("seasonal_data")  # Ensure proper nesting
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
