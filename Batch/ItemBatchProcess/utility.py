import nltk
import numpy
import torch
from nltk.sentiment import SentimentIntensityAnalyzer
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType
from pyspark.sql.dataframe import DataFrame
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import MinMaxScaler
from transformers import BertTokenizer, BertModel

nltk.download("vader_lexicon")

sia = SentimentIntensityAnalyzer()
sentence_transformer_model = SentenceTransformer("all-MiniLM-L6-v2")
scaler = MinMaxScaler()

tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
bert_model = BertModel.from_pretrained('bert-base-uncased')

# TODO: @udf can be problem of the error

def analyze_sentiment(text):
    """Compute sentiment score using VADER"""
    if text:
        return sia.polarity_scores(text)["compound"]
    return 0.0


def weighted_text(content_str, weights):
    """ Apply weights to different product attributes for embedding. """
    weighted_parts = []

    for key, weight in weights.items():
        if key in content_str.lower():
            value = content_str.split(f"{key.capitalize()}:")[1].split("|")[0].strip()
            weighted_parts.append((value + " ") * int(weight))  # Repeat based on weight
    return " ".join(weighted_parts)


def vectorize_item_contents(product_data):
    weights = {
        "category": 4.0,
        "brand": 3.0,
        "tags": 2.0,
        "details": 2.0,
        "price bucket": 1.5,
        "material": 1.2,
        "name": 1.2,
        "gender": 1.0,
        "color": 0.8,
        "size": 0.5
    }
    weighted_content = weighted_text(product_data["content"], weights)
    vector_embedding = sentence_transformer_model.encode(weighted_content).tolist()

    return vector_embedding


def safe_get(value, default=0.0):
    return value if value not in [None, "", [], {}] else default


def vectorize_item_features(
        purchase_count, view_count, wishlist_count, cart_addition_count,
        conversion_rate, abandonment_rate, avg_rating, trending_score,
        winter_sales, spring_sales, summer_sales, fall_sales,
        daily_units_sold, trend_7_day, trend_30_day,
        last_1_day_sales, last_7_days_sales, last_30_days_sales, content
):

    weights = {
        "seasonal_sales": 1.5,
        "latest_sales_summary": 2.0,
        "historical_sales_summary": 1.2,
        "avg_rating": 1.8,
        "purchase_count": 1.2,
        "view_count": 0.8,
        "conversion_rate": 1.1,
        "abandonment_rate": 1.1,
        "trending_score": 1.4,
        "default": 1.0
    }

    try:
        numeric_features = [
            0.1,  # Dummy feature to avoid zero division
            safe_get(purchase_count) * weights["purchase_count"],
            safe_get(view_count) * weights["view_count"],
            safe_get(wishlist_count) * weights["default"],
            safe_get(cart_addition_count) * weights["default"],
            safe_get(conversion_rate) * weights["conversion_rate"],
            safe_get(abandonment_rate) * weights["abandonment_rate"],
            safe_get(avg_rating) * weights["avg_rating"],
            safe_get(trending_score) * weights["trending_score"],

            safe_get(winter_sales) * weights["seasonal_sales"],
            safe_get(spring_sales) * weights["seasonal_sales"],
            safe_get(summer_sales) * weights["seasonal_sales"],
            safe_get(fall_sales) * weights["seasonal_sales"],

            safe_get(daily_units_sold) * weights["historical_sales_summary"],
            safe_get(trend_7_day) * weights["historical_sales_summary"],
            safe_get(trend_30_day) * weights["historical_sales_summary"],

            safe_get(last_1_day_sales) * weights["latest_sales_summary"],
            safe_get(last_7_days_sales) * weights["latest_sales_summary"],
            safe_get(last_30_days_sales) * weights["latest_sales_summary"],
            ]

        inputs = tokenizer(content, return_tensors="pt", truncation=True, padding=True, max_length=512)
        with torch.no_grad():
            outputs = bert_model(**inputs)

        embeddings = outputs.last_hidden_state[:, 0, :]
        text_embedding = embeddings.squeeze().numpy()
        combined_features = numpy.concatenate([numeric_features, text_embedding])

        return combined_features.tolist()
    except Exception as e:
        print(f"Error in vectorize_item_features: {e}")
        return [0.1] + [0.0] * 786


def _extract_details_column(df: DataFrame):
    df_details = df.select(
        col("product_id"),
        *[expr(f"details['{key}']").alias(key) for key in df.select("details.*").columns]
    )

    # Create a string representation of key-value pairs
    details_columns = df_details.columns[1:]  # Exclude product_id
    df_result = df_details.withColumn(
        "details",
        concat_ws(", ", *[
            when(col(c).isNotNull(), concat(lit(f"{c}="), col(c))).otherwise(lit("")) for c in details_columns
        ])
    )

    # Remove any leading/trailing commas and spaces
    df_result = df_result.withColumn("details", expr("trim(translate(details, ',', ' '))"))
    return df_result


def _create_product_contents(df):
    df = df.withColumn(
        "content",
        concat_ws(
            " | ",
            concat(lit("Name:"), col("product_name")),
            concat(lit("Category:"), col("category")),
            concat(lit("Tags:"), col("tags")),
            when(col("brand").isNotNull(), concat_ws("", lit("Brand:"), col("brand"))),
            when(col("material").isNotNull(), concat_ws("", lit("Material:"), col("material"))),
            when(col("gender").isNotNull(), concat_ws("", lit("Gender:"), col("gender"))),
            when(col("colors").isNotNull(), concat_ws("", lit("Color:"), col("colors"))),
            when(col("sizes").isNotNull(), concat_ws("", lit("Size:"), col("sizes"))),
            when(col("language").isNotNull(), concat_ws("", lit("Language:"), col("language"))),
            when(col("format").isNotNull(), concat_ws("", lit("Format:"), col("format"))),
            when(col("price_bucket").isNotNull(), concat_ws("", lit("Price bucket:"), col("price_bucket"))),
            concat_ws("", lit("Details:"), col("details"))
        ),
    )
    return df


def distribution_of_df_by_range(df: DataFrame, range: int = 5):
    """
    :param df:
    :param range: Distribute the df by mode (max 5 distributed files)
    :return:
    """

    df = df.withColumn("product_num", regexp_replace(col("product_id"), "[^0-9]", "").cast("int"))
    df = df.withColumn("product_range", (col("product_num") % range).cast("int"))

    return df.repartitionByRange(range, "product_range")
