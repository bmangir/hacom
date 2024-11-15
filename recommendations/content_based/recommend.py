import json
import re
from flask import Flask, request, jsonify, Blueprint
import pandas as pd
import joblib
import csv
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split
import numpy as np

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

content_based_blueprint = Blueprint("content_based", __name__)

ORDER_DATA_PATH = "sample_data/chatgpt_orders_data.json"
BROWSING_HISTORY_DATA_PATH = "sample_data/chatgpt_browsing_history_data.json"
PRODUCT_REVIEWS_DATA_PATH = "sample_data/chatgpt_product_reviews_data.json"
CLICK_STREAM_DATA_PATH = "sample_data/chatgpt_clickstream_data.json"

# Load saved models
vectorizer = joblib.load("ml_model/tfidf_vectorizer.pkl")
cosine_sim = joblib.load("ml_model/cosine_similarity_matrix.pkl")
product_index = joblib.load("ml_model/product_index.pkl")
product_df = joblib.load("ml_model/product_data.pkl")

# Load user interaction data
with open(BROWSING_HISTORY_DATA_PATH, "r") as file:
    browsing_history = json.load(file)
with open(ORDER_DATA_PATH, "r") as file:
    orders = json.load(file)
with open(CLICK_STREAM_DATA_PATH, "r") as file:
    clickstream = json.load(file)
with open(PRODUCT_REVIEWS_DATA_PATH, "r") as file:
    product_reviews = json.load(file)

# Convert interactions to DataFrames
browsing_df = pd.DataFrame(browsing_history)
orders_df = pd.DataFrame(orders)
clickstream_df = pd.DataFrame(clickstream)
reviews_df = pd.DataFrame(product_reviews)


def get_user_interacted_products(user_id):
    interacted_products = pd.concat([
        orders_df[orders_df["user_id"] == user_id]["product_id"],
        reviews_df[reviews_df["user_id"] == user_id]["product_id"],
        browsing_df[browsing_df["user_id"] == user_id]["product_id"],
        clickstream_df[clickstream_df["user_id"] == user_id]["product_id"].apply(lambda x: x[1])
    ]).unique()
    return interacted_products


def recommend_for_user(user_id, top_n=5):
    interacted_products = get_user_interacted_products(user_id)
    if len(interacted_products) == 0:
        print(f"No interactions found for customer {user_id}.")
        return pd.DataFrame()

    recommendations = pd.DataFrame()
    all_sim_scores = []  # List to store the similarity scores with product_ids

    for product_id in interacted_products:
        if product_id in product_index:
            idx = product_index[product_id]
            sim_scores = list(enumerate(cosine_sim[idx]))
            sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
            sim_scores = sim_scores[1 : top_n + 1]  # Skip the first one (it will be the same product)

            for score in sim_scores:
                product_idx = score[0]
                similarity = score[1]
                recommendations = pd.concat([recommendations, product_df.iloc[product_idx][["product_id"]]])
                all_sim_scores.append({"product_id": product_df.iloc[product_idx]["product_id"], "similarity_score": similarity})

    recommendations = recommendations.drop_duplicates()

    # Create a DataFrame for recommendations with similarity scores
    recommendation_with_scores = pd.DataFrame(all_sim_scores)
    recommendation_with_scores = recommendation_with_scores.drop_duplicates().reset_index(drop=True)

    return recommendation_with_scores


def recommend_popular_products(top_n=5):
    # Return the top N most popular products (based on frequency or sales)
    popular_products = product_df.head(top_n)  # Assuming the first rows are popular, you can replace this with a more sophisticated logic
    recommendation_with_scores = pd.DataFrame({
        "product_id": popular_products["product_id"],
        "similarity_score": [1.0] * len(popular_products)  # Assigning max similarity to popular products
    })
    return recommendation_with_scores


def extract_product_id_from_clickstream(row):
    if "product_id" in row:
        return row["product_id"]
    elif "page_url" in row:
        parts = re.split(r"[/?=&]", row["page_url"])
        return parts[-1] if parts[-1].startswith("P") else None
    return None

if "product_id" not in clickstream_df.columns:
    clickstream_df["product_id"] = clickstream_df.apply(extract_product_id_from_clickstream, axis=1)

# Example usage for cross-validation
if __name__ == "__main__":
    user_id = int(input("Enter User ID: "))
    recommended_products = recommend_for_user(user_id, top_n=5)
    if not recommended_products.empty:
        print("\nRecommended Products:")
        print(recommended_products.sort_values(by=["similarity_score"], ascending=False))