from flask import Flask, request, jsonify, Blueprint
import pandas as pd
import joblib
import csv
from sklearn.metrics.pairwise import cosine_similarity

content_based_blueprint = Blueprint("content_based", __name__)

# Load the model and data once when the API starts
tfidf_vectorizer = joblib.load('/Users/berkantmangir/Desktop/graduation_project/hacom/recommendations/content_based/tfidf_vectorizer_content_based.joblib')
df_products = pd.read_csv("/Users/berkantmangir/Desktop/graduation_project/hacom/recommendations/content_based/products.csv")
tfidf_matrix = tfidf_vectorizer.transform(df_products['content'])


@content_based_blueprint.route('/recommend/contentBased', methods=['POST'])
def recommend():
    data = request.get_json()
    source_product_id = data.get("product_id")
    user_id = data.get("user_id")

    # Find the index of the source product
    source_idx = df_products[df_products['product_id'] == source_product_id].index[0]

    # Calculate cosine similarity for the source product
    sim_scores = cosine_similarity(tfidf_matrix[source_idx], tfidf_matrix).flatten()
    top_indices = sim_scores.argsort()[-6:][::-1][1:]  # Get top 5 recommendations, exclude itself

    similarity_threshold = 0.75

    # Format recommendations
    recommendations = [
        {
            "user_id": user_id,
            "source_product_id": source_product_id,
            "source_product_name": df_products['product_name'][source_idx],
            "recommended_product_id": df_products['product_id'][idx],
            "recommended_product_name": df_products['product_name'][idx],
            "similarity_score": round(sim_scores[idx], 2)
        }
        for idx in top_indices #if sim_scores[idx] >= similarity_threshold
    ]

    return jsonify({"Content_Based_Filtering": recommendations})
