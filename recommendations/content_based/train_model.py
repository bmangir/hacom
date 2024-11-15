"""
This file is for train the model and save it for future usage.
TODO: Make this with Apache Airflow periodically
"""
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import joblib
import json

PRODUCT_DATA_PATH = "/Users/berkantmangir/Desktop/graduation_project/sample_data_for_training_ml/chatgpt_sample_data/chatgpt_product_data.json"

# Load product data
with open(PRODUCT_DATA_PATH, "r") as file:
    product_data = json.load(file)
product_df = pd.DataFrame(product_data)

# Combine product features into a single text field
product_df["text_features"] = (
        product_df["product_name"] + " " +
        product_df["category"] + " " +
        product_df["brand"] + " " +
        product_df["price"].apply(str) + " " +  # Add price as a string
        product_df["tags"].apply(lambda x: " ".join(x) if isinstance(x, list) else "") + " " +  # Tags as space-separated string
        product_df["details"].apply(lambda x: " ".join(map(str, x.values())) if isinstance(x, dict) else "")
)

# Vectorize product features
vectorizer = TfidfVectorizer(stop_words="english")
tfidf_matrix = vectorizer.fit_transform(product_df["text_features"])

# Compute cosine similarity
cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

# Map product_id to index
product_index = pd.Series(product_df.index, index=product_df["product_id"]).drop_duplicates()

# Save the model components
joblib.dump(vectorizer, "ml_model/tfidf_vectorizer.pkl")
joblib.dump(cosine_sim, "ml_model/cosine_similarity_matrix.pkl")
joblib.dump(product_index, "ml_model/product_index.pkl")
joblib.dump(product_df, "ml_model/product_data.pkl")

print("Model trained and saved successfully!")
