"""
This file is for train the model and save it for future usage.
TODO: Make this with Apache Airflow periodically
"""

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import joblib

# Sample product data
#products = [
#    {"product_id": "P201", "product_name": "Wireless Earbuds", "category": "Electronics", "tags": ["wireless", "audio", "portable"]},
#    {"product_id": "P250", "product_name": "Portable Power Bank", "category": "Electronics", "tags": ["portable", "charging", "battery"]},
#    {"product_id": "P301", "product_name": "The Great Adventure", "category": "Books", "tags": ["fiction", "adventure", "novel"]}
#]

#products = pd.read_json("/Users/berkantmangir/Desktop/sample_product_data.json")
products = pd.read_json("/Users/berkantmangir/Desktop/graduation_project/sample_data_for_training_ml/chatgpt_product_data.json")

# Convert data to a DataFrame
df_products = pd.DataFrame(products)

# Create a content feature
df_products['content'] = (
        df_products['product_name'] + " " +
        df_products['category'] + " " +
        df_products['tags'].apply(lambda x: ' '.join(x)) + " " +  # Join tags within each list
        df_products['brand'] + " " +
        df_products['details'].apply(lambda x: ' '.join(str(v) for v in x.values()))
)

# Train TF-IDF vectorizer
tfidf_vectorizer = TfidfVectorizer(stop_words='english', min_df=1, max_features=1000, ngram_range=(1, 2))
tfidf_matrix = tfidf_vectorizer.fit_transform(df_products['content'])

# Save both the model (vectorizer) and product data
joblib.dump(tfidf_vectorizer, 'tfidf_vectorizer_content_based.joblib')
df_products.to_csv("products.csv", index=False)
print("Model and data saved.")
