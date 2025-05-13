import os

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import MinMaxScaler
from transformers import BertTokenizer, BertModel

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_PRODUCTS_DB=os.getenv("MONGO_PRODUCTS_DB")
MONGO_BROWSING_DB=os.getenv("MONGO_BROWSING_DB")
MONGO_RECOMMENDATION_DB=os.getenv("MONGO_RECOMMENDATION_DB")
MONGO_AGG_DATA_DB = os.getenv("MONGO_AGG_DATA_DB")
MONGO_LOGS_DB = os.getenv("MONGO_LOGS_DB")
MONGO_PRECOMPUTED_DB = os.getenv("MONGO_PRECOMPUTED_DB")

NEON_PG_URI = os.getenv("NEON_PG_URI")
JDBC_URL = os.getenv("JDBC_URL")
NEON_DB_USER = os.getenv("NEON_DB_USER")
NEON_DB_PASSWORD = os.getenv("NEON_DB_PASSWORD")

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
ITEM_CONTENTS_HOST = os.getenv("ITEM_CONTENTS_HOST")
ITEM_FEATURES_HOST = os.getenv("ITEM_FEATURES_HOST")
USER_FEATURES_HOST = os.getenv("USER_FEATURES_HOST")

CONFLUENT_BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
CONFLUENT_API_KEY = os.getenv("CONFLUENT_API_KEY")
CONFLUENT_API_SECRET = os.getenv("CONFLUENT_API_SECRET")
CONFLUENT_INTERACTION_TOPIC = os.getenv("CONFLUENT_INTERACTION_TOPIC")
CONFLUENT_CLIENT_ID = os.getenv("CONFLUENT_CLIENT_ID")

client = MongoClient(MONGO_URI,
                     serverSelectionTimeoutMS=5000,
                     tlsCAFile=certifi.where(),
                     maxPoolSize=200)


NEW_PINECONE_API = os.getenv("NEW_PINECONE_API")
NEW_USER_FEATURES_HOST = os.getenv("NEW_USER_FEATURES_HOST")
NEW_ITEM_FEATURES_HOST = os.getenv("NEW_ITEM_FEATURES_HOST")
NEW_ITEM_CONTENTS_HOST = os.getenv("NEW_ITEM_CONTENTS_HOST")

nltk.download("vader_lexicon")

sia = SentimentIntensityAnalyzer()
sentence_transformer_model = SentenceTransformer("all-MiniLM-L6-v2")
scaler = MinMaxScaler()

tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
bert_model = BertModel.from_pretrained('bert-base-uncased')
