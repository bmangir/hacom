import os

import certifi
from dotenv import load_dotenv
from pinecone import Pinecone
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_PRODUCTS_DB=os.getenv("MONGO_PRODUCTS_DB")
MONGO_BROWSING_DB=os.getenv("MONGO_BROWSING_DB")
MONGO_RECOMMENDATION_DB=os.getenv("MONGO_RECOMMENDATION_DB")
MONGO_AGG_DATA_DB = os.getenv("MONGO_AGG_DATA_DB")
MONGO_LOGS_DB = os.getenv("MONGO_LOGS_DB")
MONGO_PRECOMPUTED_DB = os.getenv("MONGO_PRECOMPUTED_DB")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INTERACTION_TOPIC = os.getenv("KAFKA_INTERACTION_TOPIC", "interactionTopic")

NEON_PG_URI = os.getenv("NEON_PG_URI")
JDBC_URL=os.getenv("JDBC_URL")
NEON_DB_USER=os.getenv("NEON_DB_USER")
NEON_DB_PASSWORD=os.getenv("NEON_DB_PASSWORD")

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
ITEM_CONTENTS_HOST = os.getenv("ITEM_CONTENTS_HOST")
ITEM_FEATURES_HOST = os.getenv("ITEM_FEATURES_HOST")
USER_FEATURES_HOST = os.getenv("USER_FEATURES_HOST")

client = MongoClient(MONGO_URI,
                     serverSelectionTimeoutMS=5000,
                     tlsCAFile=certifi.where(),
                     maxPoolSize=200)

pinecone = Pinecone(PINECONE_API_KEY)
