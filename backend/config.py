import os

import certifi
from dotenv import load_dotenv
from mongoengine import connect
from pinecone import Pinecone
from pymongo import MongoClient

load_dotenv()

PG_CONNECTOR_DICT = {
    "PG_DB_NAME": os.getenv("PG_DB_NAME"),
    "PG_DB_USER": os.getenv("PG_DB_USER"),
    "PG_DB_PASSWORD": os.getenv("PG_DB_PASSWORD"),
    "PG_DB_HOST": os.getenv("PG_DB_HOST"),
    "PG_DB_PORT": os.getenv("PG_DB_PORT")
}

MONGO_URI = os.getenv("MONGO_URI")
MONGO_PRODUCTS_DB=os.getenv("MONGO_PRODUCTS_DB")
MONGO_BROWSING_DB=os.getenv("MONGO_BROWSING_DB")
MONGO_RECOMMENDATION_DB=os.getenv("MONGO_RECOMMENDATION_DB")
MONGO_AGG_DATA_DB = os.getenv("MONGO_AGG_DATA_DB")
MONGO_LOGS_DB = os.getenv("MONGO_LOGS_DB")
MONGO_PRECOMPUTED_DB = os.getenv("MONGO_PRECOMPUTED_DB")


NEON_PG_URI = os.getenv("NEON_PG_URI")
JDBC_URL=os.getenv("JDBC_URL")
NEON_DB_USER=os.getenv("NEON_DB_USER")
NEON_DB_PASSWORD=os.getenv("NEON_DB_PASSWORD")

JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY')
JWT_ACCESS_TOKEN_EXPIRES = os.getenv('JWT_ACCESS_TOKEN_EXPIRES', 3600)

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
pc = Pinecone(api_key=PINECONE_API_KEY)
ITEM_CONTENTS_HOST = os.getenv("ITEM_CONTENTS_HOST")
ITEM_FEATURES_HOST = os.getenv("ITEM_FEATURES_HOST")
USER_FEATURES_HOST = os.getenv("USER_FEATURES_HOST")

client = MongoClient(MONGO_URI,
                     serverSelectionTimeoutMS=5000,
                     tlsCAFile=certifi.where(),
                     maxPoolSize=200)
