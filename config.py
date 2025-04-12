import os

import certifi
from dotenv import load_dotenv
from pinecone import Pinecone
from pymongo import MongoClient

metadata_fields = {
    "Clothing": {
        "brands": ["ComfortWear", "StylePro", "UrbanEdge", "ClassicThreads", "TrendyFit"],
        "sizes": ["XS", "S", "M", "L", "XL", "XXL"],
        "colors": ["Red", "Blue", "Green", "Black", "White", "Yellow", "Purple"],
        "genders": ["Men", "Women", "Unisex"],
        "materials": ["Cotton", "Polyester", "Denim", "Linen", "Wool", "Silk"],
        "fit": ["Slim Fit", "Regular Fit", "Loose Fit"],
        "fabric_types": ["100% Cotton", "50% Polyester, 50% Cotton", "100% Wool", "Denim", "Linen Blend"],
        "wash_instructions": ["Machine Wash", "Hand Wash", "Dry Clean Only", "Cold Wash"]
    },

    "Electronics": {
        "brands": ["TechMax", "ElectroHub", "GigaCore", "InnoVision", "FutureTek"],
        "products": ["Smartphone", "Laptop", "Tablet", "Smartwatch", "Headphones", "Camera", "Monitor"],
        "categories": ["Mobile Devices", "Computers", "Accessories", "Wearables", "Audio", "Photography"],
        "materials": ["Aluminum", "Plastic", "Glass", "Metal"]
    },

    "Books": {
        "types": ["Fiction", "Non-Fiction", "Mystery", "Biography", "Science", "Fantasy", "Romance", "History"],
        "authors": ["John Doe", "Jane Smith", "Alice Johnson", "Mark Brown", "Emily Davis", "Robert White", "Sophie Green"],
        "publishers": ["Penguin Books", "HarperCollins", "Random House", "Macmillan", "Simon & Schuster", "Oxford Press", "Cambridge University Press"],
        "languages": ["English", "Spanish", "French", "German", "Italian", "Portuguese"],
        "tags": ["bestseller", "new release", "classic", "award-winning", "top-rated", "limited edition"],
        "formats": ["Hardcover", "Paperback", "E-Book"]
    }
}


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_PRODUCTS_DB=os.getenv("MONGO_PRODUCTS_DB")
MONGO_BROWSING_DB=os.getenv("MONGO_BROWSING_DB")
MONGO_RECOMMENDATION_DB=os.getenv("MONGO_RECOMMENDATION_DB")
MONGO_AGG_DATA_DB = os.getenv("MONGO_AGG_DATA_DB")
MONGO_LOGS_DB = os.getenv("MONGO_LOGS_DB")


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
