import os

from dotenv import load_dotenv

load_dotenv()

PG_CONNECTOR_DICT = {
    "PG_DB_NAME": os.getenv("PG_DB_NAME"),
    "PG_DB_USER": os.getenv("PG_DB_USER"),
    "PG_DB_PASSWORD": os.getenv("PG_DB_PASSWORD"),
    "PG_DB_HOST": os.getenv("PG_DB_HOST"),
    "PG_DB_PORT": os.getenv("PG_DB_PORT")
}

MONGO_URI = os.getenv("MONGO_URI")

JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY')
JWT_ACCESS_TOKEN_EXPIRES = os.getenv('JWT_ACCESS_TOKEN_EXPIRES', 3600)
