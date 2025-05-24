import certifi
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from backend.config import MONGO_URI


class MongoConnector:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.client = MongoClient(MONGO_URI)
        return cls._instance

    def __init__(self):
        self.client = self.__create_client()

    @staticmethod
    def __create_client():
        try:
            client = MongoClient(MONGO_URI,
                                 serverSelectionTimeoutMS=5000,
                                 tlsCAFile=certifi.where(),
                                 maxPoolSize=200)
            print("Mongo client is created successfully")

            return client
        except ConnectionFailure as e:
            print(f"Connection failed: {e}")
        except ServerSelectionTimeoutError as e:
            print(f"Server selection timed out: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        return None

    def get_client(self):
        return self.client
