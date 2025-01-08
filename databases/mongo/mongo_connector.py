import certifi
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from backend.config.config import MONGO_URI


class MongoConnector:
    def __init__(self):
        self.client = self.__create_client()

    @staticmethod
    def __create_client():
        try:
            client = MongoClient(MONGO_URI,
                                 serverSelectionTimeoutMS=5000,
                                 tlsCAFile=certifi.where())
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
