from mongoengine import connect, connection

from config import MONGO_URI


def init_mongodb():
    connect(
        host=MONGO_URI,
        alias="default",
        maxPoolSize=100
    )
    try:
        conn = connection.get_connection('default')
        print(conn.server_info())  # Test connection
        print("MongoDB connection successful")
        return conn
    except Exception as e:
        print(f"MongoDB connection failed: {str(e)}")
        raise