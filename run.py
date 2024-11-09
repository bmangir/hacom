from db.mongo import MongoConnector
from db.postgres import PostgresConnector

PostgresConnector.initialize_connection_pool()
postgresSQL_conn = PostgresConnector.get_connection()
PostgresConnector.close_all_connections()

# Initialize the MongoDB connector instance
mongo_connector = MongoConnector()
mongodb_client = mongo_connector.get_client()