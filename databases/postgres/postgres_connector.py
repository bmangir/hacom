import psycopg2
from psycopg2 import pool

from backend.config.config import PG_CONNECTOR_DICT


class PostgresConnector:
    connection_pool = None

    @classmethod
    def initialize_connection_pool(cls, minconn=1, maxconn=20):
        if cls.connection_pool is None:
            try:
                cls.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn, maxconn,
                    database=PG_CONNECTOR_DICT["PG_DB_NAME"],
                    user=PG_CONNECTOR_DICT["PG_DB_USER"],
                    password=PG_CONNECTOR_DICT["PG_DB_PASSWORD"],
                    host=PG_CONNECTOR_DICT["PG_DB_HOST"],
                    port=PG_CONNECTOR_DICT["PG_DB_PORT"]
                )
                print("Connection pool created successfully")
            except psycopg2.Error as error:
                print("Failed to create connection pool", error)

    @classmethod
    def get_connection(cls):
        if cls.connection_pool:
            try:
                print("Connection is created successfully")
                return cls.connection_pool.getconn()
            except psycopg2.Error as error:
                print("Failed to get connection from pool", error)
        return None

    @classmethod
    def return_connection(cls, connection):
        if cls.connection_pool and connection:
            try:
                cls.connection_pool.putconn(connection)
            except psycopg2.Error as error:
                print("Failed to return connection to pool", error)

    @classmethod
    def close_all_connections(cls):
        if cls.connection_pool:
            try:
                cls.connection_pool.closeall()
                print("All connections in the pool are closed")
            except psycopg2.Error as error:
                print("Failed to close all connections in the pool", error)