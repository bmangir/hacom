import psycopg2
from psycopg2 import pool

from backend.config.config import NEON_PG_URI


class NeonPostgresConnector:
    connection_pool = None

    @classmethod
    def initialize_connection_pool(cls, minconn=1, maxconn=20):
        if cls.connection_pool is None:
            try:
                cls.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn, maxconn,
                    NEON_PG_URI
                )
                print("Neon connection pool created successfully")
            except psycopg2.Error as error:
                print("Failed to create Neon connection pool", error)

    @classmethod
    def get_connection(cls):
        if cls.connection_pool:
            try:
                print("Neon connection is created successfully")
                return cls.connection_pool.getconn()
            except psycopg2.Error as error:
                print("Failed to get connection from Neon pool", error)
        return None

    @classmethod
    def return_connection(cls, connection):
        if cls.connection_pool and connection:
            try:
                cls.connection_pool.putconn(connection)
            except psycopg2.Error as error:
                print("Failed to return connection to Neon pool", error)

    @classmethod
    def close_all_connections(cls):
        if cls.connection_pool:
            try:
                cls.connection_pool.closeall()
                print("All Neon connections in the pool are closed")
            except psycopg2.Error as error:
                print("Failed to close all connections in the Neon pool", error)
