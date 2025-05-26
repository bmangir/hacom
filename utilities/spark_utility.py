import json
import os
import traceback
from _decimal import Decimal
from datetime import datetime
import subprocess

from pymongo import errors
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
import inspect

from config import MONGO_URI, client, MONGO_LOGS_DB, MONGO_PRODUCTS_DB, MONGO_BROWSING_DB, JDBC_URL, NEON_DB_USER, \
    NEON_DB_PASSWORD

# Set transformers cache directory to GCS
os.environ['TRANSFORMERS_CACHE'] = 'gs://my-spark-bucket-mangir/models'
os.environ['SENTENCE_TRANSFORMERS_HOME'] = 'gs://my-spark-bucket-mangir/models'

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.postgresql:postgresql:42.6.0,"
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 "
    "pyspark-shell"
)
db = client[MONGO_LOGS_DB]
collection = db["logs"]


def create_spark_session(app_name, spark=None, num_of_partition="100"):
    if spark is None:
        # Define all required jars including MongoDB dependencies
        jars = [
            "gs://my-spark-bucket-mangir/jars/mongo-spark-connector_2.12-10.3.0.jar",
            "gs://my-spark-bucket-mangir/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar",
            "gs://my-spark-bucket-mangir/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
            "gs://my-spark-bucket-mangir/jars/postgresql-42.6.0.jar",
            "gs://my-spark-bucket-mangir/jars/bson-4.11.1.jar",
            "gs://my-spark-bucket-mangir/jars/mongodb-driver-core-4.11.1.jar",
            "gs://my-spark-bucket-mangir/jars/mongodb-driver-sync-4.11.1.jar"
        ]
        jars_path = ",".join(jars)
        
        spark = SparkSession.builder.appName(app_name) \
            .config("spark.mongodb.read.connection.uri", MONGO_URI) \
            .config("spark.mongodb.write.connection.uri", MONGO_URI) \
            .config("spark.mongodb.input.partitioner", "MongoSamplePartitioner") \
            .config("spark.mongodb.input.partitionerOptions.samplesPerPartition", "1000") \
            .config("spark.sql.extensions", "com.mongodb.spark.sql.connector.MongoSparkConnector") \
            .config("spark.jars", jars_path) \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.default.parallelism", "10") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.shuffle.service.enabled", "true") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", "1") \
            .config("spark.dynamicAllocation.maxExecutors", "2") \
            .config("spark.dynamicAllocation.executorIdleTimeout", "30s") \
            .config("spark.dynamicAllocation.schedulerBacklogTimeout", "15s") \
            .config("spark.cleaner.periodicGC.interval", "1min") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "512m") \
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
            .config("spark.sql.shuffle.partitions.maxParallelism", "10") \
            .config("spark.sql.autoBroadcastJoinThreshold", "5m") \
            .config("spark.sql.broadcastTimeout", "300") \
            .config("spark.network.timeout", "420s") \
            .config("spark.executor.heartbeatInterval", "30s") \
            .getOrCreate()

    return spark


def read_from_mongodb(spark, db_name, coll_name, filter={}):
    if isinstance(filter, str):
        filter = json.loads(filter)

    # Create pipeline with filter
    pipeline = [{"$match": filter}]

    try:
        df = spark.read \
            .format("mongodb") \
            .option("connection.uri", MONGO_URI) \
            .option("database", db_name) \
            .option("collection", coll_name) \
            .option("pipeline", json.dumps(pipeline)) \
            .load()
        
        print(f"Successfully read from MongoDB: {db_name}.{coll_name}")
        return df
        
    except Exception as e:
        print(f"Error reading from MongoDB: {str(e)}")
        raise


def read_postgres_table(spark, table_name) -> DataFrame:
    return spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table_name) \
        .option("driver", "org.postgresql.Driver") \
        .option("user", NEON_DB_USER) \
        .option("password", NEON_DB_PASSWORD) \
        .load()


def extract_data(spark: SparkSession):
    try:
        # Read from MongoDB
        clickstream_df = read_from_mongodb(spark, MONGO_BROWSING_DB, "clickstream")
        products_df = read_from_mongodb(spark, MONGO_PRODUCTS_DB, "products")
        browsing_df = read_from_mongodb(spark, MONGO_BROWSING_DB, "browsing_history")
        searchs_df = read_from_mongodb(spark, MONGO_BROWSING_DB, "search_history")

        # Read from NeonDB
        users_df = read_postgres_table(spark, "users")
        orders_df = read_postgres_table(spark, "orders")
        reviews_df = read_postgres_table(spark, "product_reviews")
        cart_df = read_postgres_table(spark, "cart")
        wishlist_df = read_postgres_table(spark, "wishlist")
        session_df = read_postgres_table(spark, "sessions")

        collection.insert_one(document={
            "process_type": "batch",
            "caller_function": inspect.stack()[1].function,
            "error_type": None,
            "error_msg": None,
            "where": {
                "func": "extract_data",
                "op": "Extract data from MongoDB or NeonDB"
            },
            "succeed_msg": "Data are extracted successfully from MongoDB and NeonDB",
            "status": "S",
            "at": datetime.now(),
            "comment": ""
        })

        return preprocess_raw_data(users_df, orders_df, reviews_df, cart_df, wishlist_df, browsing_df, clickstream_df, session_df, products_df)

    except Exception as e:
        print(f"\nERROR in extract_data: {str(e)}")

        collection.insert_one(document={
            "process_type": "batch",
            "caller_function": inspect.stack()[1].function,
            "error_type": e.__class__.__name__,
            "error_msg": traceback.format_exc(),
            "where": {
                "func": "extract_data",
                "op": "Extract data from MongoDB or NeonDB"
            },
            "succeed_msg": None,
            "status": "F",
            "at": datetime.now(),
            "comment": "Failure to extract the data from MongoDB or Postgresql."
        })
        raise


def preprocess_raw_data(users_df, orders_df, reviews_df, cart_df, wishlist_df, browsing_df, clickstream_df, session_df, products_df):
    """
    Process the raw data for further usages
    Select needed columns, drop not needed column
    Find / calculate some values for browsing history df / session df
    :param users_df:
    :param orders_df:
    :param reviews_df:
    :param cart_df:
    :param wishlist_df:
    :param browsing_df:
    :param clickstream_df:
    :param session_df:
    :param products_df:
    :return:
    """

    users_df = users_df.drop("email", "password")
    users_ids_df = users_df.select("user_id").distinct()
    product_ids_df = products_df.select("product_id").distinct()

    orders_df = orders_df \
        .withColumnRenamed("product_id", "order_product_id") \
        .withColumnRenamed("quantity", "order_quantity") \
        .withColumn("order_timestamp", (col("order_date") / 1000).cast("timestamp")) \
        .select("order_id", "user_id", "order_product_id", "order_date", "status", "order_quantity",
                "total_amount", "payment_method", "updated_at", "order_timestamp", "unit_price", "shipping_address")
    reviews_df = reviews_df \
        .withColumnRenamed("product_id", "review_product_id")
    cart_df = cart_df \
        .withColumnRenamed("product_id", "cart_product_id") \
        .withColumnRenamed("quantity", "cart_quantity") \
        .withColumnRenamed("cart_id", "cart_cart_id") \
        .withColumnRenamed("action_type", "cart_action_type") \
        .withColumnRenamed("action_date", "cart_action_date") \
        .select("user_id", "cart_product_id", "cart_action_type", "cart_quantity", "cart_action_date")
    wishlist_df = wishlist_df \
        .withColumnRenamed("product_id", "wishlist_product_id") \
        .withColumnRenamed("action_type", "wishlist_action_type") \
        .withColumnRenamed("action_date", "wishlist_action_date") \
        .select("user_id", "wishlist_product_id", "wishlist_action_type", "wishlist_action_date")
    browsing_df = browsing_df.withColumnRenamed("referrer_url", "browsing_referred_url") \
        .withColumnRenamed("page_url", "browsing_page_url") \
        .withColumn("browsing_timestamp", coalesce(col("timestamp"), col("view_date"))) \
        .drop("_id", "product_id", "history_id", "session_id", "view_date")

    clickstream_df = clickstream_df.drop("_id", "product_id", "session_id")

    try:
        browsing_df = browsing_df \
            .withColumn("product_id",
                        when(
                            col("browsing_page_url").rlike(r"^/product/[\w\d]+"),
                            regexp_extract(col("browsing_page_url"), r"/product/([\w\d]+)", 1)
                        ).when(
                            col("browsing_page_url").rlike(r"^/products/details/[\w\d]+"),
                            regexp_extract(col("browsing_page_url"), r"/products/details/([\w\d]+)", 1)
                        ).otherwise(None)) \
            .withColumn("category",
                        when(
                            col("browsing_page_url").rlike("^/category/"),
                            regexp_extract(col("browsing_page_url"), r"/category/([A-Za-z0-9_/]+)", 1)
                        ).otherwise(None))
    except Exception as e:
        collection.insert_one(document={
            "process_type": "batch",
            "caller_function": inspect.stack()[1].function,
            "error_type": e.__class__.__name__,
            "error_msg": traceback.format_exc(),
            "where": {
                "func": "preprocess_data",
                "op": "Find the product id in browsing_df"
            },
            "succeed_msg": None,
            "status": "F",
            "at": datetime.now(),
            "comment": "The regex can be wrong."
        })

        raise

    try:
        session_df = session_df \
            .withColumn("end_time",
                        when(col("end_time").isNull() & col("start_time").isNotNull(), col("start_time"))
                        .when(col("end_time").isNull() & col("start_time").isNull(), lit(0))
                        .otherwise(col("end_time"))) \
            .withColumn("start_time",
                        when(col("start_time").isNull() & col("end_time").isNotNull(), col("end_time"))
                        .when(col("start_time").isNull() & col("end_time").isNull(), lit(0))
                        .otherwise(col("start_time")))
    except Exception as e:
        collection.insert_one(document={
            "process_type": "batch",
            "caller_function": inspect.stack()[1].function,
            "error_type": e.__class__.__name__,
            "error_msg": traceback.format_exc(),
            "where": {
                "func": "preprocess_data",
                "op": "Avoid for none values of end_time and start_time in session_df"
            },
            "succeed_msg": None,
            "status": "F",
            "at": datetime.now(),
            "comment": "The when conditions can be wrong."
        })

        raise

    collection.insert_one(document={
        "process_type": "batch",
        "caller_function": inspect.stack()[1].function,
        "error_type": None,
        "error_msg": None,
        "where": {
            "func": "preprocess_data",
            "op": "Avoid for none values of end_time and start_time in session_df"
        },
        "succeed_msg": "Successfully preprocessed the raw data",
        "status": "S",
        "at": datetime.now(),
        "comment": "This is for further usage"
    })

    return users_df, orders_df, reviews_df, cart_df, wishlist_df, browsing_df, clickstream_df, session_df, products_df, users_ids_df, product_ids_df


def store_to_hdfs(df: DataFrame, target_path: str, column_name, format="parquet", mode="overwrite"):
    # hdfs_path = f"hdfs://localhost:9000/{target_path}/{formatted_date}/"
    formatted_date = datetime.now().strftime("%Y%m%d%H")

    try:
        df.repartition(column_name) \
            .write \
            .mode(mode) \
            .partitionBy(column_name) \
            .parquet(f"{target_path}/{formatted_date}/")

        collection.insert_one(document={
            "process_type": "batch",
            "caller_function": inspect.stack()[1].function,
            "error_type": None,
            "error_msg": None,
            "where": {
                "func": "store_to_hdfs",
                "op": "Writing the df to HDFS"
            },
            "succeed_msg": f"Stored DataFrame successfully to {target_path}/{formatted_date} in HDFS",
            "status": "S",
            "at": datetime.now(),
            "comment": ""
        })

    except Exception as e:
        collection.insert_one(document={
            "process_type": "batch",
            "caller_function": inspect.stack()[1].function,
            "error_type": e.__class__.__name__,
            "error_msg": traceback.format_exc(),
            "where": {
                "func": "store_to_hdfs",
                "op": "Writing the df to HDFS"
            },
            "succeed_msg": None,
            "status": "F",
            "at": datetime.now(),
            "comment": f"column_name can be wrong"
        })


def read_from_hdfs(spark: SparkSession, base_path: str):
    try:
        directories = os.listdir(base_path)  # List all directories in the base path
        valid_directories = [d for d in directories if len(d) == 10 and d.isdigit()]  # Filter out any non-directory files and ensure the directories follow the expected format
        sorted_directories = sorted(valid_directories, reverse=True)  # Sort the directories in descending order based on the directory name (yyyymmddhh)
        latest_directory = sorted_directories[0]  # Get the latest directory (the first in the sorted list)

        # Read data from the latest directory
        latest_data_path = os.path.join(base_path, latest_directory)
        df = spark.read.parquet(latest_data_path)

        collection.insert_one(document={
            "process_type": "batch",
            "caller_function": inspect.stack()[1].function,
            "error_type": None,
            "error_msg": None,
            "where": {
                "func": "read_from_hdfs",
                "op": "Reading parquet files from HDFS"
            },
            "succeed_msg": f"Read data from {latest_data_path} in HDFS",
            "status": "S",
            "at": datetime.now(),
            "comment": "Parquet files to dataframe"
        })

        return df

    except Exception as e:
        collection.insert_one(document={
            "process_type": "batch",
            "caller_function": inspect.stack()[1].function,
            "error_type": e.__class__.__name__,
            "error_msg": traceback.format_exc(),
            "where": {
                "func": "read_from_hdfs",
                "op": "Reading parquet files from HDFS"
            },
            "succeed_msg": None,
            "status": "F",
            "at": datetime.now(),
            "comment": "Can be there is no any file in HDFS or spark worker is stopped."
        })

        return


def store_df_to_mongodb(db_name, collection_name, df: DataFrame, mode="append"):
    try:
        df.write \
            .format("mongodb") \
            .mode(mode) \
            .option("spark.mongodb.write.connection.uri", MONGO_URI) \
            .option("database", db_name) \
            .option("collection", collection_name) \
            .save()

        print("DF is stored to MongoDB")
    except Exception as e:
        print(f"Error storing DataFrame to MongoDB: {e}")


def update_doc_in_mongodb(db_name, collection_name, df, id_col):
    try:
        db = client[db_name]
        collection = db[collection_name]

        for row in df.collect():
            doc = bson_safe(row)
            id = doc[f"{id_col}"]

            if "_id" in doc:
                del doc["_id"]

            collection.update_one(
                {f"{id_col}": id},
                {"$set": doc},
                upsert=True
            )

        print("Docs are updated")
    except Exception as e:
        print(f"Error update the doc in MongoDB: {e}")
        raise


def bson_safe(obj):
    if isinstance(obj, Row):
        return bson_safe(obj.asDict())
    elif isinstance(obj, dict):
        return {k: bson_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [bson_safe(item) for item in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj