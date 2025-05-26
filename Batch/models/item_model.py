from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from config import MONGO_AGG_DATA_DB
from spark_utility import extract_data, read_from_hdfs, store_to_hdfs, store_df_to_mongodb
from pinecone_utility import store_to_pinecone


def fetch_data(spark: SparkSession):
    if not spark:
        raise RuntimeError("Spark session is not active in fetch_data!")
    return extract_data(spark=spark)


def fetch_data_from_hdfs(spark, target_path, base_path="exp_result/"):
    return read_from_hdfs(spark=spark, base_path=base_path+target_path)


def store_df_to_hdfs(df, col_name, target_path):
    store_to_hdfs(df=df, target_path=f"exp_result/{target_path}", column_name=col_name, format="parquet", mode="overwrite")


def store_agg_data_to_mongodb(agg_df: DataFrame, coll_name):
    store_df_to_mongodb(db_name=MONGO_AGG_DATA_DB, collection_name=coll_name, df=agg_df)


def store_vectors(vectorized_df: DataFrame, index_host, batch_size=400):
    print("Number of partitions: ", vectorized_df.rdd.getNumPartitions())

    store_to_pinecone(
        vectors_df=vectorized_df,
        host=index_host,
        batch_size=batch_size
    )