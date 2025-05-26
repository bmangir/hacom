from pyspark.sql import SparkSession, DataFrame

from config import MONGO_AGG_DATA_DB, NEW_USER_FEATURES_HOST as USER_FEATURES_HOST
from spark_utility import extract_data, read_from_hdfs, store_df_to_mongodb, store_to_hdfs
from pinecone_utility import store_to_pinecone


def fetch_data(spark: SparkSession):
    return extract_data(spark=spark)


def fetch_data_from_hdfs(spark, base_path="exp_result/"):
    return read_from_hdfs(spark=spark, base_path=base_path+"user_features/")


def store_df_to_hdfs(df, col_name):
    store_to_hdfs(df=df, target_path="exp_result/user_features", column_name=col_name, format="parquet", mode="overwrite")


def store_agg_data_to_mongodb(agg_df: DataFrame):
    store_df_to_mongodb(db_name=MONGO_AGG_DATA_DB, collection_name="user_features", df=agg_df, mode="overwrite")


def store_vectors(vectorized_df: DataFrame):
    store_to_pinecone(
        vectors_df=vectorized_df,
        host=USER_FEATURES_HOST,
        batch_size=600
    )