from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

from utilities.pinecone_utility import find_similar_objects


def find_similar_objects_udf(idx_host, top_k):
    """Creates a UDF for finding similar objects."""
    def inner_udf(id):
        result = find_similar_objects(id, idx_host, top_k)
        if result is None:
            return None
        else:
            return result

    return udf(inner_udf, ArrayType(StringType()))