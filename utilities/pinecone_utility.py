import json
import traceback

from pinecone import Pinecone
from pyspark.sql.dataframe import DataFrame

from config import PINECONE_API_KEY


MAX_BYTES = 4 * 1024 * 1024  # 4MB
SAFETY_MARGIN = 0.9  # Use 90% of limit to avoid edge overflow

def get_vector_size_bytes(vector_dict):
    """Rough estimate of payload size when serialized to JSON."""
    return len(json.dumps(vector_dict).encode("utf-8"))

def store_to_pinecone(vectors_df: DataFrame, host, batch_size=400):
    def func(iterable):
        """
        Function to process each partition of data and create batches for upserting.
        It will use multiple threads to process the data in parallel.
        """
        pinecone = Pinecone(api_key=PINECONE_API_KEY)
        index = pinecone.Index(host=host)
        batch_list = []
        current_size = 0

        for row in iterable:
            vector_dict = row.asDict()
            item_size = get_vector_size_bytes(vector_dict)

            if current_size + item_size > MAX_BYTES * SAFETY_MARGIN:
                try:
                    print(f"Upserting batch of size {len(batch_list)}, approx size: {current_size} bytes")
                    index.upsert(vectors=batch_list)
                except Exception as e:
                    print(f"Pinecone upsert error: {e}")
                batch_list = []
                current_size = 0

            batch_list.append(vector_dict)
            current_size += item_size

        # Submit final batch
        if batch_list:
            try:
                print(f"Final upsert of {len(batch_list)} vectors, approx size: {current_size} bytes")
                index.upsert(vectors=batch_list)
            except Exception as e:
                print(f"Pinecone final batch error: {e}")

    try:
        vectors_df.foreachPartition(func)
    except Exception as e:
        print(e)
        print()


def retrieve_vector(id, idx_host_name):
    # TODO: Get old vector from Pinecone
    pinecone = Pinecone(api_key=PINECONE_API_KEY)
    index = pinecone.Index(host=idx_host_name)

    old_vector = index.fetch([id])["vectors"][id]["values"]

    return old_vector


def find_similar_objects(id, idx_host, top_k=5):
    pinecone = Pinecone(api_key=PINECONE_API_KEY)
    index = pinecone.Index(host=idx_host, pool_threads=50)  # Use the same index name as in store_to_pinecone(
    try:
        vector = index.fetch(ids=[id])
        if id not in vector["vectors"]:
            print(f"ID {id} not found in Pinecone.")
            return None

        query_vector = vector["vectors"][id]["values"]

        response = index.query(
            vector=query_vector,
            top_k=top_k,  # Get top_k most similar users
        )
        similar_ids = []
        for match in response["matches"]:
            similar_ids.append(match["id"])

        return similar_ids

    except Exception as e:
        print(f"Error finding similar users: {e}")
        return None