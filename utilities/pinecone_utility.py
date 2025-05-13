import json
import traceback
from concurrent.futures import ThreadPoolExecutor
from pinecone import Pinecone
from pyspark.sql.dataframe import DataFrame
from config import NEW_PINECONE_API as PINECONE_API_KEY
import threading

MAX_BYTES = 4 * 1024 * 1024  # 4MB
SAFETY_MARGIN = 0.9  # Use 90% of limit to avoid edge overflow

# Connection pool for Pinecone
class PineconeConnectionPool:
    _instance = None
    _connections = {}
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(PineconeConnectionPool, cls).__new__(cls)
        return cls._instance
    
    def get_connection(self, host):
        with self._lock:
            if host not in self._connections:
                self._connections[host] = Pinecone(api_key=PINECONE_API_KEY).Index(host=host, pool_threads=50)
            return self._connections[host]

# Create a global connection pool instance
connection_pool = PineconeConnectionPool()

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

def find_similar_objects(id, idx_host, top_k=5):
    try:
        # Get connection from pool
        index = connection_pool.get_connection(idx_host)
        
        # Fetch vector
        vector = index.fetch(ids=[id])
        if id not in vector["vectors"]:
            print(f"ID {id} not found in Pinecone.")
            return None

        query_vector = vector["vectors"][id]["values"]

        # Query for similar objects
        response = index.query(
            vector=query_vector,
            top_k=top_k
        )

        similar_ids = [match["id"] for match in response["matches"]]
        return similar_ids

    except Exception as e:
        print(f"Error finding similar objects for {id}: {str(e)}")
        return None