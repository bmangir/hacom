import traceback

from pinecone import Pinecone
from pyspark.sql.dataframe import DataFrame

from config import PINECONE_API_KEY


def store_to_pinecone(vectors_df: DataFrame, host, index_name, dimension, metric="cosine", batch_size=400):
    def upsert_batch(batch_list):
        """
        Function to upsert a batch of vectors into Pinecone index.
        This function will run in a separate thread.
        """
        try:
            # Initialize Pinecone client inside the thread
            #pinecone_params = pinecone_params_bc.value
            pinecone = Pinecone(api_key=PINECONE_API_KEY)
            index = pinecone.Index(host=host)
            #index = get_or_create_pinecone_index(pinecone=pinecone,
            #                                     index_name=pinecone_params['index_name'],
            #                                     dimension=pinecone_params['dimension'],
            #                                     metric=pinecone_params['metric'])
            index.upsert(vectors=batch_list)
            print(f"Upserted {len(batch_list)} vectors")
        except Exception as e:
            print(f"Pinecone upsert error in thread: {e}")

    def func(iterable):
        """
        Function to process each partition of data and create batches for upserting.
        It will use multiple threads to process the data in parallel.
        """
        pinecone = Pinecone(api_key=PINECONE_API_KEY)
        index = pinecone.Index(host=host)
        batch_list = []
        for row in iterable:
            vector_dict = row.asDict()
            batch_list.append(vector_dict)

            if len(batch_list) >= batch_size:
                try:
                    index.upsert(vectors=batch_list)
                    print(f"Upserted {len(batch_list)} vectors")
                except Exception as e:
                    print(f"Pinecone upsert error in thread: {e}")

                batch_list = []  # Reset batch list after submission
        # Handle remaining batch
        if batch_list:
            upsert_batch(batch_list)  # Submit remaining batch for upsert

    try:
        vectors_df.foreachPartition(func)
        print("-")
    except Exception as e:
        traceback.format_exc()
        print(e.__class__.__name__)
        print(e)
        print("--")


def retrieve_vector(id, idx_host_name):
    # TODO: Get old vector from Pinecone
    pinecone = Pinecone(api_key=PINECONE_API_KEY)
    index = pinecone.Index(host=idx_host_name)

    old_vector = index.fetch([id])["vectors"][id]["values"]

    return old_vector