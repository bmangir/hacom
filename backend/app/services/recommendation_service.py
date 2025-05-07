import itertools
import re
import threading
from collections import defaultdict

from flask import session
from datetime import datetime, timedelta
from functools import lru_cache

import numpy as np
import pandas as pd

from pinecone import Pinecone
from sentence_transformers import SentenceTransformer

from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity


class RecommendationService:
    def __init__(self, pinecone_index, products_df):
        pass

    def get_recommendations(self, user_id, num_recommendations=5):
        pass

    def _get_product_details(self, product_ids):
        """Get full product details for recommendation output"""
        recommended_products = []
        for pid in product_ids:
            product_info = self.products_df[self.products_df['product_id'] == pid].iloc[0]
            recommended_products.append({
                'id': product_info['product_id'],
                'name': product_info['product_name'],
                'category': product_info['category'],
                'price': float(product_info['price']),
                'stock_quantity': int(product_info['stock_quantity']),
                'brand': product_info['brand'],
                'rating': float(product_info['rating']) if 'rating' in product_info else None,
                'image_url': self._get_product_image(product_info['category']),
                'details': product_info['details'],
                'tags': product_info['tags'],
                'size': product_info['size'],
                'color': product_info['color'],
                'format': product_info['format'],
                'material': product_info['material'],
            })
        return recommended_products