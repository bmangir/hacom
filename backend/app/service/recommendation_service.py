import re
import time
from datetime import datetime, timedelta

import pandas as pd
import json
import os
import pickle

from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity

from databases.mongo.mongo_connector import MongoConnector
from backend.config.config import MONGO_BROWSING_DB, MONGO_PRODUCTS_DB, MONGO_RECCOMENDATION_DB
from databases.postgres.neon_postgres_connector import NeonPostgresConnector


class RecommendationService:
    _instance = None
    _cache_duration = timedelta(minutes=5)  # Changed from hours=1 to seconds=0
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RecommendationService, cls).__new__(cls)
            cls._instance._initialized = False
            cls._instance._last_fetch_time = None
        return cls._instance
        
    def __init__(self):
        if self._initialized:
            return
            
        # Initialize empty DataFrames and dictionaries
        self.orders_df = None
        self.reviews_df = None
        self.products_df = None
        self.clickstream_df = None
        self.browsing_df = None
        self.user_data = None
        self.product_data = {}  # Initialize empty product data dictionary
        
        # Initialize data only once
        self._fetch_data_from_db()
        self._initialized = True

    def _should_refresh_data(self):
        """Check if data should be refreshed based on cache duration."""
        if not self._last_fetch_time:
            return True
        return datetime.now() - self._last_fetch_time > self._cache_duration

    def _fetch_data_from_db(self):
        """Fetch data from database only if cache is expired."""
        if not self._should_refresh_data() and self.products_df is not None:
            return  # Use cached data if it's still valid

        print("Fetching fresh data from database...")  # Debug log
        conn = None
        try:
            # Initialize MongoDB connection
            mongo_conn = MongoConnector()
            client = mongo_conn.get_client()
            if not client:
                print("MongoDB connection failed")
                return

            # PostgreSQL connection
            conn = NeonPostgresConnector.get_connection()
            if not conn:
                print("PostgreSQL connection failed")
                return

            # PostgreSQL queries
            orders_query = "SELECT * FROM orders;"
            self.orders_df = pd.read_sql_query(orders_query, conn)

            reviews_query = "SELECT * FROM product_reviews;"
            self.reviews_df = pd.read_sql_query(reviews_query, conn)

            user_query = "SELECT * FROM users;"
            self.user_data = pd.read_sql_query(user_query, conn)

            # MongoDB queries
            products_collection = client[MONGO_PRODUCTS_DB]['products']
            self.products_df = pd.DataFrame(list(products_collection.find({}, {'_id': 0})))

            # Create product_data dictionary from products_df
            self.product_data = self.products_df.set_index('product_id').to_dict('index')

            clickstream_collection = client[MONGO_BROWSING_DB]['clickstream']
            self.clickstream_df = pd.DataFrame(list(clickstream_collection.find({}, {'_id': 0})))

            browsing_collection = client[MONGO_BROWSING_DB]['browsing_history']
            self.browsing_df = pd.DataFrame(list(browsing_collection.find({}, {'_id': 0})))

            self._add_product_id_to_browsing_history()
            self._add_product_id_to_clickstream()

            # Update last fetch time
            self._last_fetch_time = datetime.now()

        except Exception as e:
            print(f"Error occurred in _fetch_data_from_db: {e}")
        finally:
            if conn:
                NeonPostgresConnector.return_connection(conn)

    def refresh_data(self):
        """Manually refresh the data cache."""
        self._last_fetch_time = None
        self._fetch_data_from_db()

    def _calculate_user_similarity(self):
        """Calculate user similarity based on their interactions using Pearson correlation."""
        numerical_columns = ['avg_rating', 'order_frequency', 'total_spending', 'total_browsing_time', 'total_interactions']
        pearson_user_features_matrix = self.user_features.set_index('user_id')[numerical_columns]
        pearson_similarity_matrix = pearson_user_features_matrix.T.corr(method='pearson')

        # Convert to a DataFrame
        pearson_similarity_df = pd.DataFrame(pearson_similarity_matrix, index=self.user_features['user_id'], columns=self.user_features['user_id'])
        
        return pearson_similarity_df

    def find_similar_users(self, user_id, top_n=5):
        """Find top-n users similar to the given user_id."""
        #if user_id not in self.similarity_df.index:
        #    return pd.Series()

        # Average rating per user
        user_avg_ratings = self.reviews_df.groupby('user_id')['rating'].mean().reset_index()
        user_avg_ratings.rename(columns={'rating': 'avg_rating'}, inplace=True)
        # Order frequency and total spending
        user_orders = self.orders_df.groupby('user_id').agg(
            order_frequency=('order_id', 'count'),
            total_spending=('quantity', lambda x: (self.orders_df.loc[x.index, 'quantity'] * self.orders_df.loc[x.index, 'unit_price']).sum())
        ).reset_index()
        # Total browsing time per user
        user_browsing_time = self.browsing_df.groupby('user_id')['view_duration'].sum().reset_index()
        user_browsing_time.rename(columns={'view_duration': 'total_browsing_time'}, inplace=True)
        # Total interactions and most common action
        user_clickstream = self.clickstream_df.groupby('user_id').agg(
            total_interactions=('action', 'count'),
            most_common_action=('action', lambda x: x.mode()[0] if not x.mode().empty else None)
        ).reset_index()
        # Merge all features into a single DataFrame
        user_features = pd.merge(user_avg_ratings, user_orders, on='user_id', how='outer')
        user_features = pd.merge(user_features, user_browsing_time, on='user_id', how='outer')
        user_features = pd.merge(user_features, user_clickstream, on='user_id', how='outer')

        # Fill missing values with 0 for numeric columns
        user_features.fillna({'avg_rating': 0, 'order_frequency': 0, 'total_spending': 0, 'total_browsing_time': 0, 'total_interactions': 0}, inplace=True)

        # Handle missing categorical values
        user_features['most_common_action'].fillna('unknown', inplace=True)
        numerical_columns = ['avg_rating', 'order_frequency', 'total_spending', 'total_browsing_time', 'total_interactions']
        scaler = MinMaxScaler()
        user_features[numerical_columns] = scaler.fit_transform(user_features[numerical_columns])

        # Prepare feature matrix for similarity computation
        cosine_user_features_matrix = user_features[numerical_columns]

        # Compute cosine similarity
        cosine_similarity_matrix = cosine_similarity(cosine_user_features_matrix)
        cosine_similarity_df = pd.DataFrame(cosine_similarity_matrix, index=user_features['user_id'], columns=user_features['user_id'])

        similar_users = cosine_similarity_df[user_id].sort_values(ascending=False).iloc[1:top_n+1]  # Exclude self
        #similar_users = self.similarity_df.loc[user_id].sort_values(ascending=False)[1:top_n+1]
        return similar_users

    def recommend_items(self, user_id, top_n=5):
        """
        Recommend items to the given user based on similar users.
        
        The recommendation system uses a hybrid approach:
        1. First tries collaborative filtering based on user similarity
        2. Falls back to popularity-based recommendations if no recommendations found
        
        Args:
            user_id (str): The ID of the user to get recommendations for
            top_n (int): Number of recommendations to return (default: 5)
            
        Returns:
            list: List of recommended product dictionaries with details
        """

        self._fetch_data_from_db()
        try:
            # Find similar users using collaborative filtering
            similar_users = self.find_similar_users(user_id, top_n=5).index.tolist()
            print(similar_users)

            # Get products interacted by similar users from different sources
            similar_users_orders = self.orders_df[self.orders_df['user_id'].isin(similar_users)]
            similar_users_reviews = self.reviews_df[self.reviews_df['user_id'].isin(similar_users)]
            similar_users_browsing = self.browsing_df[self.browsing_df['user_id'].isin(similar_users)]

            # Combine all product interactions to get a comprehensive view
            similar_users_products = pd.concat([
                similar_users_orders[['product_id']],
                similar_users_reviews[['product_id']],
                similar_users_browsing[['product_id']]
            ])

            # Get unique products to avoid duplicates
            similar_users_products = similar_users_products['product_id'].unique()

            # Get products the target user has already interacted with to exclude them
            user_orders = self.orders_df[self.orders_df['user_id'] == user_id]['product_id'].tolist()
            user_reviews = self.reviews_df[self.reviews_df['user_id'] == user_id]['product_id'].tolist()
            user_browsing = self.browsing_df[self.browsing_df['user_id'] == user_id]['product_id'].tolist()

            user_interacted_products = set(user_orders + user_reviews + user_browsing)

            # Filter out products the user has already interacted with
            recommendations = [product for product in similar_users_products if product not in user_interacted_products]
            
            # If no recommendations found, fall back to popularity-based recommendations
            if not recommendations:
                recommendations = self._get_popular_products(user_interacted_products)
            
            # Get detailed information for recommended products
            recommended_products = []
            for product_id in recommendations[:top_n]:
                product_info = self.products_df[self.products_df['product_id'] == product_id].iloc[0]
                category = product_info['category']
                recommended_products.append({
                    'id': product_info['product_id'],
                    'name': product_info['product_name'],
                    'category': category,
                    'price': float(product_info['price']),
                    'stock_quantity': int(product_info['stock_quantity']),
                    'brand': product_info['brand'],
                    'rating': float(product_info['rating']) if 'rating' in product_info else None,
                    'image_url': self._get_product_image(category)
                })

            return recommended_products

        except Exception as e:
            print(f"Error generating recommendations: {str(e)}")
            return []

    def _add_product_id_to_browsing_history(self):
        # Regex pattern to extract product_id from the URL (assuming product_id starts with P followed by numbers)
        pattern = r"/products/details/(P\d+)"

        # Apply regex to extract product_id from the page_url column
        self.browsing_df['product_id'] = self.browsing_df['page_url'].apply(
            lambda url: re.search(pattern, url).group(1) if re.search(pattern, url) else None
        )

        # Drop rows where product_id could not be extracted (optional, can be kept based on need)
        #self.browsing_df = self.browsing_df.dropna(subset=['product_id'])

    def _add_product_id_to_clickstream(self):
        # Regex pattern to extract product_id from the URL (assuming product_id starts with P followed by numbers)
        pattern = r"/products/details/(P\d+)"

        self.clickstream_df['product_id'] = self.clickstream_df['page_url'].apply(
            lambda url: re.search(pattern, url).group(1) if re.search(pattern, url) else None
        )

        return self.clickstream_df

    def _get_popular_products(self, excluded_products):
        """
        Get popular products based on order status and visit weights.
        
        This is a fallback recommendation system that uses a weighted scoring approach:
        
        Weights:
        1. Order Status Weights (Total: 0.6 or 60%):
           - Completed orders: 0.4 (40%) - Highest weight as these represent successful purchases
           - Shipped orders: 0.1 (10%) - Lower weight as they're in progress
           - Pending orders: 0.1 (10%) - Lower weight as they might not complete
           
        2. Visit Weight (0.4 or 40%):
           - Product visits/views: 0.4 (40%) - High weight as it shows user interest
           
        The final score for each product is calculated as:
        score = (completed_orders * 0.4) + (shipped_orders * 0.1) + 
                (pending_orders * 0.1) + (visits * 0.4)
        
        Args:
            excluded_products (set): Set of product IDs to exclude from recommendations
            
        Returns:
            list: List of product IDs sorted by their popularity score
        """
        try:
            # Define weights for different order statuses (total: 0.6 or 60%)
            order_weights = {
                'Delivered': 0.4,  # 40% weight for completed orders
                'Shipped': 0.1,    # 10% weight for shipped orders
                'Pending': 0.1     # 10% weight for pending orders
            }
            
            # Initialize DataFrame for order-based scores
            order_scores = pd.DataFrame()
            
            # Calculate weighted scores for each order status
            for status, weight in order_weights.items():
                status_orders = self.orders_df[self.orders_df['status'] == status]
                status_counts = status_orders['product_id'].value_counts()
                status_scores = status_counts * weight
                
                # Add scores to the DataFrame
                if order_scores.empty:
                    order_scores = pd.DataFrame(status_scores)
                    order_scores.columns = ['score']
                else:
                    order_scores['score'] = order_scores['score'].add(status_scores, fill_value=0)
            
            # Calculate visit-based popularity (weight: 0.4 or 40%)
            visit_counts = self.browsing_df['product_id'].value_counts()
            visit_scores = visit_counts * 0.4
            
            # Combine order-based and visit-based scores
            if order_scores.empty:
                final_scores = pd.DataFrame(visit_scores)
                final_scores.columns = ['score']
            else:
                final_scores = order_scores.copy()
                final_scores['score'] = final_scores['score'].add(visit_scores, fill_value=0)
            
            # Sort by final score and exclude already interacted products
            final_scores = final_scores.sort_values('score', ascending=False)
            recommended_products = [
                product_id for product_id in final_scores.index 
                if product_id not in excluded_products
            ]
            
            return recommended_products
            
        except Exception as e:
            print(f"Error getting popular products: {str(e)}")
            return []

    def get_user_profile(self, user_id):
        """Get user profile information."""
        try:
            return self.user_data[self.user_data['user_id'] == user_id].iloc[0].to_dict()
        except Exception as e:
            print(f"Error getting user profile: {str(e)}")
            return None 

    def get_most_visited_categories(self, user_id, top_n=5):
        """Get the most visited categories for a user based on their interactions."""
        # Ensure data is loaded
        self._fetch_data_from_db()  # This will only fetch if cache is expired
        
        try:
            # Get all product interactions for the user
            user_orders = self.orders_df[self.orders_df['user_id'] == user_id]['product_id'].tolist()
            user_browsing = self.browsing_df[self.browsing_df['user_id'] == user_id]['product_id'].tolist()
            user_clicks = self.clickstream_df[self.clickstream_df['user_id'] == user_id]['product_id'].tolist()
            
            # Combine all product interactions
            all_interactions = user_orders + user_browsing + user_clicks
            
            # Get categories from products_df instead of product_data
            product_categories = self.products_df[self.products_df['product_id'].isin(all_interactions)]['category']
            
            # Count category frequencies
            category_counts = product_categories.value_counts()
            
            # Get top N categories
            top_categories = category_counts.head(top_n).index.tolist()
            
            return top_categories
            
        except Exception as e:
            print(f"Error getting most visited categories: {str(e)}")
            return []

    def _get_product_image(self, category=None):
        """Return default product image path."""
        return '/static/images/default-product.jpg'

    def get_product_details(self, product_id):
        """Get detailed information for a specific product."""
        try:
            product_info = self.products_df[self.products_df['product_id'] == product_id].iloc[0]
            category = product_info['category']
            
            # Fetch reviews for the product
            reviews = self.reviews_df[self.reviews_df['product_id'] == product_id].to_dict(orient='records')
            
            # Format reviews to include username and review text
            formatted_reviews = []
            for review in reviews:
                formatted_reviews.append({
                    'username': review['user_id'],  # Assuming user_id is used as username
                    'comment': review['review_text'],
                    'rating': review['rating'],
                    'date': review['review_date'],
                    'helpful_count': review['review_helpful_count']
                })
            
            return {
                'id': product_info['product_id'],
                'name': product_info['product_name'],
                'merchant_id': product_info['merchant_id'],
                'category': category,
                'price': float(product_info['price']),
                'stock_quantity': int(product_info['stock_quantity']),
                'brand': product_info['brand'],
                'rating': float(product_info['rating']) if 'rating' in product_info else None,
                'image_url': self._get_product_image(category),
                'reviews': formatted_reviews  # Add formatted reviews to the returned product details
            }
        except Exception as e:
            print(f"Error getting product details: {str(e)}")
            return None 

    def search_products(self, query):
        """Search products in DataFrame based on multiple fields."""
        try:
            query = query.lower()
            
            # Convert columns to string type
            str_columns = ['product_name', 'brand', 'category', 'material', 
                         'gender', 'color', 'author', 'language', 'format']
             
            df_copy = self.products_df.copy()
            for col in str_columns:
                if col in df_copy.columns:
                    df_copy[col] = df_copy[col].astype(str)
             
            # Create mask for each searchable field
            name_mask = df_copy['product_name'].str.lower().str.contains(query, na=False)
            brand_mask = df_copy['brand'].str.lower().str.contains(query, na=False)
            category_mask = df_copy['category'].str.lower().str.contains(query, na=False)
            material_mask = df_copy['material'].str.lower().str.contains(query, na=False)
            gender_mask = df_copy['gender'].str.lower().str.contains(query, na=False)
            color_mask = df_copy['color'].str.lower().str.contains(query, na=False)
            author_mask = df_copy['author'].str.lower().str.contains(query, na=False)
            language_mask = df_copy['language'].str.lower().str.contains(query, na=False)
            format_mask = df_copy['format'].str.lower().str.contains(query, na=False)
             
            # Search in lists (tags, details, sizes)
            tags_mask = df_copy['tags'].apply(lambda x: query in [str(tag).lower() for tag in (x if isinstance(x, list) else [])])
            details_mask = df_copy['details'].apply(lambda x: query in [str(detail).lower() for detail in (x if isinstance(x, list) else [])])
            sizes_mask = df_copy['size'].apply(lambda x: query in [str(size).lower() for size in (x if isinstance(x, list) else [])])
             
            # Combine all masks
            combined_mask = (
                name_mask | brand_mask | category_mask | material_mask | 
                gender_mask | color_mask | author_mask | language_mask | 
                format_mask | tags_mask | details_mask | sizes_mask
            )
             
            # Get matching products and sort by rating
            matching_products = df_copy[combined_mask].copy()
            matching_products['rating'] = pd.to_numeric(matching_products['rating'], errors='coerce')
            matching_products = matching_products.sort_values(by='rating', ascending=False).head(50)
             
            products = []
            for _, product in matching_products.iterrows():
                category = product['category']
                products.append({
                    'id': product['product_id'],
                    'name': product['product_name'],
                    'description': product.get('description', ''),
                    'price': float(product['price']),
                    'brand': product['brand'],
                    'image_url': self._get_product_image(category),
                    'category': category,
                    'material': product.get('material', ''),
                    'color': product.get('color', ''),
                    'rating': float(product['rating']) if pd.notnull(product['rating']) else None,
                    'tags': product.get('tags', []),
                    'sizes': product.get('sizes', [])
                })
                 
            return {"success": True, "products": products}
             
        except Exception as e:
            print(f"Error searching products: {str(e)}")
            return {"success": False, "message": str(e)} 

    def get_similar_products(self, product_id, limit=5):
        """Get similar products based on category and attributes."""
        try:
            product = self.products_df[self.products_df['product_id'] == product_id].iloc[0]
            category = product['category']
            
            # Get products in same category
            similar_products = self.products_df[
                (self.products_df['category'] == category) & 
                (self.products_df['product_id'] != product_id)
            ]
            
            # Sort by rating and get top N
            similar_products = similar_products.sort_values('rating', ascending=False).head(limit)
            
            return similar_products.to_dict('records')
            
        except Exception as e:
            print(f"Error getting similar products: {str(e)}")
            return [] 

    def get_merchant_details(self, merchant_id):
        """Get merchant details and their products."""
        try:
            # Get merchant's products
            merchant_products = self.products_df[
                self.products_df['merchant_id'] == merchant_id
            ]
            
            if merchant_products.empty:
                return None
            
            # Get merchant info (you'll need to add this to your data)
            merchant_info = {
                'merchant_id': merchant_id,
                'name': merchant_products.iloc[0].get('merchant_name', 'Unknown Merchant'),
                'products': merchant_products.to_dict('records')
            }
            
            return merchant_info
            
        except Exception as e:
            print(f"Error getting merchant details: {str(e)}")
            return None 

    def get_similar_products_by_category(self, category, product_id, limit=5):
        """Get similar products based on category."""
        try:
            # Get products in same category excluding current product
            similar_products = self.products_df[
                (self.products_df['category'] == category) & 
                (self.products_df['product_id'] != product_id)
            ]
            
            # Sort by rating and get top N
            similar_products = similar_products.sort_values('rating', ascending=False).head(limit)
            
            # Format products with default image
            products = []
            for _, product in similar_products.iterrows():
                products.append({
                    'id': product['product_id'],
                    'name': product['product_name'],
                    'price': float(product['price']),
                    'brand': product['brand'],
                    'rating': float(product['rating']) if pd.notnull(product['rating']) else None,
                    'image_url': '/static/images/default-product.jpg'
                })
            
            return products
            
        except Exception as e:
            print(f"Error getting category similar products: {str(e)}")
            return [] 

    def get_recently_viewed_products(self, user_id, limit=10):
        """Get recently viewed products for a user."""
        try:
            # Ensure data is loaded
            mongo_conn = MongoConnector()
            client = mongo_conn.get_client()
            if not client:
                print("MongoDB connection failed")
                return

            browsing_collection = client[MONGO_BROWSING_DB]['browsing_history']
            browsing_df = pd.DataFrame(list(browsing_collection.find({}, {'_id': 0})))
            
            # Get recent product views from browsing history
            recent_views = browsing_df[
                (browsing_df['user_id'] == user_id) &
                (browsing_df['page_url'].str.contains('/product/'))
            ].sort_values('timestamp', ascending=False)
            
            # Extract product IDs from URLs
            product_ids = recent_views['page_url'].str.extract(r'/product/(.+)')[0].unique()[:limit]
            
            # Get product details
            products = []
            for prod_id in product_ids:
                product = self.get_product_details(prod_id)
                if product:
                    products.append(product)
                    
            return products
        except Exception as e:
            print(f"Error getting recently viewed products: {str(e)}")
            return []

    def get_purchased_products(self, user_id, limit=10):
        """Get purchased products for a user."""
        try:
            # Ensure data is loaded
            # PostgreSQL connection
            conn = NeonPostgresConnector.get_connection()
            if not conn:
                print("PostgreSQL connection failed")
                return

            # PostgresSQL queries
            orders_query = "SELECT * FROM orders;"
            orders_df = pd.read_sql_query(orders_query, conn)
            
            # Get purchased products from orders
            recent_purchases = orders_df[
                orders_df['user_id'] == user_id
            ].sort_values('order_date', ascending=False)
            
            # Get unique product IDs from recent purchases
            product_ids = recent_purchases['product_id'].unique()[:limit]
            
            # Get product details
            products = []
            for prod_id in product_ids:
                product = self.get_product_details(prod_id)
                if product:
                    products.append(product)
                    
            return products
        except Exception as e:
            print(f"Error getting purchased products: {str(e)}")
            return [] 