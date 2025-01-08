import pandas as pd
import json
import os
import pickle
from databases.postgres.postgres_connector import PostgresConnector

class RecommendationService:
    def __init__(self):
        # Load the pre-trained models and data from model_for_user_based
        base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 
                                'databases/model_for_user_based')
        
        # Load user features
        with open(os.path.join(base_path, 'user_features.pkl'), 'rb') as f:
            self.user_features = pickle.load(f)
            
        # Load similarity matrix
        with open(os.path.join(base_path, 'similarity_df.pkl'), 'rb') as f:
            self.similarity_df = pickle.load(f)
            
        # Load product mapping
        with open(os.path.join(base_path, 'user_product_map.pkl'), 'rb') as f:
            self.product_data = pickle.load(f)
            
        # Load the ecommerce dataset
        with open(os.path.join(os.path.dirname(base_path), 'data/ecommerce_dataset.json'), 'r') as f:
            ecommerce_data = json.load(f)
            
        # Convert lists to DataFrames
        self.products_df = pd.DataFrame(ecommerce_data['products'])
        self.browsing_df = pd.DataFrame(ecommerce_data['browsing_history'])
        self.clickstream_df = pd.DataFrame(ecommerce_data['clickstream'])
        self.orders_df = pd.DataFrame(ecommerce_data['orders'])
        self.reviews_df = pd.DataFrame(ecommerce_data['reviews'])
        
        # Load user data
        with open(os.path.join(os.path.dirname(base_path), 'data/user_data.json'), 'r') as f:
            self.user_data = pd.DataFrame(json.load(f))

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
        if user_id not in self.similarity_df.index:
            return pd.Series()
        
        similar_users = self.similarity_df.loc[user_id].sort_values(ascending=False)[1:top_n+1]
        return similar_users

    def recommend_items(self, user_id, top_n=5):
        """Recommend items to the given user based on similar users."""
        try:
            user_id = "U11996"
            similar_users = self.find_similar_users(user_id, top_n=5).index.tolist()

            # Get products interacted by similar users
            similar_users_orders = self.orders_df[self.orders_df['user_id'].isin(similar_users)]
            similar_users_reviews = self.reviews_df[self.reviews_df['user_id'].isin(similar_users)]
            similar_users_browsing = self.browsing_df[self.browsing_df['user_id'].isin(similar_users)]

            # Combine product interactions from orders, reviews, and browsing history
            similar_users_products = pd.concat([
                similar_users_orders[['product_id']],
                similar_users_reviews[['product_id']],
                similar_users_browsing[['product_id']]
            ])

            # Get unique products interacted by similar users
            similar_users_products = similar_users_products['product_id'].unique()

            # Get products the target user has already interacted with
            user_orders = self.orders_df[self.orders_df['user_id'] == user_id]['product_id'].tolist()
            user_reviews = self.reviews_df[self.reviews_df['user_id'] == user_id]['product_id'].tolist()
            user_browsing = self.browsing_df[self.browsing_df['user_id'] == user_id]['product_id'].tolist()

            user_interacted_products = set(user_orders + user_reviews + user_browsing)

            # Filter out products the user has already interacted with
            recommendations = [product for product in similar_users_products if product not in user_interacted_products]
            
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

    def get_user_profile(self, user_id):
        """Get user profile information."""
        try:
            return self.user_data[self.user_data['user_id'] == user_id].iloc[0].to_dict()
        except Exception as e:
            print(f"Error getting user profile: {str(e)}")
            return None 

    def get_most_visited_categories(self, user_id, top_n=5):
        """Get the most visited categories for a user based on their interactions."""
        try:
            # Get all product interactions for the user
            user_orders = self.orders_df[self.orders_df['user_id'] == user_id]['product_id'].tolist()
            user_browsing = self.browsing_df[self.browsing_df['user_id'] == user_id]['product_id'].tolist()
            user_clicks = self.clickstream_df[self.clickstream_df['user_id'] == user_id]['product_id'].tolist()
            
            # Combine all product interactions
            all_interactions = user_orders + user_browsing + user_clicks
            
            # Get categories for all interacted products
            categories = []
            for prod_id in all_interactions:
                if prod_id in self.product_data:
                    category = self.product_data[prod_id].get('category')
                    if category:
                        categories.append(category)
            
            # Count category frequencies
            category_counts = pd.Series(categories).value_counts()
            
            # Get top N categories
            top_categories = category_counts.head(top_n).index.tolist()
            
            return top_categories
            
        except Exception as e:
            print(f"Error getting most visited categories: {str(e)}")
            return [] 

    def _get_product_image(self, category):
        """Get a relevant product image based on category."""
        return '/static/images/default-product.jpg'

    def get_product_details(self, product_id):
        """Get detailed information for a specific product."""
        try:
            product_info = self.products_df[self.products_df['product_id'] == product_id].iloc[0]
            category = product_info['category']
            return {
                'id': product_info['product_id'],
                'name': product_info['product_name'],
                'category': category,
                'price': float(product_info['price']),
                'stock_quantity': int(product_info['stock_quantity']),
                'brand': product_info['brand'],
                'rating': float(product_info['rating']) if 'rating' in product_info else None,
                'image_url': self._get_product_image(category)
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