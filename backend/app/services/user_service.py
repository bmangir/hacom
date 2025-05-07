import time

import pandas as pd
from datetime import datetime, timedelta

from backend.config.config import MONGO_BROWSING_DB, MONGO_PRODUCTS_DB
from databases.mongo.mongo_connector import MongoConnector
from databases.postgres.neon_postgres_connector import NeonPostgresConnector


class UserService:

    def get_user_first_name(self, user_id):
        """Get the first name of a user."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            
            query = "SELECT first_name FROM users WHERE user_id = %s"
            cursor.execute(query, [user_id])
            result = cursor.fetchone()
            
            return result[0] if result else None

        except Exception as e:
            print(f"Error getting user first name: {str(e)}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

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
                #product = get_product_details(prod_id)
                product = client[MONGO_PRODUCTS_DB]['products'].find_one({'product_id': prod_id}, {'_id': 0})
                if product:
                    products.append(product)

            return products
        except Exception as e:
            print(f"Error getting recently viewed products: {str(e)}")
            return []

    def get_purchased_products(self, user_id, date_range='all', sort_by='recent', page=1, per_page=12):
        """Get purchased products with filtering and pagination."""
        conn = None
        cursor = None
        try:
            # Connect to PostgreSQL for orders
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Base query parts
            select_clause = """
                SELECT o.order_id, o.product_id, o.quantity, o.order_date, o.total_amount,
                       r.review_id, status
                FROM orders o
                LEFT JOIN product_reviews r ON o.product_id = r.product_id AND o.user_id = r.user_id
            """
            where_clause = "WHERE o.user_id = %s"
            params = [user_id]

            # Apply date range filter
            if date_range != 'all':
                date_filter = {
                    'today': datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                    'week': datetime.now() - timedelta(days=7),
                    'month': datetime.now() - timedelta(days=30)
                }
                if date_range in date_filter:
                    where_clause += " AND o.order_date >= %s"
                    params.append(date_filter[date_range])

            # Apply sorting
            order_clause = ""
            if sort_by == 'recent':
                order_clause = "ORDER BY o.order_date DESC"
            elif sort_by == 'price-high':
                order_clause = "ORDER BY o.total_amount DESC"
            elif sort_by == 'price-low':
                order_clause = "ORDER BY o.total_amount ASC"
            elif sort_by == 'name':
                order_clause = "ORDER BY o.product_id"  # We'll sort by name after getting product details

            # Add pagination
            limit_clause = "LIMIT %s OFFSET %s"
            params.extend([per_page, (page - 1) * per_page])

            # Combine query
            query = f"{select_clause} {where_clause} {order_clause} {limit_clause}"
            
            # Execute query
            cursor.execute(query, params)
            orders = cursor.fetchall()

            # Get product details from MongoDB
            mongo_conn = MongoConnector()
            client = mongo_conn.get_client()
            if not client:
                print("MongoDB connection failed")
                return {'products': [], 'total': 0}

            products_collection = client[MONGO_PRODUCTS_DB]['products']
            
            # Combine order and product information
            products = []
            for order in orders:
                order_id, product_id, quantity, order_date, total_amount, review_id, status = order
                
                # Get product details from MongoDB
                product = products_collection.find_one({'product_id': product_id}, {'_id': 0})
                if product:
                    # Add order details to product info
                    product['purchase_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(order_date / 1000))
                    product['quantity'] = quantity
                    product['total_price'] = float(total_amount)
                    product['has_review'] = review_id is not None
                    product['status'] = status
                    products.append(product)

            # Apply name sorting if needed
            if sort_by == 'name':
                products.sort(key=lambda x: x.get('product_name', ''))

            # Get total count for pagination
            count_query = f"SELECT COUNT(DISTINCT order_id) FROM orders o {where_clause}"
            cursor.execute(count_query, [user_id])
            total_count = cursor.fetchone()[0]

            return {'products': products, 'total': total_count}

        except Exception as e:
            print(f"Error getting purchased products: {str(e)}")
            return {'products': [], 'total': 0}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    def get_total_purchased_products(self, user_id, date_range='all'):
        """Get total count of purchased products for pagination."""
        conn = None
        cursor = None
        try:
            # Connect to PostgreSQL
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Base query
            query = "SELECT COUNT(DISTINCT order_id) FROM orders WHERE user_id = %s"
            params = [user_id]

            # Apply date range filter
            if date_range != 'all':
                date_filter = {
                    'today': datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                    'week': datetime.now() - timedelta(days=7),
                    'month': datetime.now() - timedelta(days=30)
                }
                if date_range in date_filter:
                    query += " AND order_date >= %s"
                    params.append(date_filter[date_range])

            # Execute query
            cursor.execute(query, params)
            total = cursor.fetchone()[0]
            return total

        except Exception as e:
            print(f"Error getting total purchased products: {str(e)}")
            return 0
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    def get_visited_products(self, user_id, date_range='all', sort_by='recent', page=1, per_page=12):
        """Get visited products with filtering and pagination."""
        try:
            # Connect to MongoDB
            mongo_conn = MongoConnector()
            client = mongo_conn.get_client()
            if not client:
                print("MongoDB connection failed")
                return {'products': [], 'total': 0}

            browsing_collection = client[MONGO_BROWSING_DB]['browsing_history']
            products_collection = client[MONGO_PRODUCTS_DB]['products']

            # Base pipeline for aggregation
            pipeline = [
                {'$match': {'user_id': user_id, 'page_url': {'$regex': '/product/'}}},
                # Extract product_id from page_url
                {'$addFields': {
                    'product_id': {'$arrayElemAt': [{'$split': ['$page_url', '/']}, -1]}
                }},
                # Group by product_id to get latest visit
                {'$group': {
                    '_id': '$product_id',
                    'last_visit': {'$max': '$timestamp'},
                    'visit_count': {'$sum': 1}
                }}
            ]

            # Apply date range filter
            if date_range != 'all':
                date_filter = {
                    'today': datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                    'week': datetime.now() - timedelta(days=7),
                    'month': datetime.now() - timedelta(days=30)
                }
                if date_range in date_filter:
                    filter_timestamp = date_filter[date_range].timestamp()
                    pipeline[0]['$match']['timestamp'] = {'$gte': filter_timestamp}

            # Get total count before pagination
            count_pipeline = pipeline.copy()
            count_pipeline.append({'$count': 'total'})
            total_count_result = list(browsing_collection.aggregate(count_pipeline))
            total_count = total_count_result[0]['total'] if total_count_result else 0

            # Apply sorting
            sort_field = {
                'recent': {'last_visit': -1},
                'name': {'product_name': 1},
                'price-high': {'price': -1},
                'price-low': {'price': 1}
            }.get(sort_by, {'last_visit': -1})

            # Add pagination
            pipeline.extend([
                {'$sort': sort_field},
                {'$skip': (page - 1) * per_page},
                {'$limit': per_page}
            ])

            # Get product visits
            visits = list(browsing_collection.aggregate(pipeline))

            # Get product details
            products = []
            for visit in visits:
                product = products_collection.find_one({'product_id': visit['_id']}, {'_id': 0})
                if product:
                    product['last_visit_date'] = visit['last_visit'].strftime('%Y-%m-%d %H:%M:%S')
                    product['visit_count'] = visit['visit_count']
                    products.append(product)

            # Apply name sorting if needed (since it requires product details)
            if sort_by == 'name':
                products.sort(key=lambda x: x.get('product_name', ''))
            elif sort_by == 'price-high':
                products.sort(key=lambda x: float(x.get('price', 0)), reverse=True)
            elif sort_by == 'price-low':
                products.sort(key=lambda x: float(x.get('price', 0)))

            return {'products': products, 'total': total_count}

        except Exception as e:
            print(f"Error getting visited products: {str(e)}")
            return {'products': [], 'total': 0}

    def get_total_visited_products(self, user_id, date_range='all'):
        """Get total count of visited products for pagination."""
        try:
            # Connect to MongoDB
            mongo_conn = MongoConnector()
            client = mongo_conn.get_client()
            if not client:
                print("MongoDB connection failed")
                return 0

            browsing_collection = client[MONGO_BROWSING_DB]['browsing_history']

            # Base query
            query = {'user_id': user_id, 'page_url': {'$regex': '/product/'}}

            # Apply date range filter
            if date_range != 'all':
                date_filter = {
                    'today': datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                    'week': datetime.now() - timedelta(days=7),
                    'month': datetime.now() - timedelta(days=30)
                }
                if date_range in date_filter:
                    query['timestamp'] = {'$gte': date_filter[date_range].timestamp()}

            # Get total count
            total = browsing_collection.count_documents(query)
            return total

        except Exception as e:
            print(f"Error getting total visited products: {str(e)}")
            return 0