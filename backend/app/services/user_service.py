import re
import time
import json

import pandas as pd
from datetime import datetime, timedelta

from backend.app.models.user_models import ProductDetails
from backend.config import MONGO_BROWSING_DB, MONGO_PRODUCTS_DB, client
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

    """def get_recently_viewed_products(self, user_id, limit=10):
        Get recently viewed products for a user.
        try:
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
            return []"""
    def get_recently_viewed_products(self, user_id, limit=10):
        """Get recently viewed products for a user."""
        try:
            browsing_collection = client[MONGO_BROWSING_DB]['browsing_history']

            # MongoDB'de filtrele, sıralayıp sınırla
            cursor = browsing_collection.find(
                {
                    'user_id': user_id,
                    'page_url': {'$regex': '/product/'}
                },
                {'_id': 0, 'page_url': 1, 'timestamp': 1}
            ).sort('timestamp', -1).limit(limit * 2)  # limit*2: to avoid for same product

            product_ids = []
            seen = set()

            for doc in cursor:
                match = re.search(r'/product/([^/?#]+)', doc['page_url'])
                if match:
                    pid = match.group(1)
                    if pid not in seen:
                        seen.add(pid)
                        product_ids.append(pid)
                if len(product_ids) >= limit:
                    break

            # Ürün detaylarını çek
            products = []
            for prod_id in product_ids:
                product = client[MONGO_PRODUCTS_DB]['products'].find_one(
                    {'product_id': prod_id}, {'_id': 0}
                )
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

            #products_collection = client[MONGO_PRODUCTS_DB]['products']
            
            # Combine order and product information
            products = []
            for order in orders:
                order_id, product_id, quantity, order_date, total_amount, review_id, status = order

                # Get product details from MongoDB
                product = ProductDetails.objects(product_id=product_id).first()
                if product:
                    # Add order details to product info
                    product_data = product.to_mongo().to_dict()
                    product_data['purchase_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(order_date / 1000))
                    product_data['quantity'] = quantity
                    product_data['total_price'] = float(total_amount)
                    product_data['has_review'] = review_id is not None
                    product_data['status'] = status
                    products.append(product_data)

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
                #product = products_collection.find_one({'product_id': visit['_id']}, {'_id': 0})
                product = ProductDetails.objects(product_id=visit['_id']).first()
                if product:
                    product_data = product.to_mongo().to_dict()
                    product_data['last_visit_date'] = visit['last_visit'].strftime('%Y-%m-%d %H:%M:%S')
                    product_data['visit_count'] = visit['visit_count']
                    products.append(product_data)

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

    def get_user_reviews(self, user_id, date_range='all', sort_by='recent', page=1, per_page=12):
        """Get user reviews with filtering and pagination."""
        conn = None
        cursor = None
        try:
            # Connect to PostgreSQL
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Base query parts
            select_clause = """
                SELECT r.review_id, r.product_id, r.rating, r.comment, r.review_date,
                       o.order_id, o.quantity, o.total_amount
                FROM product_reviews r
                LEFT JOIN orders o ON r.product_id = o.product_id AND r.user_id = o.user_id
            """
            where_clause = "WHERE r.user_id = %s"
            params = [user_id]

            # Apply date range filter
            if date_range != 'all':
                date_filter = {
                    'today': datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                    'week': datetime.now() - timedelta(days=7),
                    'month': datetime.now() - timedelta(days=30)
                }
                if date_range in date_filter:
                    where_clause += " AND r.review_date >= %s"
                    params.append(date_filter[date_range])

            # Apply sorting
            order_clause = ""
            if sort_by == 'recent':
                order_clause = "ORDER BY r.review_date DESC"
            elif sort_by == 'rating-high':
                order_clause = "ORDER BY r.rating DESC"
            elif sort_by == 'rating-low':
                order_clause = "ORDER BY r.rating ASC"

            # Add pagination
            limit_clause = "LIMIT %s OFFSET %s"
            params.extend([per_page, (page - 1) * per_page])

            # Combine query
            query = f"{select_clause} {where_clause} {order_clause} {limit_clause}"
            
            # Execute query
            cursor.execute(query, params)
            reviews = cursor.fetchall()

            # Get product details and combine with review information
            result_reviews = []
            for review in reviews:
                review_id, product_id, rating, comment, review_date, order_id, quantity, total_amount = review

                # Get product details from MongoDB
                product = ProductDetails.objects(product_id=product_id).first()
                if product:
                    # Add review details to product info
                    review_data = product.to_mongo().to_dict()
                    review_data.update({
                        'review_id': review_id,
                        'rating': rating,
                        'comment': comment,
                        'review_date': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(review_date / 1000)),
                        'order_id': order_id,
                        'quantity': quantity,
                        'total_amount': float(total_amount) if total_amount else None
                    })
                    result_reviews.append(review_data)

            # Get total count for pagination
            count_query = f"SELECT COUNT(*) FROM product_reviews r {where_clause}"
            cursor.execute(count_query, [user_id])
            total_count = cursor.fetchone()[0]

            return {'reviews': result_reviews, 'total': total_count}

        except Exception as e:
            print(f"Error getting user reviews: {str(e)}")
            return {'reviews': [], 'total': 0}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    def get_user_settings(self, user_id):
        """Get all user settings and preferences."""
        conn = None
        cursor = None
        try:
            # Connect to PostgreSQL
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Get user profile information
            profile_query = """
                SELECT u.email, u.first_name, u.last_name, u.gender, u.birthdate, u.address,
                       u.phone_number, u.profile_picture, u.created_at,
                       u.notification_preferences, u.privacy_settings, u.language_preference,
                       u.currency_preference, u.timezone
                FROM users u
                WHERE u.user_id = %s
            """
            cursor.execute(profile_query, [user_id])
            profile = cursor.fetchone()

            if not profile:
                return {}

            # Convert profile data to dictionary
            settings = {
                'email': profile[0],
                'first_name': profile[1],
                'last_name': profile[2],
                'gender': profile[3],
                'birthdate': profile[4].strftime('%Y-%m-%d') if profile[4] else None,
                'address': profile[5],
                'phone_number': profile[6],
                'profile_picture': profile[7],
                'created_at': profile[8].strftime('%Y-%m-%d %H:%M:%S') if profile[8] else None,
                'notification_preferences': profile[9] or {},
                'privacy_settings': profile[10] or {},
                'language_preference': profile[11] or 'en',
                'currency_preference': profile[12] or 'USD',
                'timezone': profile[13] or 'UTC'
            }

            return settings

        except Exception as e:
            print(f"Error getting user settings: {str(e)}")
            return {}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    def update_user_settings(self, user_id, data):
        """Update user settings and preferences."""
        conn = None
        cursor = None
        try:
            # Connect to PostgreSQL
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Validate required fields
            required_fields = ['email', 'first_name', 'last_name']
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                return None, f"Missing required fields: {', '.join(missing_fields)}"

            # Validate email format
            if not re.match(r"[^@]+@[^@]+\.[^@]+", data['email']):
                return None, "Invalid email format"

            # Check if email is already taken by another user
            cursor.execute(
                "SELECT user_id FROM users WHERE email = %s AND user_id != %s",
                [data['email'], user_id]
            )
            if cursor.fetchone():
                return None, "Email is already taken"

            # Update user profile
            update_query = """
                UPDATE users
                SET email = %s,
                    first_name = %s,
                    last_name = %s,
                    gender = %s,
                    birthdate = %s,
                    address = %s,
                    phone_number = %s,
                    notification_preferences = %s,
                    privacy_settings = %s,
                    language_preference = %s,
                    currency_preference = %s,
                    timezone = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
                RETURNING user_id
            """
            
            cursor.execute(update_query, [
                data['email'],
                data['first_name'],
                data['last_name'],
                data.get('gender'),
                data.get('birthdate'),
                data.get('address'),
                data.get('phone_number'),
                json.dumps(data.get('notification_preferences', {})),
                json.dumps(data.get('privacy_settings', {})),
                data.get('language_preference', 'en'),
                data.get('currency_preference', 'USD'),
                data.get('timezone', 'UTC'),
                user_id
            ])

            # Handle profile picture update if provided
            if 'profile_picture' in data and data['profile_picture']:
                # Here you would typically handle file upload and storage
                # For now, we'll just update the URL
                cursor.execute(
                    "UPDATE users SET profile_picture = %s WHERE user_id = %s",
                    [data['profile_picture'], user_id]
                )

            conn.commit()
            return True, None

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error updating user settings: {str(e)}")
            return None, str(e)
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)