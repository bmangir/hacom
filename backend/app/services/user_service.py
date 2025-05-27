import re
import time
import json

import pandas as pd
from datetime import datetime, timedelta

from backend.app.models.user_models import ProductDetails
from backend.config import MONGO_BROWSING_DB, MONGO_PRODUCTS_DB, client
from backend.utils.utils import _get_product_details
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
            browsing_collection = client[MONGO_BROWSING_DB]['browsing_history']

            cursor = browsing_collection.find(
                {
                    'user_id': user_id,
                    'page_url': {'$regex': '(/product/|/products/details/)'}
                },
                {'_id': 0, 'page_url': 1, 'timestamp': 1}
            ).sort('timestamp', -1).limit(limit * 2)  # limit*2: to avoid for same product

            product_ids = []
            seen = set()

            for doc in cursor:
                match = re.search(r'/product/([^/?#]+)|/products/details/([^/?#]+)', doc['page_url'])
                if match:
                    # Get the product ID from whichever group matched (first or second)
                    pid = match.group(1) if match.group(1) else match.group(2)
                    if pid not in seen:
                        if not (pid is None or pid == 'None'):
                            seen.add(pid)
                            product_ids.append(pid)
                if len(product_ids) >= limit:
                    break

            #products = []
            #for prod_id in product_ids:
            #    product = client[MONGO_PRODUCTS_DB]['products'].find_one(
            #        {'product_id': prod_id}, {'_id': 0}
            #    )
            #    if product:
            #        products.append(product)

            products = _get_product_details(product_ids)

            return products

        except Exception as e:
            print(f"Error getting recently viewed products: {str(e)}")
            return []

    def get_purchased_products(self, user_id, date_range='all', sort_by='recent', page=1, per_page=12):
        """Get purchased products grouped by order, with filtering and pagination."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Base query: get all orders for the user (with filters)
            where_clause = "WHERE o.user_id = %s"
            params = [user_id]

            # Date filter
            if date_range != 'all':
                date_filter = {
                    'today': datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
                    'week': datetime.now() - timedelta(days=7),
                    'month': datetime.now() - timedelta(days=30)
                }
                if date_range in date_filter:
                    where_clause += " AND o.order_date >= %s"
                    params.append(date_filter[date_range])

            # Count total orders for pagination
            count_query = f"SELECT COUNT(DISTINCT o.order_id) FROM orders o {where_clause}"
            cursor.execute(count_query, params)
            total_orders = cursor.fetchone()[0]

            # Sorting
            order_clause = "ORDER BY order_date DESC"
            if sort_by == 'price-high':
                order_clause = "ORDER BY total_amount DESC"
            elif sort_by == 'price-low':
                order_clause = "ORDER BY total_amount ASC"
            # (Sorting by name is not supported at order level)

            # Pagination
            limit_clause = "LIMIT %s OFFSET %s"
            params_with_pagination = params + [per_page, (page - 1) * per_page]

            # Get unique orders for this page, with correct total and summary
            orders_query = f"""
                SELECT o.order_id, MIN(o.order_date) as order_date, SUM(o.total_amount) as total_amount, MIN(o.status) as status
                FROM orders o
                {where_clause}
                GROUP BY o.order_id
                {order_clause}
                {limit_clause}
            """
            cursor.execute(orders_query, params_with_pagination)
            orders = cursor.fetchall()

            order_list = []
            for order_id, order_date, total_amount, status in orders:
                # Get all items for this order
                items_query = """
                    SELECT o.product_id, o.quantity, o.total_amount, r.review_id
                    FROM orders o
                    LEFT JOIN product_reviews r ON o.product_id = r.product_id AND o.user_id = r.user_id
                    WHERE o.order_id = %s
                """
                cursor.execute(items_query, [order_id])
                items = cursor.fetchall()
                product_items = []
                for product_id, quantity, item_total, review_id in items:
                    product = ProductDetails.objects(product_id=product_id).first()
                    if product:
                        product_data = product.to_mongo().to_dict()
                        product_data['quantity'] = quantity
                        product_data['total_price'] = float(item_total)
                        product_data['has_review'] = review_id is not None
                        product_items.append(product_data)
                order_list.append({
                    'order_id': order_id,
                    'order_date': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(order_date / 1000)),
                    'total_amount': float(total_amount),
                    'status': status,
                    'items': product_items
                })

            return {'orders': order_list, 'total': total_orders}

        except Exception as e:
            print(f"Error getting purchased products: {str(e)}")
            return {'orders': [], 'total': 0}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    def get_visited_products(self, user_id, date_range='all', sort_by='recent', page=1, per_page=12):
        """Get visited products with filtering and pagination."""
        try:
            browsing_collection = client[MONGO_BROWSING_DB]['browsing_history']

            # Base pipeline for aggregation
            pipeline = [
                {'$match': {'user_id': user_id, 'page_url': {'$regex': '/product/'}}},
                {'$addFields': {
                    'product_id': {'$arrayElemAt': [{'$split': ['$page_url', '/']}, -1]}
                }},
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
                SELECT r.review_id, r.product_id, r.rating, r.review_text, r.review_date,
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
        """Get user settings (profile fields only, matching get_user_details)."""
        conn = None
        cursor = None
        try:
            # Connect to PostgreSQL
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Get user profile information (matching get_user_details)
            profile_query = """
                SELECT u.email, u.first_name, u.last_name, u.gender, u.birth_date, u.address, u.join_date
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
                'join_date': profile[6].strftime('%Y-%m-%d %H:%M:%S') if profile[6] else None
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
        """Update user settings (profile fields only, matching get_user_details)."""
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

            # Update user profile (only allowed fields)
            update_query = """
                UPDATE users
                SET email = %s,
                    first_name = %s,
                    last_name = %s,
                    gender = %s,
                    birthdate = %s,
                    address = %s,
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
                user_id
            ])

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


    def get_user_details(self, user_id):
        """Get detailed user information including statistics."""
        conn = None
        cursor = None
        try:
            # Connect to PostgreSQL
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Get user profile information
            #profile_query = """
            #    SELECT u.email, u.first_name, u.last_name, u.gender, u.birthdate, u.address,
            #           u.phone_number, u.profile_picture, u.created_at,
            #           u.notification_preferences, u.privacy_settings, u.language_preference,
            #           u.currency_preference, u.timezone
            #    FROM users u
            #    WHERE u.user_id = %s
            #"""
            profile_query = """
                SELECT u.email, u.first_name, u.last_name, u.gender, u.birth_date, u.address, u.join_date
                FROM users u
                WHERE u.user_id = %s
            """
            cursor.execute(profile_query, [user_id])
            profile = cursor.fetchone()

            if not profile:
                return {}

            # Get user statistics
            stats_query = """
                SELECT 
                    (SELECT COUNT(*) FROM orders WHERE user_id = %s) as total_orders,
                    (SELECT COUNT(*) FROM product_reviews WHERE user_id = %s) as total_reviews,
                    (SELECT SUM(total_amount) FROM orders WHERE user_id = %s) as total_spent
            """
            cursor.execute(stats_query, [user_id, user_id, user_id])
            stats = cursor.fetchone()

            # Get recent activity
            activity_query = """
                SELECT 
                    'order' as type,
                    to_timestamp(order_date/1000) as date,
                    product_id,
                    total_amount as amount
                FROM orders 
                WHERE user_id = %s
                UNION ALL
                SELECT 
                    'review' as type,
                    review_date as date,
                    product_id,
                    rating as amount
                FROM product_reviews 
                WHERE user_id = %s
                ORDER BY date DESC
                LIMIT 5
            """
            cursor.execute(activity_query, [user_id, user_id])
            recent_activity = cursor.fetchall()

            # Convert profile data to dictionary
            user_details = {
                'profile': {
                    'email': profile[0],
                    'first_name': profile[1],
                    'last_name': profile[2],
                    'gender': profile[3],
                    'birthdate': profile[4].strftime('%Y-%m-%d') if profile[4] else None,
                    'address': profile[5],
                    'created_at': profile[6].strftime('%Y-%m-%d %H:%M:%S') if profile[6] else None,
                },
                'statistics': {
                    'total_orders': stats[0] or 0,
                    'total_reviews': stats[1] or 0,
                    'total_spent': float(stats[2]) if stats[2] else 0
                },
                'recent_activity': [
                    {
                        'type': activity[0],
                        'date': activity[1].strftime('%Y-%m-%d %H:%M:%S') if activity[1] else None,
                        'product_id': activity[2],
                        'amount': float(activity[3]) if activity[0] == 'order' else int(activity[3])
                    }
                    for activity in recent_activity
                ]
            }

            return user_details

        except Exception as e:
            print(f"Error getting user details: {str(e)}")
            return {}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    def get_user_notifications(self, user_id, page=1, per_page=10):
        """Get user notifications with pagination."""
        conn = None
        cursor = None
        try:
            # Connect to PostgreSQL
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Get notifications
            notifications_query = """
                SELECT 
                    notification_id,
                    type,
                    message,
                    created_at,
                    is_read,
                    related_id
                FROM notifications
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(notifications_query, [user_id, per_page, (page - 1) * per_page])
            notifications = cursor.fetchall()

            # Get total count
            count_query = "SELECT COUNT(*) FROM notifications WHERE user_id = %s"
            cursor.execute(count_query, [user_id])
            total = cursor.fetchone()[0]

            # Convert notifications to list of dictionaries
            notification_list = [
                {
                    'id': n[0],
                    'type': n[1],
                    'message': n[2],
                    'created_at': n[3].strftime('%Y-%m-%d %H:%M:%S'),
                    'is_read': n[4],
                    'related_id': n[5]
                }
                for n in notifications
            ]

            return {
                'notifications': notification_list,
                'total': total
            }

        except Exception as e:
            print(f"Error getting user notifications: {str(e)}")
            return {'notifications': [], 'total': 0}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)
