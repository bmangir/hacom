import re
import time
from datetime import datetime, timedelta

from backend.app.models.user_models import UBCF, ContentBased, PersonalizedTrending, RecentlyViewed, \
    CategoryRecommendation
from backend.utils.utils import _get_product_details
from backend.config import client, MONGO_BROWSING_DB, MONGO_PRODUCTS_DB
from databases.postgres.neon_postgres_connector import NeonPostgresConnector

class UserRecommendationService:
    def __init__(self, mongo_db, cache):
        self.db = mongo_db
        self.cache = cache
            
        # Create indexes for better performance
        try:
            browsing_collection = client[MONGO_BROWSING_DB]['browsing_history']
            products_collection = client[MONGO_PRODUCTS_DB]['products']
            
            # Create indexes if they don't exist
            browsing_collection.create_index([("user_id", 1), ("timestamp", -1)])
            browsing_collection.create_index([("page_url", 1)])
            products_collection.create_index([("product_id", 1)])
        except Exception as e:
            print(f"Error creating indexes: {str(e)}")

    def get_ubcf_recommendations(self, user_id, num_recommendations=10):
        # Try to get from cache first
        cached_items = self.cache.get_user_recommendations(user_id, "ubcf")
        if cached_items:
            return cached_items

        # If not in cache, get from MongoDB
        items = UBCF.objects(user_id=user_id).limit(num_recommendations)

        if items is None or len(items) == 0:
            items = UBCF.objects(user_id=user_id).limit(num_recommendations)

        recc_items = []
        for item in items:
            recc_items.append(item.recc_item)

        result = _get_product_details(recc_items)

        # Cache the results
        if result:
            self.cache.set_user_recommendations(user_id, result, "ubcf")

        return result

    def get_content_based_recommendations(self, user_id, num_recommendations=10):
        # Try to get from cache first
        cached_items = self.cache.get_user_recommendations(user_id, "content_based")
        if cached_items:
            return cached_items

        # If not in cache, get from MongoDB
        items = ContentBased.objects(user_id=user_id).limit(num_recommendations)
        recc_items = []

        for item in items:
            recc_items.append(item.recc_item)

        result = _get_product_details(recc_items)

        # Cache the results
        if result:
            self.cache.set_user_recommendations(user_id, result, "content_based")

        return result

    def get_personalized_trendings(self, user_id, num_recommendations=10):
        # Try to get from cache first
        cached_items = self.cache.get_user_recommendations(user_id, "personalized_trending")
        if cached_items:
            return cached_items

        # If not in cache, get from MongoDB
        items = PersonalizedTrending.objects(user_id=user_id).order_by('-final_trending_score').limit(num_recommendations)
        recc_items = []

        for item in items:
            recc_items.append(item.recc_item)

        result = _get_product_details(recc_items)

        # Cache the results
        if result:
            self.cache.set_user_recommendations(user_id, result, "personalized_trending")

        return result

    def get_most_visited_categories(self, user_id, num_categories=3):
        # Try to get from cache first
        cached_cats = self.cache.get_user_recommendations(user_id, "categories")
        if cached_cats:
            return cached_cats

        # If not in cache, get from MongoDB
        cats = CategoryRecommendation.objects(user_id=user_id).limit(num_categories)
        recc_cats = []

        for c in cats:
            recc_cats.append(c.category)

        # Cache the results
        if recc_cats:
            self.cache.set_user_recommendations(user_id, recc_cats, "categories")

        return recc_cats

    def get_recently_viewed_based_recommendations(self, user_id, num_recommendations=10):
        # Try to get from cache first
        cached_items = self.cache.get_user_recommendations(user_id, "recently_viewed")
        if cached_items:
            return cached_items

        # If not in cache, get from MongoDB
        items = RecentlyViewed.objects(user_id=user_id).order_by('-recc_at').limit(num_recommendations)
        recc_items = []

        for item in items:
            recc_items.append(item.recc_item)

        result = _get_product_details(recc_items)

        # Cache the results
        if result:
            self.cache.set_user_recommendations(user_id, result, "recently_viewed")

        return result

    def get_recently_viewed_products(self, user_id, limit=10):
        """Get recently viewed products for a user."""
        # Try to get from cache first
        cached_products = self.cache.get_recently_viewed_products(user_id)
        if cached_products:
            return cached_products[:limit]  # Return only requested number of items

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

            products = _get_product_details(product_ids)

            # Cache the results
            if products:
                self.cache.set_recently_viewed_products(user_id, products)

            return products
        except Exception as e:
            print(f"Error getting recently viewed products: {str(e)}")
            return []

    def get_purchased_products(self, user_id, date_range='all', sort_by='recent', page=1, per_page=12):
        """Get purchased products with filtering and pagination."""
        # Try to get from cache first
        cached_data = self.cache.get_purchased_products(user_id, date_range, sort_by, page)
        if cached_data:
            return cached_data

        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

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
                order_clause = "ORDER BY o.product_id"

            # Add pagination
            limit_clause = "LIMIT %s OFFSET %s"
            params.extend([per_page, (page - 1) * per_page])

            # Combine query
            query = f"{select_clause} {where_clause} {order_clause} {limit_clause}"

            # Execute query
            cursor.execute(query, params)
            orders = cursor.fetchall()

            products_collection = client[MONGO_PRODUCTS_DB]['products']

            # Combine order and product information
            products = []
            for order in orders:
                order_id, product_id, quantity, order_date, total_amount, review_id, status = order

                # Get product details from MongoDB
                product = products_collection.find_one({'product_id': product_id}, {'_id': 0})
                if product:
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

            result = {'products': products, 'total': total_count}
            
            # Cache the results
            if products:
                self.cache.set_purchased_products(user_id, date_range, sort_by, page, result)

            return result

        except Exception as e:
            print(f"Error getting purchased products: {str(e)}")
            return {'products': [], 'total': 0}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    def get_visited_products(self, user_id, date_range='all', sort_by='recent', page=1, per_page=12):
        """Get visited products with filtering and pagination."""
        # Try to get from cache first
        cached_data = self.cache.get_visited_products(user_id, date_range, sort_by, page)
        if cached_data:
            return cached_data

        try:
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

            result = {'products': products, 'total': total_count}
            
            # Cache the results
            if products:
                self.cache.set_visited_products(user_id, date_range, sort_by, page, result)

            return result

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