import datetime

from backend.app.models.user_models import ProductDetails
from databases.postgres.neon_postgres_connector import NeonPostgresConnector


class ReviewService:
    def __init__(self, cache):
        self.cache = cache

    def get_product_reviews(self, product_id: str):
        """Get reviews for a product with user details."""
        # Try to get from cache first
        cached_reviews = self.cache.get_item_recommendations(product_id, "reviews")
        if cached_reviews:
            return cached_reviews

        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Get reviews with user information
            review_query = """
                SELECT r.review_id, r.rating, r.review_text, r.review_date, r.review_helpful_count,
                       u.first_name, u.last_name
                FROM product_reviews r
                JOIN users u ON r.user_id = u.user_id
                WHERE r.product_id = %s
                ORDER BY r.review_date DESC
            """
            cursor.execute(review_query, [product_id])
            reviews = cursor.fetchall()

            # Format reviews
            formatted_reviews = []
            for review in reviews:
                review_id, rating, comment, review_date, helpful_count, first_name, last_name = review
                formatted_reviews.append({
                    'review_id': review_id,
                    'rating': rating,
                    'comment': comment,
                    'date': review_date,
                    'helpful_count': helpful_count or 0,
                    'first_name': first_name,
                    'last_name': last_name
                })

            # Cache the results
            if formatted_reviews:
                self.cache.set_item_recommendations(product_id, formatted_reviews, "reviews")

            return formatted_reviews

        except Exception as e:
            print(f"Error getting product reviews: {str(e)}")
            return []
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    @staticmethod
    def create_or_update_review(user_id, product_id, rating, comment, action_type='created'):
        """Create a new review"""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            # Check if user has already reviewed this product
            cursor.execute("""
                SELECT review_helpful_count FROM product_reviews 
                WHERE user_id = %s AND product_id = %s
            """, (user_id, product_id))

            review_helpful_count = 0
            review_exists = cursor.fetchone()
            if review_exists:
                action_type = 'updated'
                review_helpful_count = review_exists[0]
            
            cursor.execute("""
                INSERT INTO product_reviews (user_id, product_id, rating, review_text, review_helpful_count, action_type)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING *
            """, (user_id, product_id, rating, comment, review_helpful_count, action_type))
            
            conn.commit()
            return cursor.fetchone()
        except Exception as e:
            conn.rollback()
            print(e)
            raise e
        finally:
            cursor.close()

    @staticmethod
    def delete_review(product_id, user_id):
        """Delete a review"""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE product_reviews
                SET action_type = 'deleted'
                WHERE (product_id, user_id, review_date) = (
                    SELECT product_id, user_id, review_date
                    FROM product_reviews
                    WHERE product_id = %s AND user_id = %s
                    ORDER BY review_date DESC
                    LIMIT 1
                );
                """, (product_id, user_id))
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()

    @staticmethod
    def get_product_average_rating(product_id):
        """Get average rating for a product"""
        product_avg_ration = ProductDetails.objects(product_id=product_id).first().avg_rating
        return product_avg_ration

    @staticmethod
    def get_user_review_for_product(user_id, product_id):
        """Get a specific user's review for a product"""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM product_reviews 
                WHERE user_id = %s AND product_id = %s
                AND action_type != 'deleted'
                ORDER BY review_date DESC
                LIMIT 1
            """, (user_id, product_id))
            return cursor.fetchone()
        finally:
            cursor.close()

