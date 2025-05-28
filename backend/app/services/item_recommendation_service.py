from sentence_transformers import SentenceTransformer
import google.generativeai as genai
import time

from backend.app.models.item_models import IBCF, BestSellers, NewArrivals, SeasonalRecc, Trending, ReviewedBased, \
    TrendingCategories
from backend.app.models.user_models import ProductDetails
from backend.utils.utils import _get_product_details
from backend.config import pc, ITEM_CONTENTS_HOST, client, MONGO_PRODUCTS_DB, GEMINI_API_KEY
from databases.postgres.neon_postgres_connector import NeonPostgresConnector


class ItemRecommendationService:

    def __init__(self, mongo_db, cache):
        self.db = mongo_db
        self.cache = cache
        self.content_model = SentenceTransformer("all-MiniLM-L6-v2")
        
        # Initialize Gemini
        genai.configure(api_key=GEMINI_API_KEY)
        self.gemini_model = genai.GenerativeModel("gemma-3-1b-it")

    def refine_query_with_gemini(self, raw_query: str) -> str:
        """Refine the search query using Gemini AI"""
        try:
            prompt = f"""
            You are a search optimization assistant.
            Improve the following search query to be more specific and helpful for finding products:

            Query: "{raw_query}"

            Output only the improved query without extra text.
            """
            response = self.gemini_model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            print(f"Error refining query with Gemini: {str(e)}")
            return raw_query  # Return original query if refinement fails

    def get_ibcf_recommendations(self, product_id: str, num_recommendations: int = 5):
        # Try to get from cache first
        cached_items = self.cache.get_item_recommendations(product_id, "ibcf")
        if cached_items:
            return cached_items

        # If not in cache, get from MongoDB
        items = IBCF.objects(product_id=product_id).limit(num_recommendations)
        recc_items = []
        for item in items:
            recc_items.append(item.recc_items)

        result = _get_product_details(recc_items)
        
        # Cache the results
        if result:
            self.cache.set_item_recommendations(product_id, result, "ibcf")

        return result

    def get_bought_together_items(self, product_id: str):
        try:
            # Try to get from cache first
            cached_items = self.cache.get_item_recommendations(product_id, "bought_together")
            if cached_items:
                return cached_items

            # If not in cache, get from MongoDB
            items = IBCF.objects(product_id=product_id, recommendation_type="bought_together").limit(5)
            ids = []
            for item in items:
                ids += item.recc_items  # Using recc_items field instead of bought_together

            result = _get_product_details(ids)

            # Cache the results
            if result:
                self.cache.set_item_recommendations(product_id, result, "bought_together")

            return result
        except Exception as e:
            print(f"Error getting bought together items: {str(e)}")
            return []

    def get_best_seller_items(self):
        # Try to get from cache first
        cached_items = self.cache.get_item_recommendations("global", "best_sellers")
        if cached_items:
            return cached_items

        # If not in cache, get from MongoDB
        items = BestSellers.objects().limit(10)
        recc_items = []
        for item in items:
            recc_items.append(item.product_id)

        result = _get_product_details(recc_items)

        # Cache the results
        if result:
            self.cache.set_item_recommendations("global", result, "best_sellers")

        return result

    def get_new_arrivals_items(self):
        # Try to get from cache first
        cached_items = self.cache.get_item_recommendations("global", "new_arrivals")
        if cached_items:
            return cached_items

        # If not in cache, get from MongoDB
        items = NewArrivals.objects().limit(10)
        recc_items = []
        for item in items:
            recc_items.append(item.product_id)

        result = _get_product_details(recc_items)

        # Cache the results
        if result:
            self.cache.set_item_recommendations("global", result, "new_arrivals")

        return result

    def get_seasonal_recommended_items(self):
        # Try to get from cache first
        cached_items = self.cache.get_item_recommendations("global", "seasonal")
        if cached_items:
            return cached_items

        # If not in cache, get from MongoDB
        items = SeasonalRecc.objects().limit(10)
        recc_items = []
        for item in items:
            recc_items.append(item.product_id)

        result = _get_product_details(recc_items)

        # Cache the results
        if result:
            self.cache.set_item_recommendations("global", result, "seasonal")

        return result

    def get_trending_items(self, num_recommendations: int = 10):
        # Try to get from cache first
        cached_items = self.cache.get_item_recommendations("global", "trending")
        if cached_items:
            return cached_items

        # If not in cache, get from MongoDB
        items = Trending.objects().order_by('-trending_score').limit(num_recommendations)
        recc_items = []
        for item in items:
            recc_items.append(item.product_id)

        result = _get_product_details(recc_items)

        # Cache the results
        if result:
            self.cache.set_item_recommendations("global", result, "trending")

        return result

    def get_reviewed_based_items(self):
        # Try to get from cache first
        cached_items = self.cache.get_item_recommendations("global", "reviewed")
        if cached_items:
            return cached_items

        # If not in cache, get from MongoDB
        ReviewedBased.objects(trending_score=None).update(set__trending_score=0.0)
        items = ReviewedBased.objects().order_by('-trending_score').limit(10)
        recc_items = []
        for item in items:
            recc_items.append(item.product_id)

        result = _get_product_details(recc_items)

        # Cache the results
        if result:
            self.cache.set_item_recommendations("global", result, "reviewed")

        return result

    def search(self, query: str, top_k: int = 3):
        try:
            # Refine query using Gemini
            refined_query = self.refine_query_with_gemini(query)
            
            index = pc.Index(host=ITEM_CONTENTS_HOST)
            query_vector = self.content_model.encode(refined_query).tolist()

            query_response = index.query(
                vector=query_vector,
                top_k=top_k,
                include_metadata=False
            )
            
            # Extract product IDs from the matches
            similar_product_ids = [match.id for match in query_response.matches]

            return _get_product_details(similar_product_ids)
            
        except Exception as e:
            print(f"Error searching for products with query '{query}': {str(e)}")
            return []

    def get_trending_categories(self, user_id, num_categories: int = 5):
        cached_cats = self.cache.get_user_recommendations(user_id, "categories")
        if cached_cats:
            return cached_cats

        categories = TrendingCategories.objects().limit(num_categories)
        recc_cats = []

        for c in categories:
            recc_cats.append(c.category)

        # Cache the results
        if recc_cats:
            self.cache.set_user_recommendations(user_id, recc_cats, "categories")

        return recc_cats

    def get_top_n_product_based_on_category(self, item_id, category: str, num_recommendations: int = 10):
        cached_items = self.cache.get_item_recommendations(item_id, "category_based")
        if cached_items:
            return cached_items

        data = TrendingCategories.objects(category=category).limit(num_recommendations)
        recc_items = []
        for x in data:
            recc_items += x.top_20_product_ids

        result = _get_product_details(recc_items)

        # Cache the results
        if result:
            self.cache.set_item_recommendations(item_id, result, "category_based")

        return result

    def get_merchant_details(self, merchant_id):
        """Get merchant details and their products."""
        try:
            merchant_products = list(client[MONGO_PRODUCTS_DB]['products'].find(
                {"merchant_id": merchant_id},
                {"_id": 0}
            ))

            if not merchant_products:
                return None

            merchant_name = merchant_products[0].get('merchant_name', 'Unknown Merchant')

            merchant_info = {
                'merchant_id': merchant_id,
                'name': merchant_name,
                'products': merchant_products
            }

            return merchant_info

        except Exception as e:
            print(f"Error getting merchant details: {str(e)}")
            return None
