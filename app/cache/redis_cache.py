import json
from redis import Redis
from typing import List, Dict, Optional

from backend.config import REDIS_USERNAME, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD


class RecommendationCache:
    def __init__(self,):
        try:
            self.redis_client = Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                username=REDIS_USERNAME,
                password=REDIS_PASSWORD
            )
            self.default_ttl = 3600  # 1 hour default TTL
        except Exception as e:
            print(f"Redis initialization error: {str(e)}")
            self.redis_client = None

    def is_connected(self) -> bool:
        """Check if Redis is connected and working"""
        if not self.redis_client:
            print("here is no Redis client available")
            return False
        try:
            print("Testing Redis connection...")
            return self.redis_client.ping()
        except Exception as e:
            print(f"Redis connection test failed: {str(e)}")
            return False

    def _get_cache_key(self, prefix: str, id: str, recc_type: str = None) -> str:
        """Generate cache key based on prefix, id and recommendation type"""
        if recc_type:
            return f"recommendations:{prefix}:{id}:{recc_type}"
        return f"recommendations:{prefix}:{id}"

    def get_user_recommendations(self, user_id: str, recc_type: str) -> Optional[List[Dict]]:
        """Get cached recommendations for a user"""
        if not self.is_connected():
            return None
            
        cache_key = self._get_cache_key("user", user_id, recc_type)
        try:
            cached_data = self.redis_client.get(cache_key)
            return json.loads(cached_data) if cached_data else None
        except Exception as e:
            print(f"Redis get error: {str(e)}")
            return None

    def set_user_recommendations(self, user_id: str, recommendations: List[Dict], recc_type: str, ttl: int = None):
        """Cache recommendations for a user"""
        if not self.is_connected():
            return
            
        cache_key = self._get_cache_key("user", user_id, recc_type)
        try:
            self.redis_client.setex(
                cache_key,
                ttl or self.default_ttl,
                json.dumps(recommendations)
            )
        except Exception as e:
            print(f"Redis set error: {str(e)}")

    def delete_user_recommendations(self, user_id: str, recc_type: str = None):
        """Delete cached recommendations for a user"""
        if not self.is_connected():
            return
            
        try:
            if recc_type:
                cache_key = self._get_cache_key("user", user_id, recc_type)
                self.redis_client.delete(cache_key)
            else:
                # Delete all recommendation types for this user
                pattern = self._get_cache_key("user", user_id, "*")
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
        except Exception as e:
            print(f"Redis delete error: {str(e)}")

    def get_item_recommendations(self, item_id: str, recc_type: str) -> Optional[List[Dict]]:
        """Get cached similar items"""
        if not self.is_connected():
            return None
            
        cache_key = self._get_cache_key("item", item_id, recc_type)
        try:
            cached_data = self.redis_client.get(cache_key)
            return json.loads(cached_data) if cached_data else None
        except Exception as e:
            print(f"Redis get error: {str(e)}")
            return None

    def set_item_recommendations(self, item_id: str, similar_items: List[Dict], recc_type: str, ttl: int = None):
        """Cache similar items"""
        if not self.is_connected():
            return
            
        cache_key = self._get_cache_key("item", item_id, recc_type)
        try:
            self.redis_client.setex(
                cache_key,
                ttl or self.default_ttl,
                json.dumps(similar_items)
            )
        except Exception as e:
            print(f"Redis set error: {str(e)}")

    def delete_item_recommendations(self, item_id: str, recc_type: str = None):
        """Delete cached recommendations for an item"""
        if not self.is_connected():
            return
            
        try:
            if recc_type:
                cache_key = self._get_cache_key("item", item_id, recc_type)
                self.redis_client.delete(cache_key)
            else:
                # Delete all recommendation types for this item
                pattern = self._get_cache_key("item", item_id, "*")
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
        except Exception as e:
            print(f"Redis delete error: {str(e)}")

    def test_connection(self) -> bool:
        """Test Redis connection"""
        return self.is_connected()

    def get_category_items(self, category: str, recc_type: str) -> Optional[List[Dict]]:
        """Get cached items for a category"""
        if not self.is_connected():
            return None
            
        cache_key = self._get_cache_key("category", category, recc_type)
        try:
            cached_data = self.redis_client.get(cache_key)
            return json.loads(cached_data) if cached_data else None
        except Exception as e:
            print(f"Redis get error: {str(e)}")
            return None

    def set_category_items(self, category: str, items: List[Dict], recc_type: str, ttl: int = None):
        """Cache items for a category"""
        if not self.is_connected():
            return
            
        cache_key = self._get_cache_key("category", category, recc_type)
        try:
            self.redis_client.setex(
                cache_key,
                ttl or self.default_ttl,
                json.dumps(items)
            )
        except Exception as e:
            print(f"Redis set error: {str(e)}")

    def delete_category_items(self, category: str, recc_type: str = None):
        """Delete cached items for a category"""
        if not self.is_connected():
            return
            
        try:
            if recc_type:
                cache_key = self._get_cache_key("category", category, recc_type)
                self.redis_client.delete(cache_key)
            else:
                # Delete all recommendation types for this category
                pattern = self._get_cache_key("category", category, "*")
                keys = self.redis_client.keys(pattern)
                if keys:
                    self.redis_client.delete(*keys)
        except Exception as e:
            print(f"Redis delete error: {str(e)}")

    def get_recently_viewed_products(self, user_id: str) -> Optional[List[Dict]]:
        """Get cached recently viewed products for a user"""
        if not self.is_connected():
            return None
            
        cache_key = self._get_cache_key("recent_views", user_id)
        try:
            cached_data = self.redis_client.get(cache_key)
            return json.loads(cached_data) if cached_data else None
        except Exception as e:
            print(f"Redis get error: {str(e)}")
            return None

    def set_recently_viewed_products(self, user_id: str, products: List[Dict], ttl: int = None):
        """Cache recently viewed products for a user"""
        if not self.is_connected():
            return
            
        cache_key = self._get_cache_key("recent_views", user_id)
        try:
            self.redis_client.setex(
                cache_key,
                ttl or self.default_ttl,
                json.dumps(products)
            )
        except Exception as e:
            print(f"Redis set error: {str(e)}")

    def get_purchased_products(self, user_id: str, date_range: str, sort_by: str, page: int) -> Optional[Dict]:
        """Get cached purchased products for a user"""
        if not self.is_connected():
            return None
            
        cache_key = self._get_cache_key("purchases", f"{user_id}:{date_range}:{sort_by}:{page}")
        try:
            cached_data = self.redis_client.get(cache_key)
            return json.loads(cached_data) if cached_data else None
        except Exception as e:
            print(f"Redis get error: {str(e)}")
            return None

    def set_purchased_products(self, user_id: str, date_range: str, sort_by: str, page: int, data: Dict, ttl: int = None):
        """Cache purchased products for a user"""
        if not self.is_connected():
            return
            
        cache_key = self._get_cache_key("purchases", f"{user_id}:{date_range}:{sort_by}:{page}")
        try:
            self.redis_client.setex(
                cache_key,
                ttl or self.default_ttl,
                json.dumps(data)
            )
        except Exception as e:
            print(f"Redis set error: {str(e)}")

    def get_visited_products(self, user_id: str, date_range: str, sort_by: str, page: int) -> Optional[Dict]:
        """Get cached visited products for a user"""
        if not self.is_connected():
            return None
            
        cache_key = self._get_cache_key("visits", f"{user_id}:{date_range}:{sort_by}:{page}")
        try:
            cached_data = self.redis_client.get(cache_key)
            return json.loads(cached_data) if cached_data else None
        except Exception as e:
            print(f"Redis get error: {str(e)}")
            return None

    def set_visited_products(self, user_id: str, date_range: str, sort_by: str, page: int, data: Dict, ttl: int = None):
        """Cache visited products for a user"""
        if not self.is_connected():
            return
            
        cache_key = self._get_cache_key("visits", f"{user_id}:{date_range}:{sort_by}:{page}")
        try:
            self.redis_client.setex(
                cache_key,
                ttl or self.default_ttl,
                json.dumps(data)
            )
        except Exception as e:
            print(f"Redis set error: {str(e)}") 