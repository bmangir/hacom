from pymongo import MongoClient
from backend.app.services.item_recommendation_service import ItemRecommendationService
from backend.app.services.user_recommendation_service import UserRecommendationService
from app.cache.redis_cache import RecommendationCache
from backend.config import MONGO_URI, MONGO_RECOMMENDATION_DB, client


class ServiceContainer:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ServiceContainer, cls).__new__(cls)
            cls._instance._initialize_services()
        return cls._instance

    def _initialize_services(self):
        """Initialize all services with proper configuration"""
        try:
            # Initialize MongoDB connection
            self.mongo_client = client
            self.db = self.mongo_client[MONGO_RECOMMENDATION_DB]
            
            # Initialize Redis cache
            self.cache = RecommendationCache()

            # Initialize recommendation services with MongoDB and Redis
            self.user_recommendation_service = UserRecommendationService(
                mongo_db=self.db,
                cache=self.cache
            )

            self.item_recommendation_service = ItemRecommendationService(
                mongo_db=self.db,
                cache=self.cache
            )
        except Exception as e:
            print(f"Error initializing services: {str(e)}")
            raise

    def test_redis_connection(self) -> bool:
        """Test Redis connection through cache service"""
        return self.cache.test_connection() if hasattr(self, 'cache') else False

    def test_mongo_connection(self) -> bool:
        """Test MongoDB connection"""
        try:
            return bool(self.mongo_client.server_info())
        except Exception as e:
            print(f"MongoDB connection test failed: {str(e)}")
            return False

# Global service container instance
service_container = ServiceContainer() 