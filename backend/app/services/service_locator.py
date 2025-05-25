from backend.app.services.cart_service import CartService
from backend.app.services.item_recommendation_service import ItemRecommendationService
from backend.app.services.order_service import OrderService
from backend.app.services.session_service import SessionService
from backend.app.services.user_recommendation_service import UserRecommendationService
from backend.app.services.tracking_service import TrackingService
from backend.app.services.user_service import UserService
from backend.app.services.wishlist_service import WishlistService
from app.cache.redis_cache import RecommendationCache
from backend.config import MONGO_RECOMMENDATION_DB, client

# Initialize Redis cache
redis_cache = RecommendationCache()

# Test Redis connection
try:
    if redis_cache.test_connection():
        print("Redis connection successful")
    else:
        print("Warning: Redis connection failed. Cache will not be available.")
except Exception as e:
    print(f"Redis initialization error: {e}")

# Initialize MongoDB connection
try:
    mongo_client = client
    mongo_db = mongo_client[MONGO_RECOMMENDATION_DB]
except Exception as e:
    mongo_db = None

# Initialize recommendation services with MongoDB and Redis
ITEM_RECOMMENDATION_SERVICE = ItemRecommendationService(
    mongo_db=mongo_db, 
    cache=redis_cache
)

USER_RECOMMENDATION_SERVICE = UserRecommendationService(
    mongo_db=mongo_db,
    cache=redis_cache
)

# Initialize other services
tracking_service = TrackingService()
cart_service = CartService()
wishlist_service = WishlistService()
session_service = SessionService()
user_service = UserService()
order_service = OrderService()