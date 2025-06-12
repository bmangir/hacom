import inspect
from datetime import datetime
import time

from backend.config import MONGO_BROWSING_DB, client
from backend.utils.utils import kafka_producer_util


class TrackingService:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TrackingService, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
        
    def __init__(self):
        if self._initialized:
            return
            
        self.client = client
        self.db = self.client[MONGO_BROWSING_DB]
        self._initialized = True
        
    def track_browsing_history(self, user_id, session_id, page_url, referrer_url, product_id=None, last_page_time=None):
        """Track user browsing history with duration calculation."""
        try:
            current_time = time.time()
            duration_seconds = None
            
            # Calculate duration if we have last page time
            if last_page_time:
                duration_seconds = int(current_time - last_page_time)
            
            browsing_history = {
                "user_id": user_id,
                "session_id": session_id,
                "page_url": page_url,
                "referrer_url": referrer_url,
                "view_duration": duration_seconds,
                "timestamp": datetime.utcnow()
            }

            # Store in MongoDB
            result = self.db.browsing_history.insert_one(browsing_history)

            # Format data for Kafka
            kafka_event = kafka_producer_util.format_interaction_event(
                user_id=user_id,
                product_id=product_id,
                event_type="view",
                details={
                    "session_id": session_id,
                    "user_id": user_id,
                    "product_id": product_id,
                    "page_url": page_url,
                    "view_date": datetime.utcnow().strftime("%Y-%m-%d"),
                    "view_duration": duration_seconds,
                    "referrer_url": referrer_url
                }
            )
            
            # Send to Kafka
            kafka_producer_util.send_event(kafka_event)
            
            return {"success": True, "id": str(result.inserted_id)}
            
        except Exception as e:
            print(f"Error tracking browsing history: {str(e)}")
            return {"success": False, "error": str(e)}
            
    def track_user_action(self, user_id, session_id, action, page_url, referrer=None, duration_seconds=None, additional_data=None):
        """Track user clickstream actions."""
        previous_frame = inspect.stack()[1]
        caller_function_name = previous_frame.function
        print(f"Func1 was called by: {caller_function_name}")
        try:
            clickstream_data = {
                "user_id": user_id,
                "session_id": session_id,
                "action": action,
                "page_url": page_url,
                "referrer": referrer,
                "duration_seconds": duration_seconds,
                "timestamp": datetime.utcnow()
            }
            
            # Add any additional data if provided
            if additional_data:
                clickstream_data.update(additional_data)
                
            result = self.db.clickstream.insert_one(clickstream_data)
            
            # Extract product_id from additional_data or page_url
            product_id = None
            if additional_data and 'product_id' in additional_data:
                product_id = additional_data['product_id']
            elif '/product/' in page_url:
                product_id = page_url.split('/product/')[-1]
            
            # Format data for Kafka
            kafka_event = kafka_producer_util.format_interaction_event(
                user_id=user_id,
                product_id=product_id,
                event_type=action,
                details={
                    "session_id": session_id,
                    "user_id": user_id,
                    "product_id": product_id,
                    "page_url": page_url,
                    "action_date": datetime.utcnow().strftime("%Y-%m-%d"),
                    "duration_seconds": duration_seconds,
                    "referrer_url": referrer,
                    **(additional_data or {})
                }
            )
            
            # Send to Kafka
            kafka_producer_util.send_event(kafka_event)
                    
            return {"success": True, "id": str(result.inserted_id)}
            
        except Exception as e:
            print(f"Error tracking user action: {str(e)}")
            return {"success": False, "error": str(e)} 