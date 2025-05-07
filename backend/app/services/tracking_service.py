from datetime import datetime


from kafka.producer import KafkaProducer

from databases.mongo.mongo_connector import MongoConnector
from backend.config.config import MONGO_BROWSING_DB
import time

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
            
        mongo_conn = MongoConnector()
        self.client = mongo_conn.get_client()
        self.db = self.client[MONGO_BROWSING_DB]
        self._initialized = True
        
    def track_browsing_history(self, user_id, session_id, page_url, referrer_url, last_page_time=None):
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

            # send this data to kafka topic
            producer = KafkaProducer()
            producer.send(topic="interactionTopic", value=browsing_history)
            
            result = self.db.browsing_history.insert_one(browsing_history)
            return {"success": True, "id": str(result.inserted_id)}
            
        except Exception as e:
            print(f"Error tracking browsing history: {str(e)}")
            return {"success": False, "error": str(e)}
            
    def track_user_action(self, user_id, session_id, action, page_url, referrer=None, duration_seconds=None, additional_data=None):
        """Track user clickstream actions."""
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
            return {"success": True, "id": str(result.inserted_id)}
            
        except Exception as e:
            print(f"Error tracking user action: {str(e)}")
            return {"success": False, "error": str(e)} 