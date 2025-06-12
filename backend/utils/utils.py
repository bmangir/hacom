from backend.app.models.user_models import ProductDetails
import json
from datetime import datetime
from confluent_kafka import Producer
from backend.config import (
    CONFLUENT_BOOTSTRAP_SERVERS,
    CONFLUENT_API_KEY,
    CONFLUENT_API_SECRET,
    CONFLUENT_CLIENT_ID,
    CONFLUENT_INTERACTION_TOPIC
)


def _get_product_details(product_ids):
    """Get full product details for recommendation output"""

    recommended_products = []
    for pid in product_ids:
        product_info = ProductDetails.objects(product_id=pid).first()
        recommended_products.append({
            'id': product_info.product_id,
            'name': product_info.product_name,
            'category': product_info.category,
            'price': float(product_info.price),
            'stock_quantity': product_info.stock_quantity,
            'brand': product_info.brand,
            'rating': float(product_info.rating) if product_info.rating else None,
            'image_url': product_info.image_url,
            'details': product_info.details,
            'tags': product_info.tags,
            'size': product_info.size,
            'color': product_info.color,
            'format': product_info.format,
            'material': product_info.material,
            'language': product_info.language,
        })

    return recommended_products


class KafkaProducerUtil:
    """Utility class for Kafka producer operations using Confluent Cloud"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaProducerUtil, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        try:
            print("Starting Kafka producer initialization...")

            conf = {
                'bootstrap.servers': CONFLUENT_BOOTSTRAP_SERVERS,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': CONFLUENT_API_KEY,
                'sasl.password': CONFLUENT_API_SECRET,
                'session.timeout.ms': '45000',
                'client.id': CONFLUENT_CLIENT_ID}

            print("Creating Kafka producer with configuration...")
            self.producer = Producer(conf)

            self.producer.flush(0)
            print("Kafka producer initialized successfully!")

        except Exception as e:
            print(f"Error initializing Kafka producer: {str(e)}")
            self.producer = None
            raise

        self._initialized = True

    def _delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def _serialize_datetime(self, data):
        """Convert datetime objects to string for JSON serialization."""
        if isinstance(data, dict):
            return {k: self._serialize_datetime(v) for k, v in data.items()}
        elif isinstance(data, datetime):
            return data.isoformat()
        return data

    def send_event(self, event_data):
        """Send event data to Kafka topic"""
        if not self.producer:
            print("Kafka producer not initialized")
            return False

        try:
            key = "berkant"
            value = json.dumps(event_data, default=self._serialize_datetime)
            self.producer.produce(CONFLUENT_INTERACTION_TOPIC, key=key, value=value)
            print(f"Produced message to topic {CONFLUENT_INTERACTION_TOPIC}: key = {key:12} value = {value:12}")

            self.producer.flush()

            return True

        except Exception as e:
            print(f"Error sending to Kafka: {str(e)}")
            return False

    def format_interaction_event(self, user_id, product_id, event_type, details):
        """Format interaction event"""
        return {
            "user_id": user_id,
            "product_id": product_id,
            "event_type": event_type,
            "details": details,
            "timestamp": datetime.utcnow()
        }


kafka_producer_util = KafkaProducerUtil()
