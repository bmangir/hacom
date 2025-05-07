from mongoengine import Document, StringField, IntField, LongField, FloatField, connect, get_connection, \
    register_connection, DictField, DateTimeField, ListField

from backend.config import MONGO_RECOMMENDATION_DB, MONGO_URI, MONGO_PRODUCTS_DB

register_connection(
    alias='recc_default',
    name=MONGO_RECOMMENDATION_DB,
    host=MONGO_URI,
    maxPoolSize=100
)

register_connection(
    alias='product_details_default',
    name=MONGO_PRODUCTS_DB,
    host=MONGO_URI,
    maxPoolSize=100
)


class UBCF(Document):
    meta = {
        'db_alias': 'recc_default',
        'collection': 'ubcf'
    }

    user_id = StringField(required=True)
    recc_item = StringField()
    recc_at = LongField()
    recommendation_score = FloatField()


class ContentBased(Document):
    meta = {
        'db_alias': 'recc_default',
        'collection': 'content_based'
    }

    user_id = StringField(required=True)
    recc_item = StringField()
    final_score = FloatField()
    recc_at = LongField()
    relevance_score = FloatField()
    source = StringField()


class PersonalizedTrending(Document):
    meta = {
        'db_alias': 'recc_default',
        'collection': 'personalized'
    }

    user_id = StringField(required=True)
    recc_item = StringField()
    final_trending_score = FloatField()
    recent_sales = IntField()
    avg_rating = FloatField()
    trend_momentum = FloatField()
    velocity_score = FloatField()
    engagement_score = FloatField()
    personalization_score = FloatField()
    conversion_rate = FloatField()
    recc_at = LongField()


class RecentlyViewed(Document):
    meta = {
        'db_alias': 'recc_default',
        'collection': 'recently_viewed'
    }

    user_id = StringField(required=True)
    recc_item = StringField()
    source = StringField()
    recc_at = LongField()


class ProductDetails(Document):
    meta = {
        'db_alias': 'product_details_default',
        'collection': 'products'
    }

    product_id = StringField(required=True)
    product_name = StringField()
    category = StringField()
    price = FloatField()
    stock_quantity = IntField()
    brand = StringField()
    rating = FloatField()
    image_url = StringField()
    date_added = DateTimeField()
    merchant_id = StringField()
    size = ListField()
    color = ListField()
    material = StringField()
    gender = StringField()
    tags = ListField()
    details = DictField()
    format = StringField()
    author = StringField()
    language = StringField()