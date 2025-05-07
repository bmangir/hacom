from mongoengine import Document, StringField, IntField, LongField, FloatField, connect, get_connection, \
    register_connection, DictField, DateTimeField

from backend.config import MONGO_RECOMMENDATION_DB, MONGO_URI

register_connection(
    alias='recc_default',
    name=MONGO_RECOMMENDATION_DB,
    host=MONGO_URI,
    maxPoolSize=100
)


class IBCF(Document):
    meta = {
        'db_alias': 'recc_default',
        'collection': 'ibcf'
    }

    product_id = StringField(required=True)
    recc_items = StringField()
    category = StringField()
    recc_at = LongField()
    brand = StringField()
    metadata_product_id = StringField()
    avg_rating = IntField()
    rank = IntField()
    source = StringField()
    trending_score = FloatField()


class BestSellers(Document):
    meta = {
        'db_alias': 'recc_default',
        'collection': 'best_sellers'
    }


class NewArrivals(Document):
    meta = {
        'db_alias': 'recc_default',
        'collection': 'new_arrivals'
    }

    # avg_rating', 'recc_at', 'date_added', 'product_id', 'trending_score
    product_id = StringField(required=True)
    avg_rating = FloatField()
    recc_at = LongField()
    date_added = DateTimeField()
    trending_score = FloatField()


class SeasonalRecc(Document):
    meta = {
        'db_alias': 'recc_default',
        'collection': 'seasonal'
    }

    product_id = StringField(required=True)
    seasonal_sales = IntField()
    trending_score = FloatField()
    recc_at = LongField()
    avg_rating = FloatField()


class ReviewedBased(Document):
    meta = {
        'db_alias': 'recc_default',
        'collection': 'reviewed_based'
    }


class Trending(Document):
    meta = {
        'db_alias': 'recc_default',
        'collection': 'trending_products'
    }
    # latest_sales_summary', 'avg_rating', 'trending_score', 'product_id
    product_id = StringField(required=True)
    avg_rating = FloatField()
    trending_score = FloatField()
    latest_sales_summary = DictField()

