from app.models.item_recc_model import IBCF, BestSellers, NewArrivals, SeasonalRecc, Trending
from app.services.utils import _get_product_details


class ItemRecommendationService:

    def __init__(self):
        pass

    def get_ibcf_recommendations(self, product_id: str, num_recommendations: int = 5):
        items = IBCF.objects(product_id=product_id).limit(num_recommendations)

        recc_items = []
        for item in items:
            recc_items.append(item.recc_items)

        return _get_product_details(recc_items)

    def get_best_seller_items(self):
        items = BestSellers.objects().limit(10)
        recc_items = []
        for item in items:
            recc_items.append(item.product_id)

        return _get_product_details(recc_items)

    def get_new_arrivals_items(self):
        items = NewArrivals.objects().limit(10)
        recc_items = []
        for item in items:
            recc_items.append(item.product_id)

        return _get_product_details(recc_items)

    def get_seasonal_recommended_items(self):
        items = SeasonalRecc.objects().limit(10)
        recc_items = []
        for item in items:
            recc_items.append(item.product_id)

        return _get_product_details(recc_items)

    def get_trending_items(self):
        items = Trending.objects().order_by('-trending_score').limit(10)
        recc_items = []
        for item in items:
            recc_items.append(item.product_id)

        return _get_product_details(recc_items)


a = ItemRecommendationService()
b = a.get_ibcf_recommendations('P1000637430', 5)

for i in b:
    print(i)
    print("-"*40)