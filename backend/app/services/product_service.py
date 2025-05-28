from backend.app.models.user_models import ProductDetails
from backend.utils.utils import _get_product_details


class ProductService:
    def __init__(self, cache):
        self.cache = cache

    def get_product_details(self, product_id: str):
        # Try to get from cache first
        cached_item = self.cache.get_item_recommendations(product_id, "details")
        if cached_item:
            return cached_item[0]  # Return first item since it's a single product

        # If not in cache, get from MongoDB
        item = _get_product_details([product_id])[0]

        if item:
            self.cache.set_item_recommendations(product_id, [item], "details")

        return item

    def get_products_by_category(self, category: str, num_recommendations: int = 20):
        category = category.title()
        if category == "Fashion":
            category = "Clothing"
        elif category == "Electronics":
            category = "Computers"
        elif category == "Phones & Tablets":
            category = "Mobile Devices"
        elif category == "Cameras":
            category = "Photography"
        elif category == "Toys & Games":
            category = "Audio"

        cached_items = self.cache.get_category_items(category, "category")
        if cached_items:
            return cached_items

        # If not in cache, get from MongoDB
        items = ProductDetails.objects(category=category).limit(num_recommendations)
        recc_items = []
        for item in items:
            recc_items.append(item.product_id)

        result = _get_product_details(recc_items)
        result.sort(key=lambda x: x.get('rating', 0), reverse=True)

        # Cache the results
        if result:
            self.cache.set_category_items(category, result, "category")

        return result