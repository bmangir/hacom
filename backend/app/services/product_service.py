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