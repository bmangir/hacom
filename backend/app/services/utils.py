from app.models.user_recc_model import ProductDetails


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
            'material': product_info.material
        })

    return recommended_products