from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from . import login_required
from ..services.service_locator import recommendation_service, tracking_service, wishlist_service
import time

product_controller_blueprint = Blueprint('product_controller_blueprint', __name__)


@product_controller_blueprint.route("/product/<product_id>")
@login_required
def product_details(product_id):
    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track product view with duration
        tracking_service.track_user_action(
            user_id=session.get('user_id'),
            session_id=session.get('session_id'),
            action='view',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={'product_id': product_id}
        )

        # Update last page time
        session['last_page_time'] = current_time

        product = recommendation_service.get_product_details(product_id)
        if not product:
            return "Product not found", 404

        user_id = session.get('user_id')
        in_wishlist = wishlist_service.is_in_wishlist(user_id, product_id)

        # Get similar products and category-based recommendations
        ibcf_similar_products = recommendation_service.recommend_items_by_ibcf(user_id)  # Primary model
        category_products = recommendation_service.get_similar_products_by_category(
            product['category'],
            product_id,
            limit=5
        )
        similar_users_liked_products = recommendation_service.similar_users_liked_recommendations(user_id, top_n=5)
        content_based_recommended_products = recommendation_service.recommend_items_content_based(user_id, top_n=5)

        vector_result = recommendation_service.content_based_recc_by_vectordb(product_id)
        print(vector_result)

        return render_template(
            "product_details.html",
            product=product,
            in_wishlist=in_wishlist,
            similar_products=ibcf_similar_products,
            category_products=category_products,
            similar_users_liked_products=similar_users_liked_products,
            content_based_recommended_products=content_based_recommended_products
        )

    except Exception as e:
        print(f"Error viewing product: {str(e)}")
        return "Error loading product", 500


@product_controller_blueprint.route("/product/<product_id>/reviews")
@login_required
def product_reviews(product_id):
    """Show all reviews for a product"""
    try:
        product = recommendation_service.get_product_details(product_id)
        if not product:
            return "Product not found", 404

        return render_template(
            "product_reviews.html",
            product=product
        )

    except Exception as e:
        print(f"Error loading reviews: {str(e)}")
        return "Error loading reviews", 500


@product_controller_blueprint.route("/search")
@login_required
def search():
    query = request.args.get('query', '')
    if not query:
        return redirect(url_for('main_controller_blueprint.home'))

    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track search action with duration
        tracking_service.track_user_action(
            user_id=session.get('user_id'),
            session_id=session.get('session_id'),
            action='search',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={'search_query': query}
        )

        # Update last page time
        session['last_page_time'] = current_time

        results = recommendation_service.search_products(query)
        return render_template('search_results.html',
                               query=query,
                               products=results.get('products', []))
    except Exception as e:
        print(f"Error searching products: {str(e)}")
        return "Error performing search", 500


@product_controller_blueprint.route("/category/<category_name>")
@login_required
def category_products(category_name):
    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track category view with duration
        tracking_service.track_user_action(
            user_id=session.get('user_id'),
            session_id=session.get('session_id'),
            action='view_category',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={'category': category_name}
        )

        # Update last page time
        session['last_page_time'] = current_time

        products = recommendation_service.get_products_by_category(category_name)
        return render_template("category_products.html",
                               category=category_name,
                               products=products)
    except Exception as e:
        print(f"Error getting category products: {str(e)}")
        return "Error loading category", 500
