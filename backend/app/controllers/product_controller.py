from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from . import login_required
from ..services.service_locator import ITEM_RECOMMENDATION_SERVICE, tracking_service, wishlist_service, \
    USER_RECOMMENDATION_SERVICE, review_service, PRODUCT_SERVICE
import time

product_controller_blueprint = Blueprint('product_controller_blueprint', __name__)


@product_controller_blueprint.route("/product/<product_id>")
@login_required
def display_details(product_id):
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

        product = PRODUCT_SERVICE.get_product_details(product_id)
        if not product:
            return "Product not found", 404

        user_id = session.get('user_id')
        in_wishlist = wishlist_service.is_in_wishlist(user_id, product_id)

        # Get top 5 reviews
        all_reviews = review_service.get_product_reviews(product_id)
        reviews = all_reviews[:5] if all_reviews else []
        total_reviews = len(all_reviews)

        # Get similar products and category-based recommendations
        ibcf_similar_products = ITEM_RECOMMENDATION_SERVICE.get_ibcf_recommendations(product_id)  # Primary model
        category_products = ITEM_RECOMMENDATION_SERVICE.get_top_n_product_based_on_category(item_id=product_id, category=product['category'])
        similar_users_liked_products = ITEM_RECOMMENDATION_SERVICE.get_reviewed_based_items()
        content_based_recommended_products = USER_RECOMMENDATION_SERVICE.get_content_based_recommendations(user_id=session["user_id"], num_recommendations=5)
        bought_together = ITEM_RECOMMENDATION_SERVICE.get_bought_together_items(product_id)
        
        # Get additional recommendations
        new_arrivals = ITEM_RECOMMENDATION_SERVICE.get_new_arrivals_items()
        best_sellers = ITEM_RECOMMENDATION_SERVICE.get_best_seller_items()
        seasonal_items = ITEM_RECOMMENDATION_SERVICE.get_seasonal_recommended_items()

        return render_template(
            "product_details.html",
            product=product,
            reviews=reviews,
            total_reviews=total_reviews,
            in_wishlist=in_wishlist,
            similar_products=ibcf_similar_products,
            category_products=category_products,
            similar_users_liked_products=similar_users_liked_products,
            content_based_recommended_products=content_based_recommended_products,
            bought_together=bought_together,
            new_arrivals=new_arrivals,
            best_sellers=best_sellers,
            seasonal_items=seasonal_items
        )

    except Exception as e:
        print(f"Error viewing product: {str(e)}")
        return "Error loading product", 500



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

        # Get search results with query refinement
        results = ITEM_RECOMMENDATION_SERVICE.search(query)
        return render_template('search_results.html',
                               query=query,
                               products=results)
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

        products = PRODUCT_SERVICE.get_products_by_category(category_name)
        return render_template("category_products.html",
                               category=category_name,
                               products=products)
    except Exception as e:
        print(f"Error getting category products: {str(e)}")
        return "Error loading category", 500
