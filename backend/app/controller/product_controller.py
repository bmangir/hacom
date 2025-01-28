from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from ..controller import login_required
from ..service.service_locator import recommendation_service, tracking_service
from ..service.cart_service import CartService
from ..service.wishlist_service import WishlistService
import time

product_controller_blueprint = Blueprint('product_controller_blueprint', __name__)

# Keep cart and wishlist services as they're stateless
cart_service = CartService()
wishlist_service = WishlistService()

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
        similar_products = recommendation_service.get_similar_products(product_id, limit=5)
        category_products = recommendation_service.get_similar_products_by_category(
            product['category'], 
            product_id, 
            limit=5
        )
        
        return render_template(
            "product_details.html",
            product=product,
            in_wishlist=in_wishlist,
            similar_products=similar_products,
            category_products=category_products
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

@product_controller_blueprint.route("/cart/add/<product_id>", methods=['POST'])
@login_required
def add_to_cart(product_id):
    try:
        user_id = session.get('user_id')
        quantity = request.json.get('quantity', 1)
        
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None
        
        # Track add to cart action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='add_to_cart',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={
                'product_id': product_id,
                'quantity': quantity
            }
        )
        
        # Update last page time
        session['last_page_time'] = current_time
        
        result = cart_service.add_to_cart(user_id, product_id, quantity)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@product_controller_blueprint.route("/cart/update/<product_id>", methods=['POST'])
@login_required
def update_cart_quantity(product_id):
    try:
        user_id = session.get('user_id')
        quantity = request.json.get('quantity', 1)  # Get the updated quantity from the request
        
        result = cart_service.update_quantity(user_id, product_id, quantity)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@product_controller_blueprint.route("/cart/remove/<product_id>", methods=['POST'])
@login_required
def remove_from_cart(product_id):
    try:
        user_id = session.get('user_id')
        
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None
        
        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='remove_from_cart',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={'product_id': product_id}
        )
        
        # Update last page time
        session['last_page_time'] = current_time
        
        result = cart_service.remove_from_cart(user_id, product_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@product_controller_blueprint.route("/cart")
@login_required
def view_cart():
    try:
        user_id = session.get('user_id')
        cart_response = cart_service.get_cart_items(user_id)
        
        if not cart_response['success']:
            return "Error loading cart", 500
        
        cart_items = cart_response['items']
        products = []
        total = 0
        
        for item in cart_items:
            product = recommendation_service.get_product_details(item['product_id'])
            if product:
                product['quantity'] = item['quantity']
                total += product['price'] * item['quantity']
                products.append(product)
        
        return render_template("cart.html", cart_items=products, total=total)
    except Exception as e:
        print(f"Error viewing cart: {str(e)}")
        return "Error loading cart", 500

@product_controller_blueprint.route("/wishlist/add/<product_id>", methods=['POST'])
@login_required
def add_to_wishlist(product_id):
    try:
        user_id = session.get('user_id')
        
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None
        
        # Track add to wishlist action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='add_to_wishlist',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={'product_id': product_id}
        )
        
        # Update last page time
        session['last_page_time'] = current_time
        
        result = wishlist_service.add_to_wishlist(user_id, product_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@product_controller_blueprint.route("/wishlist/remove/<product_id>", methods=['POST'])
@login_required
def remove_from_wishlist(product_id):
    try:
        user_id = session.get('user_id')
        
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None
        
        # Track remove from wishlist action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='remove_from_wishlist',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={'product_id': product_id}
        )
        
        # Update last page time
        session['last_page_time'] = current_time
        
        result = wishlist_service.remove_from_wishlist(user_id, product_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@product_controller_blueprint.route("/wishlist")
@login_required
def view_wishlist():
    try:
        user_id = session.get('user_id')
        wishlist_response = wishlist_service.get_wishlist_items(user_id)  # Get the wishlist items
        
        if not wishlist_response['success']:
            return "Error loading wishlist", 500
        
        wishlist_items = wishlist_response['items']  # Extract items from the response
        products = []
        for item in wishlist_items:
            product = recommendation_service.get_product_details(item['product_id'])
            if product:
                products.append(product)
                
        return render_template("wishlist.html", wishlist_items=products)
    except Exception as e:
        print(f"Error viewing wishlist: {str(e)}")
        return "Error loading wishlist", 500 