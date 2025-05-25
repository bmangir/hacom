from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from . import login_required
from ..services.service_locator import tracking_service, cart_service, wishlist_service
import time

from backend.utils.utils import _get_product_details

cart_wishlist_controller_blueprint = Blueprint('cart_wishlist_controller_blueprint', __name__)


@cart_wishlist_controller_blueprint.route("/cart/add/<product_id>", methods=['POST'])
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

        result = cart_service.add_to_cart(user_id, product_id, session.get('session_id'), quantity)
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@cart_wishlist_controller_blueprint.route("/cart/update/<cart_id>", methods=['POST', 'GET'])
@login_required
def update_cart_quantity(cart_id):
    try:
        quantity = int(request.args.get('quantity'))  # Get the updated quantity from the request

        result = cart_service.update_quantity(cart_id, quantity)

        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track add to cart action with duration
        tracking_service.track_user_action(
            user_id=session.get('user_id'),
            session_id=session.get('session_id'),
            action='update_cart',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={
                'cart_id': cart_id,
                'quantity': quantity
            }
        )

        if result.get("success"):
            return redirect(url_for("cart_wishlist_controller_blueprint.view_cart"))
        else:
            raise Exception
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@cart_wishlist_controller_blueprint.route("/cart/remove/<product_id>", methods=['POST'])
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

        result = cart_service.remove_from_cart(user_id, product_id, session.get('session_id'))
        if result['success']:
            return redirect(url_for('cart_wishlist_controller_blueprint.view_cart'))

        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@cart_wishlist_controller_blueprint.route("/cart")
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
            product = _get_product_details([item['product_id']])[0]
            if product:
                product['quantity'] = item['quantity']
                product['cart_id'] = item['cart_id']
                total += product['price'] * item['quantity']
                products.append(product)

        return render_template("cart.html", cart_items=products, total=total)
    except Exception as e:
        print(f"Error viewing cart: {str(e)}")
        return "Error loading cart", 500


@cart_wishlist_controller_blueprint.route("/wishlist/add/<product_id>", methods=['POST'])
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

        result = wishlist_service.add_to_wishlist(user_id, product_id, session.get('session_id'))
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@cart_wishlist_controller_blueprint.route("/wishlist/remove/<product_id>", methods=['POST'])
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

        result = wishlist_service.remove_from_wishlist(user_id, product_id, session.get('session_id'))
        if result.get("success"):
            return redirect(url_for("cart_wishlist_controller_blueprint.view_wishlist"))
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@cart_wishlist_controller_blueprint.route("/wishlist")
@login_required
def view_wishlist():
    try:
        user_id = session.get('user_id')
        
        # Track wishlist view
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None
        
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='view_wishlist',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration
        )
        
        # Update last page time
        session['last_page_time'] = current_time
        
        wishlist_response = wishlist_service.get_wishlist_items(user_id)  # Get the wishlist items

        if not wishlist_response['success']:
            return "Error loading wishlist", 500

        wishlist_items = wishlist_response['items']  # Extract items from the response
        products = []
        #for item in wishlist_items:
        #    product = _get_product_details(item['product_id'])
        #    if product:
        #        products.append(product)

        return render_template("wishlist.html", wishlist_items=wishlist_items)
    except Exception as e:
        print(f"Error viewing wishlist: {str(e)}")
        return "Error loading wishlist", 500


@cart_wishlist_controller_blueprint.route("/wishlist/clear", methods=['POST'])
@login_required
def clear_wishlist():
    try:
        user_id = session.get('user_id')

        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track clear wishlist action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='clear_wishlist',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration
        )

        # Update last page time
        session['last_page_time'] = current_time

        result = wishlist_service.clear_wishlist(user_id, session.get('session_id'))
        if result.get("success"):
            return redirect(url_for("cart_wishlist_controller_blueprint.view_wishlist"))
        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500