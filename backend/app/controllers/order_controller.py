from flask import Blueprint, jsonify, request, render_template, session, redirect, url_for
from . import login_required
from ..services.service_locator import cart_service, order_service
from ..services.order_service import OrderService
from ..services.cart_service import CartService

order_bp = Blueprint('order', __name__)


@order_bp.route('/checkout', methods=['GET'])
@login_required
def checkout_page():
    """Render the checkout page."""
    try:
        cart_items = cart_service.get_cart_items(session['user_id'])
        if not cart_items.get("items"):
            return redirect(url_for('cart_wishlist_controller_blueprint.view_cart'))
        
        # Calculate totals
        #subtotal = sum(item['price'] * item['quantity'] for item in cart_items)
        subtotal = 0
        for item in cart_items.get('items'):
            subtotal += item['price'] * item['quantity']
        shipping_cost = 10.00  # Fixed shipping cost for now
        total = subtotal + shipping_cost
        
        return render_template('checkout.html',
                             cart_items=cart_items.get("items"),
                             subtotal=subtotal,
                             shipping_cost=shipping_cost,
                             total=total)
    except Exception as e:
        print(f"Error loading checkout page: {str(e)}")
        return jsonify({'error': 'Failed to load checkout page'}), 500


@order_bp.route('/api/checkout', methods=['POST'])
@login_required
def process_checkout():
    """Process the checkout and create a new order."""
    try:
        user_id = session['user_id']
        data = request.get_json()
        
        # Get cart items
        cart_items = cart_service.get_cart_items(user_id)
        
        if not cart_items:
            return jsonify({'error': 'Cart is empty'}), 400
        
        # Create order
        order = order_service.create_order(
            user_id=user_id,
            cart_items=cart_items.get("items"),
            shipping_info=data['shipping_info'],
            payment_method=data['payment_method']
        )
        
        # Clear cart after successful order
        cart_service.clear_cart(user_id)
        
        return jsonify({
            'message': 'Order placed successfully',
            'order_id': order['order_id']
        })
        
    except Exception as e:
        print(f"Error processing checkout: {str(e)}")
        return jsonify({'error': 'Failed to process checkout'}), 500


@order_bp.route('/order-confirmation/<order_id>')
@login_required
def order_confirmation(order_id):
    """Show the order confirmation page."""
    try:
        order = order_service.get_order(order_id)
        
        if not order or order['user_id'] != session['user_id']:
            return "Order not found", 404

        print(type(order.items()))
        return render_template('order_confirmation.html', order=order)
        
    except Exception as e:
        print(f"Error loading order confirmation: {str(e)}")
        return "Error loading order confirmation", 500


@order_bp.route('/api/orders/<order_id>/status', methods=['PUT'])
@login_required
def update_order_status(order_id):
    """Update the status of an order."""
    try:
        data = request.get_json()
        new_status = data.get('status')
        
        if not new_status:
            return jsonify({'error': 'Status is required'}), 400
            
        success = order_service.update_order_status(order_id, new_status)
        
        if success:
            return jsonify({'message': 'Order status updated successfully'})
        else:
            return jsonify({'error': 'Failed to update order status'}), 500
            
    except Exception as e:
        print(f"Error updating order status: {str(e)}")
        return jsonify({'error': 'Failed to update order status'}), 500 