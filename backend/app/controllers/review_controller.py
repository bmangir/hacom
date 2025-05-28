import time
from datetime import datetime
from flask import Blueprint, request, jsonify, render_template, redirect, url_for, flash, session

from backend.app.controllers import login_required
from backend.app.services.service_locator import ITEM_RECOMMENDATION_SERVICE, PRODUCT_SERVICE, review_service, \
    tracking_service

review_bp = Blueprint('review', __name__)

@review_bp.app_template_filter('datetime')
def format_datetime(value):
    """Format a timestamp to a readable date and time."""
    if not value:
        return ''
    if isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value)
    else:
        dt = value
    return dt.strftime('%B %d, %Y at %I:%M %p')


@review_bp.route("/product/<product_id>/reviews")
@login_required
def display_reviews(product_id):
    """Show all reviews for a product"""
    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=session.get('user_id'),
            session_id=session.get('session_id'),
            action='view_product_reviews',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={"product_id": product_id}
        )

        product = PRODUCT_SERVICE.get_product_details(product_id)
        if not product:
            return "Product not found", 404

        # Get all reviews
        reviews = review_service.get_product_reviews(product_id)
        review_based_reccs = ITEM_RECOMMENDATION_SERVICE.get_reviewed_based_items()

        return render_template(
            "product_reviews.html",
            product=product,
            reviews=reviews,
            review_based_reccs=review_based_reccs
        )

    except Exception as e:
        print(f"Error loading reviews: {str(e)}")
        return "Error loading reviews", 500


@review_bp.route("/product/<product_id>/reviews/form", methods=['GET'])
@login_required
def review_form(product_id):
    """Show review form for create or update"""
    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=session.get('user_id'),
            session_id=session.get('session_id'),
            action='create_or_update_review',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={"product_id": product_id}
        )

        product = PRODUCT_SERVICE.get_product_details(product_id)
        if not product:
            return jsonify({"status": "error", "error": "Product not found"}), 404

        # Get the existing review if any
        review = review_service.get_user_review_for_product(session.get('user_id'), product_id)

        return render_template(
            "review_update.html",
            product=product,
            review=review,
            is_update=review is not None
        )

    except Exception as e:
        print(f"Error loading review form: {str(e)}")
        flash("Error loading review form", "error")
        return redirect(url_for('user_blueprint.my_reviews'))


@review_bp.route("/product/<product_id>/reviews/submit", methods=['POST'])
@login_required
def submit_review(product_id):
    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=session.get('user_id'),
            session_id=session.get('session_id'),
            action='submit_review',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={"product_id": product_id}
        )

        rating = request.form.get('rating')
        comment = request.form.get('comment')

        if not product_id or not rating or not comment:
            return jsonify({"status": "error", "error": "All fields are required"}), 400

        # Create or update the review
        review_service.create_or_update_review(
            product_id=product_id,
            user_id=session.get('user_id'),
            rating=rating,
            comment=comment
        )

        return jsonify({"status": "success", "message": "Review submitted successfully"})

    except Exception as e:
        print(f"Error submitting review: {str(e)}")
        return jsonify({"status": "error", "error": str(e)}), 500


@review_bp.route("/product/<product_id>/reviews/delete", methods=['POST'])
@login_required
def delete_review(product_id):
    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=session.get('user_id'),
            session_id=session.get('session_id'),
            action='delete_review',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={"product_id": product_id}
        )

        if not product_id:
            return jsonify({"status": "error", "error": "Product ID is required"}), 400

        # Delete the review
        review_service.delete_review(product_id, user_id=session.get('user_id'))

        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"status": "success", "message": "Review deleted successfully"})
        
        flash("Review deleted successfully", "success")
        return redirect(url_for('user_blueprint.my_reviews'))

    except Exception as e:
        print(f"Error deleting review: {str(e)}")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({"status": "error", "error": str(e)}), 500
            
        flash("Error deleting review", "error")
        return redirect(url_for('review.display_reviews', product_id=product_id))