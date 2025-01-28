from flask import request, Blueprint, url_for, render_template, jsonify, session, redirect
from ..controller import login_required
from ..service.service_locator import recommendation_service, tracking_service

main_controller_blueprint = Blueprint('main_controller_blueprint', __name__)

@main_controller_blueprint.before_request
def track_request():
    """Track every request to track user browsing."""
    if 'user_id' in session:
        tracking_service.track_browsing_history(
            user_id=session['user_id'],
            session_id=session.get('session_id'),
            page_url=request.path,
            referrer_url=request.referrer
        )

@main_controller_blueprint.route("/")
@main_controller_blueprint.route("/home")
def home():
    if 'email' in session:
        user_id = session.get('user_id')
        recommended_products = []
        most_visited_categories = []
        
        if user_id:
            try:
                # Get recommendations directly from service
                recommended_products = recommendation_service.recommend_items(user_id, top_n=5)
                most_visited_categories = recommendation_service.get_most_visited_categories(user_id, top_n=5)
            except Exception as e:
                print(f"Error getting recommendations: {str(e)}")
            
        return render_template(
            "logged_page.html",
            recommended_products=recommended_products,
            most_visited_categories=most_visited_categories
        )
    return render_template("main.html")

@main_controller_blueprint.route("/about")
def about():
    return render_template("navigations/about.html")

@main_controller_blueprint.route("/help")
def help():
    return render_template("navigations/help.html")

@main_controller_blueprint.route("/contact")
def contact():
    return render_template("navigations/contact.html")

@main_controller_blueprint.route("/api/recommendations", methods=['GET'])
@login_required
def get_recommendations():
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({
            "success": False,
            "message": "User not found"
        }), 404
        
    try:
        recommended_products = recommendation_service.recommend_items(user_id, top_n=5)
        most_visited_categories = recommendation_service.get_most_visited_categories(user_id, top_n=5)
        
        return jsonify({
            "success": True,
            "data": {
                "recommended_products": recommended_products,
                "most_visited_categories": most_visited_categories
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500