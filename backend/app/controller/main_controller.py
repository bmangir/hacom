from flask import request, Blueprint, url_for, render_template, jsonify, session, redirect
from app.controller import login_required
from app.service.recommendation_service import RecommendationService

main_controller_blueprint = Blueprint('main_controller_blueprint', __name__)
recommendation_service = RecommendationService()

@main_controller_blueprint.route("/")
@main_controller_blueprint.route("/home")
def home():
    if 'email' in session:
        user_id = session.get('user_id')
        recommended_products = []
        most_visited_categories = []
        
        if user_id:
            recommended_products = recommendation_service.recommend_items(user_id, top_n=5)
            most_visited_categories = recommendation_service.get_most_visited_categories(user_id, top_n=5)
            
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

@main_controller_blueprint.route("/my-profile")
@login_required
def profile():
    return render_template("user_profile.html")

@main_controller_blueprint.route("/api/recommendations", methods=['GET'])
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