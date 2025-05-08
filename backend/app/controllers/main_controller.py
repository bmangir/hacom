from flask import request, Blueprint, url_for, render_template, jsonify, session, redirect

from backend.app.services.service_locator import USER_RECOMMENDATION_SERVICE, tracking_service

main_controller_blueprint = Blueprint('main_controller_blueprint', __name__)


@main_controller_blueprint.route("/about")
def about():
    return render_template("navigations/about.html")


@main_controller_blueprint.route("/help")
def help():
    return render_template("navigations/help.html")


@main_controller_blueprint.route("/contact")
def contact():
    return render_template("navigations/contact.html")


@main_controller_blueprint.route("/")
@main_controller_blueprint.route("/home")
def home():
    if 'email' in session:
        user_id = session.get('user_id')
        ubcf_recommended_products = []
        most_visited_categories = []
        cb_recommended_products = []
        if user_id:
            try:
                ubcf_recommended_products = USER_RECOMMENDATION_SERVICE.get_ubcf_recommendations(user_id=user_id, num_recommendations=10)
                cb_recommended_products = USER_RECOMMENDATION_SERVICE.get_content_based_recommendations(user_id=user_id, num_recommendations=5)
                most_visited_categories = USER_RECOMMENDATION_SERVICE.get_most_visited_categories(user_id=user_id)
            except Exception as e:
                print(f"Error getting recommendations: {str(e)}")
        return render_template(
            "logged_page.html",
            recommended_products=ubcf_recommended_products,
            most_visited_categories=most_visited_categories,
            content_based_reccommendations=cb_recommended_products
        )
    return render_template("main.html")