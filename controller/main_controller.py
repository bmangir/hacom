from flask import request, Blueprint, url_for, render_template, jsonify, session, redirect

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


@main_controller_blueprint.route("/login")
def login():
    return redirect(url_for('user_blueprint.login'))


@main_controller_blueprint.route('/search', methods=['GET'])
def search():
    query = request.args.get('q', '').lower()  # Get the query from the request
    if not query:
        return jsonify({"error": "Query parameter is required"}), 400

    # Filter products and categories matching the query
    matching_products = [product for product in DATA["products"] if query in product.lower()]
    matching_categories = [category for category in DATA["categories"] if query in category.lower()]

    return jsonify({
        "products": matching_products,
        "categories": matching_categories
    })

DATA = {
    "products": [
        "Laptop", "Smartphone", "Headphones", "Monitor", "Keyboard", "Mouse", "Tablet", "Smartwatch", "Printer", "Camera"
    ],
    "categories": [
        "Electronics", "Accessories", "Appliances", "Fashion", "Books"
    ]
}