from flask import request, Blueprint, jsonify, session, render_template, redirect, url_for
import math

from . import login_required
from ..services.auth_service import AuthService
from ..services.service_locator import recommendation_service, tracking_service, session_service, user_service

user_blueprint = Blueprint('user_blueprint', __name__)


@user_blueprint.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "GET":
        return render_template("register.html")

    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ['email', 'password', 'first_name', 'last_name']
        if not all(field in data for field in required_fields):
            return jsonify({
                "success": False,
                "message": "Missing required fields"
            }), 400

        # Register user
        result, error = AuthService.register(
            data['email'],
            data['password'],
            data['first_name'],
            data['last_name'],
            data['gender'],
            data['birthdate'],
            data['address']
        )

        if error:
            return jsonify({
                "success": False,
                "message": error
            }), 400

        # Track successful registration
        tracking_service.track_user_action(
            user_id=result['user_id'],
            session_id=session.get('session_id'),
            action='register',
            page_url=request.path,
            referrer=request.referrer
        )

        return jsonify({
            "success": True,
            "data": result,
            "message": "Registration successful"
        }), 201

    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500


@user_blueprint.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html")

    try:
        data = request.get_json()

        # Validate required fields
        if not data or 'email' not in data or 'password' not in data:
            return jsonify({
                "success": False,
                "message": "Email and password are required"
            }), 400

        # Login user
        result, error = AuthService.login(data['email'], data['password'])

        if error:
            return jsonify({
                "success": False,
                "message": error
            }), 401

        # Create new session
        session_id = session_service.create_session(result['user_id'], request)
        if not session_id:
            return jsonify({
                "success": False,
                "message": "Error creating session"
            }), 500

        # Store user info in session
        session['user_id'] = result['user_id']
        session['email'] = result['email']
        session['first_name'] = user_service.get_user_first_name(result['user_id'])
        session['session_id'] = session_id

        return jsonify({
            "success": True,
            "data": result,
            "message": "Login successful"
        }), 200

    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500


@user_blueprint.route("/logout")
def logout():
    try:
        # End the session
        if 'session_id' in session:
            session_service.end_session(session['session_id'])

        session.clear()
        return redirect(url_for('main_controller_blueprint.home'))
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500


@user_blueprint.route("/my-profile")
@login_required
def profile():
    user_id = session.get('user_id')

    try:
        # Get recently visited products
        #recent_products = recommendation_service.get_recently_viewed_products(user_id, limit=10)
        recent_products = user_service.get_recently_viewed_products(user_id=user_id, limit=10)

        # Get purchased products
        purchased_products = recommendation_service.get_purchased_products(user_id, limit=10)

        return render_template(
            "user_profile.html",
            recent_products=recent_products,
            purchased_products=purchased_products
        )
    except Exception as e:
        print(f"Error loading profile: {str(e)}")
        return render_template(
            "user_profile.html",
            recent_products=[],
            purchased_products=[]
        )


@user_blueprint.route("/all-visits")
@login_required
def all_visits():
    user_id = session.get('user_id')
    page = request.args.get('page', 1, type=int)
    date_range = request.args.get('date_range', 'all')
    sort_by = request.args.get('sort_by', 'recent')
    per_page = 12  # Number of products per page

    try:
        # Get visited products with filters
        visited_products = user_service.get_visited_products(
            user_id=user_id,
            date_range=date_range,
            sort_by=sort_by,
            page=page,
            per_page=per_page
        )

        # Get pagination info
        total_products = visited_products['total']
        total_pages = (total_products + per_page - 1) // per_page

        has_prev = page > 1
        has_next = page < total_pages

        return render_template(
            "user_all_visits.html",
            visited_products=visited_products.get('products', []),
            page=page,
            total_pages=total_pages,
            has_prev=has_prev,
            has_next=has_next,
            date_range=date_range,
            sort_by=sort_by
        )
    except Exception as e:
        print(f"Error loading visited products: {str(e)}")
        return render_template(
            "user_all_visits.html",
            visited_products=[],
            page=1,
            total_pages=1,
            has_prev=False,
            has_next=False,
            date_range='all',
            sort_by='recent'
        )


@user_blueprint.route("/my-purchases")
@login_required
def all_purchases():
    user_id = session.get('user_id')
    page = request.args.get('page', 1, type=int)
    date_range = request.args.get('date_range', 'all')
    sort_by = request.args.get('sort_by', 'recent')
    per_page = 12  # Number of products per page

    try:
        # Get purchased products with filters
        purchased_products = user_service.get_purchased_products(
            user_id=user_id,
            date_range=date_range,
            sort_by=sort_by,
            page=page,
            per_page=per_page
        )

        # Get pagination info
        #total_products = user_service.get_total_purchased_products(user_id, date_range)
        total_pages = (purchased_products['total'] + per_page - 1) // per_page

        has_prev = page > 1
        has_next = page < total_pages

        return render_template(
            "user_all_purchases.html",
            purchased_products=purchased_products.get('products', []),
            page=page,
            total_pages=total_pages,
            has_prev=has_prev,
            has_next=has_next,
            date_range=date_range,
            sort_by=sort_by
        )
    except Exception as e:
        print(f"Error loading purchase history: {str(e)}")
        return render_template(
            "user_all_purchases.html",
            purchased_products=[],
            page=1,
            total_pages=1,
            has_prev=False,
            has_next=False,
            date_range='all',
            sort_by='recent'
        )


@user_blueprint.route('/visited-products', methods=['GET'])
@login_required
def get_visited_products():
    """Get visited products with filtering and pagination."""
    try:
        user_id = session.get('user_id')
        date_range = request.args.get('date_range', 'all')
        sort_by = request.args.get('sort_by', 'recent')
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 12))

        result = user_service.get_visited_products(
            user_id=user_id,
            date_range=date_range,
            sort_by=sort_by,
            page=page,
            per_page=per_page
        )

        total = result['total']
        total_pages = math.ceil(total / per_page)

        return jsonify({
            'products': result['products'],
            'total': total,
            'total_pages': total_pages,
            'current_page': page
        })

    except Exception as e:
        print(f"Error loading visited products: {str(e)}")
        return jsonify({'error': str(e)}), 500