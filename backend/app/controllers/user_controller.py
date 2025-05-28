import time

from flask import request, Blueprint, jsonify, session, render_template, redirect, url_for
import math

from . import login_required
from ..services.auth_service import AuthService
from ..services.service_locator import tracking_service, session_service, user_service, USER_RECOMMENDATION_SERVICE, ITEM_RECOMMENDATION_SERVICE

user_blueprint = Blueprint('user_blueprint', __name__)


@user_blueprint.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "GET":
        return render_template("register.html")

    try:
        data = request.get_json()
        if not data:
            return jsonify({
                "success": False,
                "message": "No data provided"
            }), 400

        # Validate required fields
        required_fields = ['email', 'password', 'first_name', 'last_name', 'gender', 'birthdate']
        missing_fields = [field for field in required_fields if field not in data or not data[field]]
        if missing_fields:
            return jsonify({
                "success": False,
                "message": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400

        # Register user
        result, error = AuthService.register(
            data['email'],
            data['password'],
            data['first_name'],
            data['last_name'],
            data['gender'],
            data['birthdate'],
            data.get('address')  # Address is optional
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
        print(f"Registration error: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Registration failed: {str(e)}"
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

        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=session['user_id'],
            session_id=session.get('session_id'),
            action='login',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={}
        )

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

        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=session['user_id'],
            session_id=session.get('session_id'),
            action='logout',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={}
        )
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
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='view_profile',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={}
        )

        # Get recently visited products
        recent_products_result = user_service.get_visited_products(user_id=user_id)
        recent_products = recent_products_result.get('products', [])[:10] if recent_products_result else []

        # Get purchased products
        purchased_products_result = user_service.get_purchased_products(user_id)
        purchased_products = purchased_products_result if purchased_products_result else {'orders': [], 'total': 0}

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
            purchased_products={'orders': [], 'total': 0}
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
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='view_all_visits',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={}
        )

        # Get visited products with filters
        visited_products = USER_RECOMMENDATION_SERVICE.get_visited_products(
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

        # Get recommended items based on recently viewed products
        recc_items_based_recently_viewed = USER_RECOMMENDATION_SERVICE.get_recently_viewed_based_recommendations(user_id, 8)

        return render_template(
            "user_all_visits.html",
            visited_products=visited_products.get('products', []),
            page=page,
            total_pages=total_pages,
            has_prev=has_prev,
            has_next=has_next,
            date_range=date_range,
            sort_by=sort_by,
            recommended_products=recc_items_based_recently_viewed
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
            sort_by='recent',
            recommended_products=[]
        )


@user_blueprint.route("/my-purchases")
@login_required
def all_purchases():
    user_id = session.get('user_id')
    page = request.args.get('page', 1, type=int)
    date_range = request.args.get('date_range', 'all')
    sort_by = request.args.get('sort_by', 'recent')
    per_page = 12  # Number of orders per page

    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='view_all_purchases',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={}
        )

        # Get purchased orders with filters
        result = user_service.get_purchased_products(
            user_id=user_id,
            date_range=date_range,
            sort_by=sort_by,
            page=page,
            per_page=per_page
        )
        orders = result.get('orders', [])
        total_orders = result.get('total', 0)
        total_pages = (total_orders + per_page - 1) // per_page

        has_prev = page > 1
        has_next = page < total_pages

        return render_template(
            "user_all_purchases.html",
            orders=orders,
            page=page,
            total_pages=total_pages,
            has_prev=has_prev,
            has_next=has_next,
            date_range=date_range,
            sort_by=sort_by,
            active_page='purchases'
        )
    except Exception as e:
        print(f"Error loading purchase history: {str(e)}")
        return render_template(
            "user_all_purchases.html",
            orders=[],
            page=1,
            total_pages=1,
            has_prev=False,
            has_next=False,
            date_range='all',
            sort_by='recent',
            active_page='purchases'
        )


@user_blueprint.route("/my-reviews")
@login_required
def my_reviews():
    user_id = session.get('user_id')
    page = request.args.get('page', 1, type=int)
    date_range = request.args.get('date_range', 'all')
    sort_by = request.args.get('sort_by', 'recent')
    per_page = 12  # Number of reviews per page

    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='view_my_reviews',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={}
        )

        # Get user reviews with filters
        reviews = user_service.get_user_reviews(
            user_id=user_id,
            date_range=date_range,
            sort_by=sort_by,
            page=page,
            per_page=per_page
        )

        # Get recommended items based on reviews
        recommended_items = ITEM_RECOMMENDATION_SERVICE.get_reviewed_based_items()

        # Get pagination info
        total_pages = (reviews['total'] + per_page - 1) // per_page

        has_prev = page > 1
        has_next = page < total_pages

        return render_template(
            "user_reviews.html",
            reviews=reviews.get('reviews', []),
            page=page,
            total_pages=total_pages,
            has_prev=has_prev,
            has_next=has_next,
            date_range=date_range,
            sort_by=sort_by,
            recommended_items=recommended_items
        )
    except Exception as e:
        print(f"Error loading reviews: {str(e)}")
        return render_template(
            "user_reviews.html",
            reviews=[],
            page=1,
            total_pages=1,
            has_prev=False,
            has_next=False,
            date_range='all',
            sort_by='recent',
            recommended_items=[]
        )


@user_blueprint.route("/my-profile/settings", methods=["GET", "POST"])
@login_required
def settings():
    user_id = session.get('user_id')
    
    if request.method == "POST":
        try:
            data = request.get_json()
            if not data:
                return jsonify({
                    "success": False,
                    "message": "No data provided"
                }), 400

            # Update user settings
            result, error = user_service.update_user_settings(user_id, data)
            
            if error:
                return jsonify({
                    "success": False,
                    "message": error
                }), 400

            current_time = time.time()
            last_page_time = session.get('last_page_time')
            duration = int(current_time - last_page_time) if last_page_time else None

            # Track remove from cart action with duration
            tracking_service.track_user_action(
                user_id=user_id,
                session_id=session.get('session_id'),
                action='update_settings',
                page_url=request.path,
                referrer=request.referrer,
                duration_seconds=duration,
                additional_data={}
            )
            return jsonify({
                "success": True,
                "message": "Settings updated successfully"
            }), 200

        except Exception as e:
            return jsonify({
                "success": False,
                "message": str(e)
            }), 500

    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='seek_settings',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={}
        )

        # Get user settings
        settings = user_service.get_user_settings(user_id)
        return render_template("user_settings.html", settings=settings)
    except Exception as e:
        print(f"Error loading settings: {str(e)}")
        return render_template("user_settings.html", settings={})


@user_blueprint.route("/my-profile/details")
@login_required
def profile_details():
    user_id = session.get('user_id')
    
    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='view_profile_details',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={}
        )

        # Get user details
        user_details = user_service.get_user_details(user_id)
        return render_template("user_details.html", user=user_details)
    except Exception as e:
        print(f"Error loading user details: {str(e)}")
        return render_template("user_details.html", user={})


@user_blueprint.route("/my-profile/notifications")
@login_required
def notifications():
    user_id = session.get('user_id')
    page = request.args.get('page', 1, type=int)
    per_page = 10  # Number of notifications per page

    try:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        duration = int(current_time - last_page_time) if last_page_time else None

        # Track remove from cart action with duration
        tracking_service.track_user_action(
            user_id=user_id,
            session_id=session.get('session_id'),
            action='view_notifications',
            page_url=request.path,
            referrer=request.referrer,
            duration_seconds=duration,
            additional_data={}
        )

        # Get user notifications
        #notifications = user_service.get_user_notifications(
        #    user_id=user_id,
        #    page=page,
        #    per_page=per_page
        #)
        notifications = {"notifications": [], "total": 0}

        # Get pagination info
        total_pages = (notifications['total'] + per_page - 1) // per_page
        has_prev = page > 1
        has_next = page < total_pages

        return render_template(
            "user_notifications.html",
            notifications=notifications.get('notifications', []),
            page=page,
            total_pages=total_pages,
            has_prev=has_prev,
            has_next=has_next
        )
    except Exception as e:
        print(f"Error loading notifications: {str(e)}")
        return render_template(
            "user_notifications.html",
            notifications=[],
            page=1,
            total_pages=1,
            has_prev=False,
            has_next=False
        )