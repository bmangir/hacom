from flask import request, Blueprint, jsonify, session, render_template, redirect, url_for
from flask_jwt_extended import jwt_required, get_jwt_identity
import uuid

from . import login_required
from ..service.auth_service import AuthService
from ..service.service_locator import recommendation_service
from ..service.session_service import SessionService
from ..service.tracking_service import TrackingService

user_blueprint = Blueprint('user_blueprint', __name__)
session_service = SessionService()
tracking_service = TrackingService()

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
        recent_products = recommendation_service.get_recently_viewed_products(user_id, limit=10)

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