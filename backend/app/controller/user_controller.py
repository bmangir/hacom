from flask import request, Blueprint, jsonify, session, render_template, redirect, url_for
from flask_jwt_extended import jwt_required, get_jwt_identity

from ..service.auth_service import AuthService

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
            data['last_name']
        )
        
        if error:
            return jsonify({
                "success": False,
                "message": error
            }), 400
            
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
            
        # Store user info in session
        session['user_id'] = result['user_id']
        session['email'] = result['email']
            
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
        session.clear()
        return redirect(url_for('main_controller_blueprint.home'))
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500

@user_blueprint.route("/profile")
@jwt_required()
def get_profile():
    try:
        user_id = get_jwt_identity()
        # Here you would typically fetch user profile data
        return jsonify({
            "success": True,
            "data": {
                "user_id": user_id
            }
        }), 200
    except Exception as e:
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500