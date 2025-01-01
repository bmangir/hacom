import pickle

import pandas as pd
from flask import request, Blueprint, url_for, render_template, jsonify, session, redirect

from controller import login_required
from service import user_service

user_blueprint = Blueprint('user_blueprint', __name__)


@user_blueprint.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        print("Headers:", request.headers)  # Log request headers
        print("Raw Data:", request.data)  # Log raw request body
        data = request.get_json()
        print("Parsed JSON:", data)  # Log parsed JSON
        data = request.get_json()
        print(data)
        first_name = data['first_name']
        last_name = data['last_name']
        email = data['email']
        password = data['password']
        gender = data['gender']
        birthdate = data['birthdate']
        address = data['address']

        print(first_name, last_name, email, password, gender, birthdate, address)
        try:
            return jsonify({"success": True, "message": "Registered Successful"})
        except Exception:
            return jsonify({"success": False, "message": "Username or email is already used!"})

    return render_template('register.html')


@user_blueprint.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        login_data = request.get_json()

        email = login_data.get("email")
        password = login_data.get("password")

        try:
            return jsonify({"success": True, "email": email})
        except Exception:
            return jsonify({"success": False, "error": "Username or password is wrong!"})

    return render_template('login.html')


@user_blueprint.route("/logout")
@user_blueprint.route("/signout")
@user_blueprint.route("/sign-out")
@login_required
def logout():
    session.clear()
    return redirect(url_for("index"))


@user_blueprint.route('/abc')
def homepage():
    try:
        return render_template('logged_page.html', username="BERKANT")
    except Exception:
        return redirect(url_for('user_blueprint.login'))


@user_blueprint.route("/cosine")
def recommends():
    id = request.args.get("id")
    top_n = request.args.get("top", default=5, type=int)
    return user_service.recommend(id, top_n)