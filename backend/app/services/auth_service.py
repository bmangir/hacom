import json

import bcrypt
from datetime import timedelta, datetime

import requests
from flask_jwt_extended import create_access_token
from flask import current_app

from databases.postgres.neon_postgres_connector import NeonPostgresConnector


class AuthService:
    @staticmethod
    def login(email, password):
        conn = None
        cursor = None
        try:
            # Get database connection
            conn = NeonPostgresConnector.get_connection()
            if not conn:
                return None, "Database connection failed"

            cursor = conn.cursor()

            # Query user
            cursor.execute(
                "SELECT user_id, email, password FROM users WHERE email = %s",
                (email,)
            )
            user = cursor.fetchone()

            if not user:
                return None, "User not found"

            # Verify password
            if not bcrypt.checkpw(password.encode('utf-8'), user[2].encode('utf-8')):
                return None, "Invalid password"

            # Create access token
            access_token = create_access_token(identity=user[0])

            return {
                "user_id": user[0],
                "email": user[1],
                "access_token": access_token
            }, None

        except Exception as e:
            return None, str(e)
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    @staticmethod
    def register(email, password, first_name, last_name, gender, birthdate, address=None):
        # Validate required frontend data
        if not email or not password or not first_name or not last_name or not gender or not birthdate:
            return None, "Missing required fields"
        
        #birthdate = datetime.strptime(birthdate, "%Y-%m-%d")
        temp_gender = gender
        if gender == "Male" or gender == "male":
            gender = "M"
        elif gender == "Female" or  gender == "female":
            gender = "F"

        conn = None
        cursor = None
        try:
            # Get database connection
            conn = NeonPostgresConnector.get_connection()
            if not conn:
                return None, "Database connection failed"

            cursor = conn.cursor()

            # Check if user exists
            cursor.execute("SELECT user_id FROM users WHERE email = %s", (email,))
            if cursor.fetchone():
                return None, "Email already registered"

            # Hash password
            password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

            # Insert new user
            cursor.execute(
                """
                INSERT INTO users (email, password, first_name, last_name, gender, birth_date, address)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING user_id
                """,
                (email, password_hash.decode('utf-8'), first_name, last_name, gender, birthdate, address)
            )
            user_id = cursor.fetchone()[0]
            
            # Try to validate birthdate format before proceeding with other operations
            try:
                birthdate_obj = datetime.strptime(birthdate, "%Y-%m-%d")
            except ValueError:
                conn.rollback()
                return None, "Invalid birthdate format. Use YYYY-MM-DD format."
            
            # convert user data to json
            user_json = {
                "user_id": user_id,
                "gender": temp_gender,
                "age": int((datetime.now() - birthdate_obj).days / 365),
                "location": address
            }

            # Only commit to database if the API call succeeds
            try:
                response = requests.post(url="http://127.0.0.1:8081/api/v1/stream/new_registration_computer", json=user_json)
                if response.status_code != 200:
                    conn.rollback()
                    return None, "Failed to send registration data to the streaming service"
            except requests.RequestException:
                conn.rollback()
                return None, "Failed to connect to the streaming service"
            
            # If we get here, everything succeeded - commit the transaction
            conn.commit()

            # Create access token
            access_token = create_access_token(identity=user_id)

            return {
                "user_id": user_id,
                "email": email,
                "access_token": access_token
            }, None

        except Exception as e:
            if conn:
                conn.rollback()
            return None, str(e)
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)
