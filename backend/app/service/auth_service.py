import bcrypt
from datetime import datetime, timedelta
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
    def register(email, password, first_name, last_name):
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
                INSERT INTO users (email, password, first_name, last_name, gender, birth_date)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING user_id
                """,
                (email, password_hash.decode('utf-8'), first_name, last_name, "M", "1997-04-09 00:00:00")
            )
            user_id = cursor.fetchone()[0]
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