from flask import Flask, send_from_directory, session, request
from flask_jwt_extended import JWTManager
from datetime import timedelta
import os
import sys
import time

from databases.postgres.neon_postgres_connector import NeonPostgresConnector

# Initialize database connections with logging
try:
    NeonPostgresConnector.initialize_connection_pool()
    print("PostgreSQL connection pool initialized successfully")
except Exception as e:
    print(f"Error initializing PostgreSQL connection pool: {e}")

# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app'))

from databases.mongo.mongo_connector import MongoConnector
from app.controller.user_controller import user_blueprint
from app.controller.main_controller import main_controller_blueprint
from app.controller.product_controller import product_controller_blueprint
from app.controller.merchant_controller import merchant_controller_blueprint
from config.config import JWT_SECRET_KEY, JWT_ACCESS_TOKEN_EXPIRES
from app.service.service_locator import tracking_service  # Import tracking_service for request tracking

app = Flask(__name__,
           static_folder='../frontend/static',
           template_folder='../frontend/templates')

# Flask session configuration
app.secret_key = JWT_SECRET_KEY

# JWT Configuration
app.config['JWT_SECRET_KEY'] = JWT_SECRET_KEY
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(seconds=int(JWT_ACCESS_TOKEN_EXPIRES))
jwt = JWTManager(app)

# Register blueprints
app.register_blueprint(user_blueprint)
app.register_blueprint(main_controller_blueprint)
app.register_blueprint(product_controller_blueprint)
app.register_blueprint(merchant_controller_blueprint)

@app.before_request
def track_request():
    """Track every request to track user browsing."""
    if 'user_id' in session:
        current_time = time.time()
        last_page_time = session.get('last_page_time')
        
        # Use the global tracking_service
        tracking_service.track_browsing_history(
            user_id=session['user_id'],
            session_id=session.get('session_id'),
            page_url=request.path,
            referrer_url=request.referrer,
            last_page_time=last_page_time
        )
        
        session['last_page_time'] = current_time

@app.after_request
def after_request(response):
    """Update last page time after each request."""
    if 'user_id' in session and request.endpoint != 'static':
        session['last_page_time'] = time.time()
    return response

# Add route for serving static files
@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('../frontend/static', path)

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8080, debug=True)
