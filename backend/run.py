import os
import sys

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from flask import Flask, send_from_directory, session, request
from flask_jwt_extended import JWTManager
from datetime import timedelta
import time
from flask_cors import CORS

from databases.mongo.mongo_connector import MongoConnector
from databases.postgres.neon_postgres_connector import NeonPostgresConnector
from backend.app.controllers.user_controller import user_blueprint
from backend.app.controllers.main_controller import main_controller_blueprint
from backend.app.controllers.product_controller import product_controller_blueprint
from backend.app.controllers.cart_wishlist_controller import cart_wishlist_controller_blueprint
from backend.app.controllers.order_controller import order_bp
from backend.app.controllers.review_controller import review_bp
from backend.redis_app.controllers.redis_controller import redis_main_controller_blueprint
from backend.app.services.service_locator import tracking_service
# Add parent directory to Python path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app'))

from databases.mongo.mongo_connector import MongoConnector
from databases.postgres.neon_postgres_connector import NeonPostgresConnector
from app.controllers.user_controller import user_blueprint
from app.controllers.main_controller import main_controller_blueprint
from app.controllers.product_controller import product_controller_blueprint
from app.controllers.cart_wishlist_controller import cart_wishlist_controller_blueprint
from app.controllers.order_controller import order_bp
from app.controllers.review_controller import review_bp
from redis_app.controllers.redis_controller import redis_main_controller_blueprint
from app.services.service_locator import tracking_service
from config import JWT_SECRET_KEY, JWT_ACCESS_TOKEN_EXPIRES

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

# Initialize database connections
try:
    NeonPostgresConnector.initialize_connection_pool()
    print("Database connections initialized successfully")
except Exception as e:
    print(f"Error initializing database connections: {e}")

def create_app():
    app = Flask(__name__,
                static_folder='../frontend/static',
                template_folder='../frontend/templates')

    # Flask session configuration
    app.secret_key = JWT_SECRET_KEY

    # JWT Configuration
    app.config['JWT_SECRET_KEY'] = JWT_SECRET_KEY
    app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(seconds=int(JWT_ACCESS_TOKEN_EXPIRES))

    # Enable CORS
    CORS(app)

    # Initialize JWT
    jwt = JWTManager(app)

    # Register blueprints
    app.register_blueprint(user_blueprint)
    app.register_blueprint(main_controller_blueprint)
    app.register_blueprint(product_controller_blueprint)
    app.register_blueprint(cart_wishlist_controller_blueprint)
    app.register_blueprint(order_bp)
    app.register_blueprint(review_bp)
    app.register_blueprint(redis_main_controller_blueprint)

    @app.before_request
    def track_request():
        """Track every request to track user browsing."""
        if 'user_id' in session:
            current_time = time.time()
            last_page_time = session.get('last_page_time')

            tracking_service.track_browsing_history(
                user_id=session['user_id'],
                session_id=session.get('session_id'),
                page_url=request.path,
                referrer_url=request.referrer,
                last_page_time=last_page_time
            )

            session['last_page_time'] = current_time

    @app.route('/static/<path:path>')
    def send_static(path):
        return send_from_directory('../frontend/static', path)

    # Error handlers
    @app.errorhandler(404)
    def page_not_found(e):
        return "404 Error"

    @app.errorhandler(500)
    def internal_server_error(e):
        return "500 Error"

    return app

app = create_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)
