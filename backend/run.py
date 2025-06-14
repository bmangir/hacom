from flask import Flask, send_from_directory, session, request
from flask_jwt_extended import JWTManager
from datetime import timedelta
import os
import sys
import time
from flask_cors import CORS

# Optimize TensorFlow and other ML libraries memory usage
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # Suppress TF logs completely
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'  # Disable oneDNN optimizations to save memory

# Add the backend directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
sys.path.append(backend_dir)

# Lazy import heavy dependencies only when needed
def get_tracking_service():
    from backend.app.services.service_locator import tracking_service
    return tracking_service

try:
    from databases.postgres.neon_postgres_connector import NeonPostgresConnector
    NeonPostgresConnector.initialize_connection_pool()
except Exception as e:
    print(f"Error initializing connections: {e}")

from backend.app.controllers.user_controller import user_blueprint
from backend.app.controllers.product_controller import product_controller_blueprint
from backend.app.controllers.merchant_controller import merchant_controller_blueprint
from backend.app.controllers.cart_wishlist_controller import cart_wishlist_controller_blueprint
from backend.app.controllers.order_controller import order_bp
from backend.redis_app.controllers.redis_controller import redis_main_controller_blueprint
from backend.app.controllers.main_controller import main_controller_blueprint
from backend.app.controllers.review_controller import review_bp
from backend.config import JWT_SECRET_KEY, JWT_ACCESS_TOKEN_EXPIRES

def create_app():
    app = Flask(__name__,
                static_folder='../frontend/static',
                template_folder='../frontend/templates')

    # Rest of your configuration
    app.secret_key = JWT_SECRET_KEY
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
    #app.register_blueprint(merchant_controller_blueprint)
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

            # Use lazy loading for tracking service
            tracking_service = get_tracking_service()
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
    # Get port from environment variable or default to 10000 (Render.com uses PORT env variable)
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
