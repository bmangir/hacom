from flask import Flask, send_from_directory, session, request, render_template
from flask_jwt_extended import JWTManager
from datetime import timedelta
import os
import sys
import time
from flask_cors import CORS



# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app'))
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

from app.controllers.user_controller import user_blueprint
from app.controllers.main_controller import main_controller_blueprint
from app.controllers.product_controller import product_controller_blueprint
from app.controllers.merchant_controller import merchant_controller_blueprint
from app.controllers.cart_wishlist_controller import cart_wishlist_controller_blueprint
from config import JWT_SECRET_KEY, JWT_ACCESS_TOKEN_EXPIRES
from app.controllers.order_controller import order_bp

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

    return app


app = create_app()
CORS(app)

@app.before_request
def track_request():
    """Track every request to track user browsing."""
    if 'user_id' in session:
        current_time = time.time()
        last_page_time = session.get('last_page_time')

        # Use the global tracking_service
        #tracking_service.track_browsing_history(
        #    user_id=session['user_id'],
        #    session_id=session.get('session_id'),
        #    page_url=request.path,
        #    referrer_url=request.referrer,
        #    last_page_time=last_page_time
        #)

        session['last_page_time'] = current_time


@app.after_request
def after_request(response):
    """Update last page time after each request."""
    if 'user_id' in session and request.endpoint and not request.endpoint.startswith('static'):
        session['last_page_time'] = time.time()
    return response


# Add route for serving static files
@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('../frontend/static', path)


if __name__ == '__main__':
    jwt = JWTManager(app)

    # Register blueprints
    app.register_blueprint(user_blueprint)
    app.register_blueprint(main_controller_blueprint)
    app.register_blueprint(product_controller_blueprint)
    app.register_blueprint(merchant_controller_blueprint)
    app.register_blueprint(cart_wishlist_controller_blueprint)
    app.register_blueprint(order_bp)

    # Error handlers
    @app.errorhandler(404)
    def page_not_found(e):
        return render_template('404.html'), 404

    @app.errorhandler(500)
    def internal_server_error(e):
        return render_template('500.html'), 500

    app.run(host="127.0.0.1", port=8080, debug=True)
