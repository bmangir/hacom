from flask import Flask, send_from_directory
from flask_jwt_extended import JWTManager
from datetime import timedelta
import os
import sys

# Add parent directory to Python path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from databases.mongo.mongo_connector import MongoConnector
from databases.postgres.neon_postgres_connector import NeonPostgresConnector
from app.controller.user_controller import user_blueprint
from app.controller.main_controller import main_controller_blueprint
from app.controller.product_controller import product_controller_blueprint
from config.config import JWT_SECRET_KEY, JWT_ACCESS_TOKEN_EXPIRES

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

# Initialize database connections
NeonPostgresConnector.initialize_connection_pool()

# Add route for serving static files
@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('../frontend/static', path)

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8080, debug=True)
