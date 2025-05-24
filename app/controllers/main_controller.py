from flask import Blueprint, jsonify
from backend.app.services.service_locator import redis_cache

main_controller_blueprint = Blueprint('main_controller', __name__)

@main_controller_blueprint.route('/api/clear-cache', methods=['POST'])
def clear_cache():
    """Clear all recommendation cache"""
    try:
        redis_cache.clear_all_cache()
        return jsonify({"message": "Cache cleared successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500 