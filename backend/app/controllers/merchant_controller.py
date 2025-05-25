from flask import Blueprint, render_template, request, session
from . import login_required
from ..services.service_locator import tracking_service, ITEM_RECOMMENDATION_SERVICE

merchant_controller_blueprint = Blueprint('merchant_controller_blueprint', __name__)

@merchant_controller_blueprint.route("/merchant/<merchant_id>")
@login_required
def merchant_details(merchant_id):
    try:
        # Get merchant details and their products
        merchant = ITEM_RECOMMENDATION_SERVICE.get_merchant_details(merchant_id)
        merchant = None
        if not merchant:
            return "Merchant not found", 404
            
        # Track merchant page view
        tracking_service.track_user_action(
            user_id=session.get('user_id'),
            session_id=session.get('session_id'),
            action='view_merchant',
            page_url=request.path,
            referrer=request.referrer,
            additional_data={'merchant_id': merchant_id}
        )
        
        return render_template(
            "merchant_details.html",
            merchant=merchant
        )
        
    except Exception as e:
        print(f"Error viewing merchant: {str(e)}")
        return "Error loading merchant", 500 