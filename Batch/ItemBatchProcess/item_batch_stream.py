from datetime import datetime

def process_item_batch(batch):
    try:
        # Extract features from batch
        purchase_count = batch.get('purchase_count', 0)
        view_count = batch.get('view_count', 0)
        wishlist_count = batch.get('wishlist_count', 0)
        cart_addition_count = batch.get('cart_addition_count', 0)
        conversion_rate = batch.get('conversion_rate', 0)
        abandonment_rate = batch.get('abandonment_rate', 0)
        avg_rating = batch.get('avg_rating', 0)
        trending_score = batch.get('trending_score', 0)
        
        # Seasonal sales
        winter_sales = batch.get('winter_sales', 0)
        spring_sales = batch.get('spring_sales', 0)
        summer_sales = batch.get('summer_sales', 0)
        fall_sales = batch.get('fall_sales', 0)
        
        # Sales trends
        daily_units_sold = batch.get('daily_units_sold', 0)
        trend_7_day = batch.get('trend_7_day', 0)
        trend_30_day = batch.get('trend_30_day', 0)
        
        # Latest sales
        last_1_day_sales = batch.get('last_1_day_sales', 0)
        last_7_days_sales = batch.get('last_7_days_sales', 0)
        last_30_days_sales = batch.get('last_30_days_sales', 0)
        
        # Content
        content = batch.get('content', '')
        
        # New features
        price_elasticity = batch.get('price_elasticity', 0)
        competitor_analysis = batch.get('competitor_analysis', 0)
        content_quality_score = batch.get('content_quality_score', 0)
        inventory_turnover = batch.get('inventory_turnover', 0)
        return_rate = batch.get('return_rate', 0)
        customer_satisfaction_score = batch.get('customer_satisfaction_score', 0)

        # Vectorize features
        vector = vectorize_item_features(
            purchase_count, view_count, wishlist_count, cart_addition_count,
            conversion_rate, abandonment_rate, avg_rating, trending_score,
            winter_sales, spring_sales, summer_sales, fall_sales,
            daily_units_sold, trend_7_day, trend_30_day,
            last_1_day_sales, last_7_days_sales, last_30_days_sales, content,
            price_elasticity, competitor_analysis, content_quality_score,
            inventory_turnover, return_rate, customer_satisfaction_score
        )

        return {
            'item_id': batch['item_id'],
            'vector': vector,
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Error processing item batch: {e}")
        return None 