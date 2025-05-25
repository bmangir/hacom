import time
from datetime import datetime

from backend.app.models.user_models import ProductDetails
from databases.postgres.neon_postgres_connector import NeonPostgresConnector
from backend.config import MONGO_PRODUCTS_DB, client
from backend.utils.utils import kafka_producer_util, _get_product_details


class WishlistService:
    @staticmethod
    def add_to_wishlist(user_id, product_id, session_id):
        """Add a product to the user's wishlist."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            
            # Check if product already exists in wishlist
            cursor.execute("""
                SELECT wishlist_id 
                FROM wishlist 
                WHERE user_id = %s AND product_id = %s AND action_type = 'removed'
            """, (user_id, product_id))

            try:
                wishlist_item = cursor.fetchone()
                product = ProductDetails.objects(product_id=product_id).first()

                if wishlist_item is None:
                    # Insert new item
                    cursor.execute("""
                        INSERT INTO wishlist (user_id, product_id, action_type, session_id)
                        VALUES (%s, %s, 'added', %s)
                        RETURNING wishlist_id
                    """, (user_id, product_id, session_id))

                    wishlist_id = cursor.fetchone()[0]

                else:
                    wishlist_id = wishlist_item[0]
                    cursor.execute("""
                        UPDATE wishlist
                        SET action_type = 'added', action_date = %s, session_id = %s
                        WHERE wishlist_id = %s;
                    """, (round(time.time() * 1000), session_id, wishlist_id))

                # Send wishlist add event to Kafka
                wishlist_details = {
                    "user_id": user_id,
                    "session_id": session_id,
                    "product_id": product_id,
                    "wishlist_id": wishlist_id,
                    "unit_price": product.price,
                    "product_name": product.product_name if product else None,
                    "category": product.category if product else None,
                    "action_date": datetime.utcnow().strftime("%Y-%m-%d")
                }

                wishlist_event = kafka_producer_util.format_interaction_event(
                    user_id=user_id,
                    product_id=product_id,
                    event_type="add_to_wishlist",
                    details=wishlist_details
                )
                #kafka_producer_util.send_event(wishlist_event)

                conn.commit()
                return {"success": True, "wishlist_id": wishlist_id}

            except:
                raise Exception()
        except Exception as e:
            if conn:
                conn.rollback()
            return {"success": False, "message": str(e)}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    @staticmethod
    def get_wishlist_items(user_id):
        """Get all items in the user's wishlist."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT w.wishlist_id, w.product_id, w.action_date
                FROM wishlist w
                WHERE w.user_id = %s AND w.action_type = 'added'
                ORDER BY w.action_date DESC
            """, (user_id,))
            try:
                items = cursor.fetchall()
                #items_list = [{"wishlist_id": item[0], "product_id": item[1], "action_date": item[2]} for item in items]
                item_ids = [item[1] for item in items]

                wishlist_items = _get_product_details(item_ids)

                #if client:
                #    products_coll = client[MONGO_PRODUCTS_DB]["products"]
#
                #    for item in items_list:
                #        product = products_coll.find_one({"product_id": item["product_id"]},
                #                                      {"_id": 0, "product_name": 1, "price": 1, "category": 1})
                #        if product:
                #            item["product_name"] = product.get("product_name")
                #            item["price"] = product.get("price")
                #            item["category"] = product.get("category")

                return {"success": True, "items": wishlist_items}

            except:
                raise Exception("Error fetching wishlist items")
        except Exception as e:
            if conn:
                conn.rollback()
            return {"success": False, "message": str(e)}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    @staticmethod
    def remove_from_wishlist(user_id, product_id, session_id):
        """Remove a product from the user's wishlist."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT wishlist_id
                FROM wishlist
                WHERE user_id = %s AND product_id = %s AND action_type = 'added';
            """, (user_id, product_id))

            try:
                existing_item = cursor.fetchone()
                if not existing_item:
                    return {"success": False, "message": "Item not found in wishlist"}

                wishlist_id = existing_item[0]

                cursor.execute("""
                    UPDATE wishlist
                    SET action_type = 'removed', action_date = %s, session_id = %s
                    WHERE wishlist_id = %s
                    RETURNING wishlist_id
                """, (round(time.time() * 1000), session_id, wishlist_id))

                if cursor.rowcount == 0:
                    return {"success": False, "message": "Error removing item from wishlist"}

                product = ProductDetails.objects(product_id=product_id).first()

                # Send wishlist remove event to Kafka
                wishlist_details = {
                    "user_id": user_id,
                    "session_id": session_id,
                    "product_id": product_id,
                    "wishlist_id": wishlist_id,
                    "unit_price": product.price,
                    "product_name": product.product_name if product else None,
                    "category": product.category if product else None,
                    "action_date": datetime.utcnow().strftime("%Y-%m-%d")
                }

                wishlist_event = kafka_producer_util.format_interaction_event(
                    user_id=user_id,
                    product_id=product_id,
                    event_type="remove_from_wishlist",
                    details=wishlist_details
                )

                #kafka_producer_util.send_event(wishlist_event)

                conn.commit()

                return {"success": True}

            except:
                raise Exception()
        except Exception as e:
            if conn:
                conn.rollback()
            return {"success": False, "message": str(e)}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    @staticmethod
    def is_in_wishlist(user_id, product_id):
        """Check if a product is already in the user's wishlist."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT wishlist_id 
                FROM wishlist 
                WHERE user_id = %s AND product_id = %s AND action_type = 'added'
            """, (user_id, product_id))
            
            return cursor.fetchone() is not None
            
        except Exception as e:
            print(f"Error checking wishlist: {str(e)}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)
                
    @staticmethod
    def clear_wishlist(user_id, session_id):
        """Clear all items from user's wishlist."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            
            # First get all the wishlist items to report to Kafka
            cursor.execute("""
                SELECT wishlist_id, product_id
                FROM wishlist 
                WHERE user_id = %s AND action_type = 'added'
            """, (user_id,))
            
            wishlist_items = cursor.fetchall()

            # Update all 'added' items to 'removed'
            cursor.execute("""
                UPDATE wishlist
                SET action_type = 'removed', action_date = %s, session_id = %s
                WHERE user_id = %s AND action_type = 'added'
            """, (round(time.time() * 1000), session_id, user_id))

            try:
                # Track each removed item
                for item in wishlist_items:
                    wishlist_id, product_id = item

                    # Get product details
                    #product = products_coll.find_one({"product_id": product_id},
                    #                              {"_id": 0, "product_name": 1, "price": 1, "category": 1})
                    product = ProductDetails.objects(product_id=product_id)

                    # Send wishlist item remove event to Kafka for each product
                    wishlist_details = {
                        "user_id": user_id,
                        "session_id": session_id,
                        "product_id": product_id,
                        "wishlist_id": wishlist_id,
                        "product_name": product.product_name if product else None,
                        "category": product.category if product else None,
                        "action_date": datetime.utcnow().strftime("%Y-%m-%d"),
                        "reason": "wishlist_cleared"
                    }

                    wishlist_event = kafka_producer_util.format_interaction_event(
                        user_id=user_id,
                        product_id=product_id,
                        event_type="wishlist_item_removed",
                        details=wishlist_details
                    )
                    #kafka_producer_util.send_event(wishlist_event)

                # Send a wishlist cleared summary event
                wishlist_cleared_event = kafka_producer_util.format_interaction_event(
                    user_id=user_id,
                    product_id=None,
                    event_type="wishlist_cleared",
                    details={
                        "user_id": user_id,
                        "item_count": len(wishlist_items),
                        "action_date": datetime.utcnow().strftime("%Y-%m-%d")
                    }
                )
                #kafka_producer_util.send_event(wishlist_cleared_event)

                conn.commit()
                return {"success": True}
            except:
                raise Exception()

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error clearing wishlist: {str(e)}")
            return {"success": False, "message": str(e)}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn) 