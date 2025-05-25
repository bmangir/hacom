import time
from datetime import datetime

from backend.app.models.user_models import ProductDetails
from backend.config import MONGO_PRODUCTS_DB, client
from databases.mongo.mongo_connector import MongoConnector
from databases.postgres.neon_postgres_connector import NeonPostgresConnector
from backend.utils.utils import kafka_producer_util


class CartService:
    @staticmethod
    def add_to_cart(user_id, product_id, session_id, quantity=1):
        """Add a product to the user's cart or update quantity if exists."""

        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            #products_coll = client[MONGO_PRODUCTS_DB]["products"]

            cursor.execute("""
                SELECT quantity, cart_id
                FROM cart
                WHERE user_id = %s AND product_id = %s AND action_type = 'removed';
            """, (user_id, product_id))
            cart_item = cursor.fetchone()
            
            # Get product details for Kafka event
            #product = products_coll.find_one({"product_id": product_id},
            #                                {"_id": 0, "product_name": 1, "price": 1, "category": 1})
            product = ProductDetails.objects(product_id=product_id).first()
            price = product.price

            if cart_item is None:
                cursor.execute("""
                    SELECT quantity, cart_id
                    FROM cart
                    WHERE user_id = %s AND product_id = %s AND action_type = 'added';
                """, (user_id, product_id))
                previous_added_item = cursor.fetchone()

                if previous_added_item is None:
                    cursor.execute("""
                        INSERT INTO cart (user_id, product_id, action_type, quantity, session_id)
                        VALUES (%s, %s, 'added', %s, %s)
                        RETURNING cart_id
                    """, (user_id, product_id, quantity, session_id))

                    cart_id = cursor.fetchone()[0]
                else:
                    cart_id = previous_added_item[1]
                    quantity = previous_added_item[0] + quantity

                    cursor.execute("""
                        UPDATE cart
                        SET action_type = 'added', quantity = %s, action_date = %s, session_id = %s
                        WHERE cart_id = %s;
                    """, (quantity, round(time.time() * 1000), session_id, cart_id))

            else:
                cart_id = cart_item[1]
                quantity = cart_item[0] + quantity

                cursor.execute("""
                    UPDATE cart
                    SET action_type = 'added', quantity = %s, action_date = %s, session_id = %s
                    WHERE cart_id = %s;
                """, (quantity, round(time.time() * 1000), session_id, cart_id))

            conn.commit()

            filter = {"product_id": product_id}
            update = {"$set": {"stock_quantity": quantity}}

            result = products_coll.update_one(filter, update)
            if not result.matched_count:  # Rollback if no matching product found
                raise Exception("No matching product found in MongoDB")
                
            # Send cart add event to Kafka
            cart_details = {
                "user_id": user_id,
                "session_id": session_id,
                "product_id": product_id,
                "cart_id": cart_id,
                "quantity": quantity,
                "unit_price": price,
                "total_price": price * quantity,
                "product_name": product.product_name if product else None,
                "category": product.category if product else None,
                "action_date": datetime.utcnow().strftime("%Y-%m-%d")
            }
            
            cart_event = kafka_producer_util.format_interaction_event(
                user_id=user_id,
                product_id=product_id,
                event_type="add_to_cart",
                details=cart_details
            )
            #kafka_producer_util.send_event(cart_event)

            return {"success": True, "cart_id": cart_id}

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
    def update_quantity(cart_id, quantity):
        """Update quantity by adding new record."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            
            # Get product_id and user_id for Kafka event
            cursor.execute("""
                SELECT user_id, product_id, quantity
                FROM cart
                WHERE cart_id = %s
            """, (cart_id,))
            
            cart_data = cursor.fetchone()
            if not cart_data:
                return {"success": False, "message": "Item not found in cart"}
                
            user_id, product_id, old_quantity = cart_data

            cursor.execute("""
                UPDATE cart
                SET quantity = %s, action_date = %s
                WHERE cart_id = %s
            """, (quantity, round(time.time() * 1000), cart_id))

            conn.commit()
            
            # Get product details for Kafka event
            if client:
                #products_coll = client[MONGO_PRODUCTS_DB]["products"]
                #product = products_coll.find_one({"product_id": product_id},
                #                               {"_id": 0, "product_name": 1, "price": 1, "category": 1})

                product = ProductDetails.objects(product_id=product_id).first()
                price = product.price
                
                # Send cart update event to Kafka
                cart_details = {
                    "user_id": user_id,
                    "product_id": product_id,
                    "cart_id": cart_id,
                    "old_quantity": old_quantity,
                    "new_quantity": quantity,
                    "quantity_change": quantity - old_quantity,
                    "unit_price": price,
                    "total_price": price * quantity,
                    "product_name": product.product_name if product else None,
                    "category": product.category if product else None,
                    "action_date": datetime.utcnow().strftime("%Y-%m-%d")
                }
                
                cart_event = kafka_producer_util.format_interaction_event(
                    user_id=user_id,
                    product_id=product_id,
                    event_type="update_cart",
                    details=cart_details
                )
                #kafka_producer_util.send_event(cart_event)
            
            return {"success": True}

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
    def get_cart_items(user_id):
        """Get all items in the user's cart."""
        conn = None
        cursor = None
        try:
            #products_coll = client[MONGO_PRODUCTS_DB]["products"]

            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT c.cart_id, c.product_id, c.quantity, c.action_date
                FROM cart c
                WHERE c.user_id = %s AND c.action_type = 'added'
                ORDER BY c.action_date DESC
            """, (user_id,))

            items = cursor.fetchall()
            # Convert items to a list of dictionaries for easier processing
            #items_list = [{"cart_id": item[0], "product_id": item[1], "quantity": item[2], "action_date": item[3]} for
            #              item in items]

            # check items is empty
            if len(items) == 0:
                return {"success": True, "items": []}

            items_list = []
            for item in items:
                #unit_price = products_coll.find_one({"product_id": item[1]}, {"_id": 0, "price": 1})
                product_detail = ProductDetails.objects(product_id=item[1]).first()
                items_list.append({
                    "cart_id": item[0],
                    "product_id": item[1],
                    "quantity": item[2],
                    "action_date": item[3],
                    "price": product_detail.price
                })

            return {"success": True, "items": items_list}

        except Exception as e:
            print(f"Error fetching cart items: {str(e)}")
            return {"success": False, "message": str(e)}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    @staticmethod
    def remove_from_cart(user_id, product_id, session_id):
        """Remove a product from the user's cart."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # First get the current quantity
            cursor.execute("""
                SELECT cart_id, quantity 
                FROM cart 
                WHERE user_id = %s AND product_id = %s AND action_type = 'added'
            """, (user_id, product_id))

            existing_item = cursor.fetchone()

            if not existing_item:
                return {"success": False, "message": "Item not found in cart"}
                
            cart_id = existing_item[0]
            quantity = existing_item[1]

            # Update the existing record to 'removed'
            cursor.execute("""
                UPDATE cart
                SET action_type = 'removed', action_date = %s, session_id = %s
                WHERE cart_id = %s
            """, (round(time.time() * 1000), session_id, cart_id,))

            conn.commit()

            if client:
                #products_coll = client[MONGO_PRODUCTS_DB]["products"]
                #product = products_coll.find_one({"product_id": product_id},
                #                               {"_id": 0, "product_name": 1, "price": 1, "category": 1})
                product = ProductDetails.objects(product_id=product_id).first()
                price = product.price
                
                # Send cart remove event to Kafka
                cart_details = {
                    "user_id": user_id,
                    "session_id": session_id,
                    "product_id": product_id,
                    "cart_id": cart_id,
                    "quantity": quantity,
                    "unit_price": price,
                    "total_price": price * quantity,
                    "product_name": product.product_name if product else None,
                    "category": product.category if product else None,
                    "action_date": datetime.utcnow().strftime("%Y-%m-%d")
                }
                
                cart_event = kafka_producer_util.format_interaction_event(
                    user_id=user_id,
                    product_id=product_id,
                    event_type="remove_from_cart",
                    details=cart_details
                )
                #kafka_producer_util.send_event(cart_event)
            
            return {"success": True}

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
    def clear_cart(user_id):
        """Clear all items from the user's cart."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            
            # First get all the cart items to report to Kafka
            cursor.execute("""
                SELECT cart_id, product_id, quantity, session_id
                FROM cart 
                WHERE user_id = %s AND action_type = 'added'
            """, (user_id,))
            
            cart_items = cursor.fetchall()

            # Update all 'added' items to 'removed'
            cursor.execute("""
                UPDATE cart
                SET action_type = 'removed', action_date = %s
                WHERE user_id = %s AND action_type = 'added'
            """, (round(time.time() * 1000), user_id))

            conn.commit()
            
            if client and cart_items:
                #products_coll = client[MONGO_PRODUCTS_DB]["products"]

                # Track each removed item
                for item in cart_items:
                    cart_id, product_id, quantity, session_id = item
                    
                    # Get product details
                    #product = products_coll.find_one({"product_id": product_id},
                    #                              {"_id": 0, "product_name": 1, "price": 1, "category": 1})
                    product = ProductDetails.objects(product_id=product_id).first()
                    price = product.price
                    
                    # Send cart clear event to Kafka for each product
                    cart_details = {
                        "user_id": user_id,
                        "session_id": session_id,
                        "product_id": product_id,
                        "cart_id": cart_id,
                        "quantity": quantity,
                        "unit_price": price,
                        "total_price": price * quantity,
                        "product_name": product.product_name if product else None,
                        "category": product.category if product else None,
                        "action_date": datetime.utcnow().strftime("%Y-%m-%d"),
                        "reason": "order_placed"  # Typically this is called after order placement
                    }
                    
                    cart_event = kafka_producer_util.format_interaction_event(
                        user_id=user_id,
                        product_id=product_id,
                        event_type="cart_item_removed",
                        details=cart_details
                    )
                    #kafka_producer_util.send_event(cart_event)
                
                # Send a cart cleared summary event
                cart_cleared_event = kafka_producer_util.format_interaction_event(
                    user_id=user_id,
                    product_id=None,
                    event_type="cart_cleared",
                    details={
                        "user_id": user_id,
                        "item_count": len(cart_items),
                        "action_date": datetime.utcnow().strftime("%Y-%m-%d"),
                        "reason": "order_placed"
                    }
                )
                #kafka_producer_util.send_event(cart_cleared_event)
                
            return {"success": True}

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error clearing cart: {str(e)}")
            return {"success": False, "message": str(e)}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)
