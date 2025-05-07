import time

from config import MONGO_PRODUCTS_DB
from databases.mongo.mongo_connector import MongoConnector
from databases.postgres.neon_postgres_connector import NeonPostgresConnector


class CartService:
    @staticmethod
    def add_to_cart(user_id, product_id, session_id, quantity=1):
        """Add a product to the user's cart or update quantity if exists."""

        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT quantity, cart_id
                FROM cart
                WHERE user_id = %s AND product_id = %s AND action_type = 'removed';
            """, (user_id, product_id))
            cart_item = cursor.fetchone()

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

            cursor.execute("""
                UPDATE cart
                SET quantity = %s, action_date = %s
                WHERE cart_id = %s
            """, (quantity, round(time.time() * 1000), cart_id))

            if not cart_id:
                return {"success": False, "message": "Item not found in cart"}

            conn.commit()
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
            mongo_conn = MongoConnector()
            client = mongo_conn.get_client()
            if not client:
                print("MongoDB connection failed")
                return

            products_coll = client[MONGO_PRODUCTS_DB]["products"]

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

            items_list = []
            for item in items:
                unit_price = products_coll.find_one({"product_id": item[1]}, {"_id": 0, "price": 1})
                items_list.append({
                    "cart_id": item[0],
                    "product_id": item[1],
                    "quantity": item[2],
                    "action_date": item[3],
                    "price": unit_price.get("price")
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

            # Update the existing record to 'removed'
            cursor.execute("""
                UPDATE cart
                SET action_type = 'removed', action_date = %s, session_id = %s
                WHERE cart_id = %s
            """, (round(time.time() * 1000), session_id, existing_item[0],))

            conn.commit()
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

            # Update all 'added' items to 'removed'
            cursor.execute("""
                UPDATE cart
                SET action_type = 'removed', action_date = %s
                WHERE user_id = %s AND action_type = 'added'
            """, (round(time.time() * 1000), user_id))

            conn.commit()
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
