from databases.postgres.neon_postgres_connector import NeonPostgresConnector
from flask import session

class CartService:
    @staticmethod
    def add_to_cart(user_id, product_id, quantity=1):
        """Add a product to the user's cart or update quantity if exists."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            
            # Check if product already exists in cart
            cursor.execute("""
                SELECT quantity 
                FROM cart 
                WHERE user_id = %s AND product_id = %s AND action_type = 'added'
            """, (user_id, product_id))
            
            existing_item = cursor.fetchone()
            
            if existing_item:
                # Update quantity of existing item
                new_quantity = existing_item[0] + quantity
            
            cursor.execute("""
                    INSERT INTO cart (user_id, product_id, action_type, quantity)
                    VALUES (%s, %s, 'added', %s)
                    RETURNING cart_id
                """, (user_id, product_id, quantity))
            
            cart_id = cursor.fetchone()[0]
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
    def update_quantity(user_id, product_id, quantity):
        """Update quantity by adding new record."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            
            # First mark existing item as removed
            cursor.execute("""
                INSERT INTO cart (user_id, product_id, action_type, quantity)
                SELECT user_id, product_id, 'removed', quantity
                FROM cart
                WHERE user_id = %s AND product_id = %s AND action_type = 'added'
            """, (user_id, product_id))
            
            # Then add new record with updated quantity
            cursor.execute("""
                INSERT INTO cart (user_id, product_id, action_type, quantity)
                VALUES (%s, %s, 'added', %s)
                RETURNING cart_id
            """, (user_id, product_id, quantity))
            
            cart_id = cursor.fetchone()
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
            items_list = [{"cart_id": item[0], "product_id": item[1], "quantity": item[2], "action_date": item[3]} for item in items]
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
    def remove_from_cart(user_id, product_id):
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
            
            # Insert a new 'removed' record
            cursor.execute("""
                INSERT INTO cart (user_id, product_id, action_type, quantity)
                VALUES (%s, %s, 'removed', %s)
            """, (user_id, product_id, existing_item[1]))
            
            # Update the existing record to 'removed'
            cursor.execute("""
                UPDATE cart 
                SET action_type = 'removed'
                WHERE cart_id = %s
            """, (existing_item[0],))
            
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