from databases.postgres.neon_postgres_connector import NeonPostgresConnector

class WishlistService:
    @staticmethod
    def add_to_wishlist(user_id, product_id):
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
                WHERE user_id = %s AND product_id = %s AND action_type = 'added'
            """, (user_id, product_id))
            
            if cursor.fetchone():
                return {"success": False, "message": "Item already in wishlist"}
            
            # Insert new item
            cursor.execute("""
                INSERT INTO wishlist (user_id, product_id, action_type)
                VALUES (%s, %s, 'added')
                RETURNING wishlist_id
            """, (user_id, product_id))
            
            wishlist_id = cursor.fetchone()[0]
            conn.commit()
            
            return {"success": True, "wishlist_id": wishlist_id}
            
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
            
            items = cursor.fetchall()
            items_list = [{"wishlist_id": item[0], "product_id": item[1], "action_date": item[2]} for item in items]
            return {"success": True, "items": items_list}
            
        except Exception as e:
            return {"success": False, "message": str(e)}
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    @staticmethod
    def remove_from_wishlist(user_id, product_id):
        """Remove a product from the user's wishlist."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            
            # Insert a new 'removed' record
            cursor.execute("""
                INSERT INTO wishlist (user_id, product_id, action_type)
                VALUES (%s, %s, 'removed')
                RETURNING wishlist_id
            """, (user_id, product_id))
            
            if cursor.rowcount == 0:
                return {"success": False, "message": "Error removing item from wishlist"}
            
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