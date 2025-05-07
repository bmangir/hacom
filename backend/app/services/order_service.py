import time
from datetime import datetime
import uuid

from backend.config.config import MONGO_PRODUCTS_DB
from databases.mongo.mongo_connector import MongoConnector
from databases.postgres.neon_postgres_connector import NeonPostgresConnector
from databases.postgres.postgres_connector import PostgresConnector


class OrderService:
    def create_order(self, user_id, cart_items, shipping_info, payment_method):
        """Create a new order in the database."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Generate unique order ID
            order_id = f"O{str(uuid.uuid4().int)[:8]}"
            order_date = datetime.now()
            status = "Pending"

            # Insert order items
            for item in cart_items:
                query = """
                    INSERT INTO orders (
                        order_id, user_id, product_id, quantity,
                        unit_price, total_amount, order_date, status,
                        payment_method, shipping_address, shipping_city,
                        shipping_zip, shipping_phone, shipping_email,
                        shipping_name
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """
                
                total_amount = item['price'] * item['quantity']
                
                params = (
                    order_id,
                    user_id,
                    item['product_id'],
                    item['quantity'],
                    item['price'],
                    total_amount,
                    round(time.time() * 1000),
                    status,
                    payment_method,
                    shipping_info['address'],
                    shipping_info['city'],
                    shipping_info['zip_code'],
                    shipping_info['phone'],
                    shipping_info['email'],
                    shipping_info['full_name']
                )
                
                cursor.execute(query, params)

            # Commit the transaction
            conn.commit()

            return {
                'order_id': order_id,
                'status': status,
                'order_date': order_date,
                'items': cart_items,
                'shipping_info': shipping_info,
                'payment_method': payment_method
            }

        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error creating order: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    def get_order(self, order_id):
        """Get order details by order ID."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Get MongoDB connection
            mongo_conn = MongoConnector()
            client = mongo_conn.get_client()
            if not client:
                print("MongoDB connection failed")
                return None

            products_coll = client[MONGO_PRODUCTS_DB]['products']

            # Get order details from PostgreSQL
            query = """
                SELECT 
                    o.order_id,
                    o.user_id,
                    o.product_id,
                    o.quantity,
                    o.unit_price,
                    o.total_amount,
                    o.order_date,
                    o.status,
                    o.payment_method,
                    o.shipping_address,
                    o.shipping_city,
                    o.shipping_zip,
                    o.shipping_phone,
                    o.shipping_email,
                    o.shipping_name
                FROM orders o
                WHERE o.order_id = %s
            """
            
            cursor.execute(query, [order_id])
            orders = cursor.fetchall()

            if not orders:
                return None

            # Initialize order details
            order_details = {
                'order_id': orders[0][0],
                'user_id': orders[0][1],
                'items': [],
                'order_date': orders[0][6],
                'status': orders[0][7],
                'payment_method': orders[0][8],
                'shipping_info': {
                    'address': orders[0][9],
                    'city': orders[0][10],
                    'zip_code': orders[0][11],
                    'phone': orders[0][12],
                    'email': orders[0][13],
                    'full_name': orders[0][14]
                }
            }

            # Process each order item
            subtotal = 0
            for order in orders:
                product_id = order[2]
                quantity = int(order[3])
                unit_price = float(order[4])
                
                # Get product details from MongoDB
                product = products_coll.find_one({'product_id': product_id})
                product_name = product.get('product_name', 'Unknown Product') if product else 'Unknown Product'
                
                # Calculate item total
                item_total = unit_price * quantity
                subtotal += item_total
                
                # Add item to order details
                order_details['items'].append({
                    'name': product_name,
                    'product_id': product_id,
                    'quantity': quantity,
                    'price': unit_price,
                    'total': item_total
                })

            # Add total calculations
            shipping_cost = 10.00  # Fixed shipping cost
            total = subtotal + shipping_cost

            order_details['subtotal'] = subtotal
            order_details['shipping_cost'] = shipping_cost
            order_details['total'] = total

            return order_details

        except Exception as e:
            print(f"Error getting order: {str(e)}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)

    def update_order_status(self, order_id, new_status):
        """Update the status of an order."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            query = "UPDATE orders SET status = %s WHERE order_id = %s"
            cursor.execute(query, [new_status, order_id])
            conn.commit()

            return True
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error updating order status: {str(e)}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)