import time
import user_agents
from databases.postgres.neon_postgres_connector import NeonPostgresConnector
from backend.utils.utils import kafka_producer_util

class SessionService:
    @staticmethod
    def create_session(user_id, request):
        """Create a new session record."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()

            # Get next session ID from sequence
            cursor.execute("SELECT nextval('session_id_seq')")
            seq_id = cursor.fetchone()[0]
            session_id = f"S{seq_id}"
            
            # Parse user agent
            user_agent = user_agents.parse(request.user_agent.string)
            
            # Get approximate location from IP (basic implementation)
            location = request.headers.get('X-Forwarded-For', request.remote_addr)
            
            cursor.execute("""
                INSERT INTO sessions 
                (session_id, user_id, start_time, location, device_type, browser_info)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING session_id
            """, (
                session_id,
                user_id,
                int(time.time()),  # epoch time
                location,
                str(user_agent.device.family),
                f"{user_agent.browser.family} {user_agent.browser.version_string}"
            ))
            
            # Send session creation event to Kafka
            event_details = {
                "session_id": session_id,
                "location": location,
                "device_type": str(user_agent.device.family),
                "browser_info": f"{user_agent.browser.family} {user_agent.browser.version_string}"
            }
            kafka_producer_util.send_event(
                kafka_producer_util.format_interaction_event(
                    user_id=user_id,
                    product_id=None,
                    event_type="create_session",
                    details=event_details
                )
            )
            
            conn.commit()
            return session_id
            
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error creating session: {str(e)}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn)
                
    @staticmethod
    def end_session(user_id, session_id):
        """End a session by setting its end time."""
        conn = None
        cursor = None
        try:
            conn = NeonPostgresConnector.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE sessions 
                SET end_time = %s
                WHERE session_id = %s
            """, (int(time.time()), session_id))
            
            # Send session end event to Kafka
            event_details = {
                "session_id": session_id,
                "end_time": int(time.time())
            }
            kafka_producer_util.send_event(
                kafka_producer_util.format_interaction_event(
                    user_id=user_id,
                    product_id=None,
                    event_type="end_session",
                    details=event_details
                )
            )
            
            conn.commit()
            return True
            
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error ending session: {str(e)}")
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                NeonPostgresConnector.return_connection(conn) 