import time
import user_agents
from databases.postgres.neon_postgres_connector import NeonPostgresConnector

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
            print(session_id)
            
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
    def end_session(session_id):
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