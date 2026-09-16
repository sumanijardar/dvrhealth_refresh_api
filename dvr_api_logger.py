import mysql.connector
from datetime import datetime
from typing import Optional, Dict, Any, List
import requests

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "esurv",
}

def get_human_readable_status(status_code: int, error_message: str = "") -> str:
    """
    Convert HTTP status code to human-readable message for DVR debugging.
    """
    status_messages = {
        200: "OK - Success",
        201: "Created - Resource created successfully",
        204: "No Content - Request successful",
        400: "Bad Request - Check DVR configuration",
        401: "Unauthorized - Incorrect username or password",
        403: "Forbidden - Access denied, check credentials",
        404: "Not Found - Check DVR name or IP address",
        405: "Method Not Allowed - API method not supported",
        408: "Request Timeout - DVR not responding",
        500: "Internal Server Error - DVR server error",
        501: "Not Implemented - Feature not supported by DVR",
        502: "Bad Gateway - DVR gateway error",
        503: "Service Unavailable - DVR service down",
        504: "Gateway Timeout - DVR timeout",
        520: "Web Server Returned Unknown Error",
        521: "Web Server Is Down",
        522: "Connection Timed Out",
    }
    
    if status_code in status_messages:
        return f"{status_code} {status_messages[status_code]}"
    
    if status_code >= 400 and status_code < 500:
        return f"{status_code} Client Error - Check DVR settings"
    
    if status_code >= 500 and status_code < 600:
        return f"{status_code} Server Error - DVR internal error"
    
    if status_code == 0:
        return "Connection Failed - DVR unreachable or timeout"
    
    return f"{status_code} Unknown Status"


def create_api_logs_table():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dvr_api_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                atm_id VARCHAR(100),
                ip VARCHAR(45),
                dvr_name VARCHAR(100),
                api_endpoint VARCHAR(255),
                http_method VARCHAR(10),
                status_code INT,
                response_message VARCHAR(500),
                error_detail TEXT,
                request_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_ip (ip),
                INDEX idx_atm_id (atm_id),
                INDEX idx_request_time (request_time)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        
        conn.commit()
    except Exception as e:
        print(f"Error creating api_logs table: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def add_api_response_column():
    """Add api_response column to dvr_status and dvr_history if not exists."""
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                ALTER TABLE dvr_status 
                ADD COLUMN api_response VARCHAR(500) AFTER login_status
            """)
            conn.commit()
        except mysql.connector.Error as e:
            if "Duplicate column name" not in str(e):
                pass
        
        try:
            cursor.execute("""
                ALTER TABLE dvr_history 
                ADD COLUMN api_response VARCHAR(500) AFTER login_status
            """)
            conn.commit()
        except mysql.connector.Error as e:
            if "Duplicate column name" not in str(e):
                pass
                
    except Exception as e:
        print(f"Error adding api_response column: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def log_api_response(
    atm_id: str,
    ip: str,
    dvr_name: str,
    endpoint: str,
    method: str,
    status_code: int,
    response_text: str = "",
    error_detail: str = ""
):
    """
    Log API response to dvr_api_logs table for debugging.
    """
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        response_message = get_human_readable_status(status_code, response_text)
        
        cursor.execute("""
            INSERT INTO dvr_api_logs 
            (atm_id, ip, dvr_name, api_endpoint, http_method, status_code, response_message, error_detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (atm_id, ip, dvr_name, endpoint, method, status_code, response_message, error_detail[:500] if error_detail else ""))
        
        conn.commit()
    except Exception as e:
        print(f"Error logging API response: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def log_all_api_responses(
    atm_id: str,
    ip: str,
    dvr_name: str,
    api_calls: List[Dict[str, Any]]
):
    """
    Log multiple API responses at once.
    api_calls: List of dicts with keys: endpoint, method, status_code, response_text, error_detail
    """
    for call in api_calls:
        log_api_response(
            atm_id=atm_id,
            ip=ip,
            dvr_name=dvr_name,
            endpoint=call.get("endpoint", ""),
            method=call.get("method", "GET"),
            status_code=call.get("status_code", 0),
            response_text=call.get("response_text", ""),
            error_detail=call.get("error_detail", "")
        )


class APIResponseLogger:
    """
    Context manager and helper for logging API responses.
    Usage:
        logger = APIResponseLogger(ip, port, "Dvr", atm_id)
        withahuva_n logger.log_request("/cgi-bin/global.cgi", "GET"):
            response = requests.get(...)
    """
    
    def __init__(self, ip: str, port: int, dvr_name: str, atm_id: str = ""):
        self.ip = ip
        self.port = port
        self.dvr_name = dvr_name
        self.atm_id = atm_id
        self.api_calls: List[Dict[str, Any]] = []
    
    def log_request(self, endpoint: str, method: str = "GET"):
        return APIRequestContext(self, endpoint, method)
    
    def add_api_call(self, endpoint: str, method: str, status_code: int, response_text: str = "", error_detail: str = ""):
        self.api_calls.append({
            "endpoint": f"http://{self.ip}:{self.port}{endpoint}",
            "method": method,
            "status_code": status_code,
            "response_text": response_text[:200] if response_text else "",
            "error_detail": error_detail
        })
    
    def save_to_database(self):
        if self.atm_id:
            log_all_api_responses(self.atm_id, self.ip, self.dvr_name, self.api_calls)
    
    def get_api_response_summary(self) -> str:
        """Get a summary of all API responses for database storage."""
        if not self.api_calls:
            return ""
        
        errors = [f"{call['status_code']}" for call in self.api_calls if call['status_code'] != 200]
        if errors:
            return f"Errors: {', '.join(errors[:5])}"
        
        return f"All {len(self.api_calls)} APIs OK"


class APIRequestContext:
    """Context manager for individual API requests."""
    
    def __init__(self, logger: APIResponseLogger, endpoint: str, method: str):
        self.logger = logger
        self.endpoint = endpoint
        self.method = method
        self.response = None
        self.error = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.response is not None:
            status_code = self.response.status_code
            response_text = self.response.text[:200] if self.response.text else ""
        else:
            status_code = 0
            response_text = ""
            if self.error:
                self.error = str(self.error)[:200]

        self.logger.add_api_call(
            self.endpoint,
            self.method,
            status_code,
            response_text,
            str(self.error) if self.error else ""
        )


def init_api_logging():
    """Initialize the API logging system - call once at script start."""
    create_api_logs_table()
    add_api_response_column()
