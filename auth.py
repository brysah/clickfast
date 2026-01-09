"""
Authentication module for dashboard access
"""
from fastapi import HTTPException, Depends, status, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from config import settings
import secrets
from datetime import datetime
from typing import Dict
import logging

# Initialize HTTP Basic Auth
security = HTTPBasic()

# Track failed login attempts (in-memory for simplicity)
failed_attempts: Dict[str, int] = {}
blocked_ips: Dict[str, datetime] = {}

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_client_ip(request: Request) -> str:
    """Get client IP address from request"""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def is_ip_blocked(ip: str) -> bool:
    """Check if IP is temporarily blocked due to failed attempts"""
    if ip in blocked_ips:
        # Block for 15 minutes after 5 failed attempts
        blocked_time = blocked_ips[ip]
        if (datetime.now() - blocked_time).seconds < 900:  # 15 minutes
            return True
        else:
            # Unblock after timeout
            del blocked_ips[ip]
            failed_attempts[ip] = 0
    return False


def record_failed_attempt(ip: str):
    """Record failed login attempt and block if necessary"""
    failed_attempts[ip] = failed_attempts.get(ip, 0) + 1
    
    if failed_attempts[ip] >= 5:
        blocked_ips[ip] = datetime.now()
        logger.warning(f"🚫 IP {ip} blocked due to multiple failed login attempts")


def authenticate_dashboard(credentials: HTTPBasicCredentials = Depends(security), request: Request = None):
    """
    Authenticate dashboard access using HTTP Basic Auth with enhanced security
    
    Args:
        credentials: HTTP Basic credentials from browser
        request: FastAPI request object for IP tracking
        
    Returns:
        Username if authentication successful
        
    Raises:
        HTTPException: 401 if authentication fails or IP is blocked
    """
    client_ip = get_client_ip(request) if request else "unknown"
    
    # Check if IP is blocked
    if is_ip_blocked(client_ip):
        logger.warning(f"🚫 Blocked IP {client_ip} attempted dashboard access")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="IP temporariamente bloqueado devido a múltiplas tentativas de login falharam",
            headers={"WWW-Authenticate": "Basic realm=\"Dashboard\""},
        )
    
    # Use constant-time comparison to prevent timing attacks
    correct_username = secrets.compare_digest(
        credentials.username, settings.DASHBOARD_USERNAME
    )
    correct_password = secrets.compare_digest(
        credentials.password, settings.DASHBOARD_PASSWORD
    )
    
    if not (correct_username and correct_password):
        record_failed_attempt(client_ip)
        logger.warning(f"🚫 Failed dashboard login attempt from IP {client_ip} with username '{credentials.username}'")
        
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Acesso não autorizado ao dashboard",
            headers={"WWW-Authenticate": "Basic realm=\"Dashboard\""},
        )
    
    # Reset failed attempts on successful login
    if client_ip in failed_attempts:
        failed_attempts[client_ip] = 0
    
    logger.info(f"✅ Successful dashboard login from IP {client_ip} with username '{credentials.username}'")
    return credentials.username


def is_authenticated(credentials: HTTPBasicCredentials = Depends(security), request: Request = None) -> bool:
    """
    Check if credentials are valid without raising exception
    Enhanced with IP tracking and security logging
    
    Args:
        credentials: HTTP Basic credentials
        request: FastAPI request object for IP tracking
        
    Returns:
        True if authenticated, False otherwise
    """
    try:
        authenticate_dashboard(credentials, request)
        return True
    except HTTPException as e:
        client_ip = get_client_ip(request) if request else "unknown"
        
        if e.status_code == status.HTTP_429_TOO_MANY_REQUESTS:
            logger.warning(f"🚫 Blocked IP {client_ip} authentication check failed")
        else:
            logger.warning(f"🔒 Authentication check failed for IP {client_ip}")
            
        return False


def get_security_stats() -> Dict:
    """
    Get current security statistics for monitoring
    
    Returns:
        Dictionary with security stats
    """
    return {
        "failed_attempts": dict(failed_attempts),
        "blocked_ips": {ip: blocked_time.isoformat() for ip, blocked_time in blocked_ips.items()},
        "total_blocked_ips": len(blocked_ips),
        "timestamp": datetime.now().isoformat()
    }