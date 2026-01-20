"""
Authentication & Authorization Module

Implements JWT-based authentication with OAuth2 password flow.
ULTIMATE FIX: Use bcrypt directly to avoid passlib backend detection issues
"""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from jose import JWTError, jwt
import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
import secrets

from src.api.database import get_db
from src.api import schemas
from src.utils import logger

SECRET_KEY = secrets.token_urlsafe(32)

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="auth/token",
    scheme_name="JWT"
)


# ==========================================
# Password Utilities - Direct bcrypt usage
# ==========================================

def get_password_hash(password: str) -> str:
    """
    Hash a password using bcrypt directly
    
    Args:
        password: Plain text password
        
    Returns:
        Hashed password string
    """
    # Ensure password is bytes
    if isinstance(password, str):
        password_bytes = password.encode('utf-8')
    else:
        password_bytes = password
    
    # Bcrypt max 72 bytes - truncate if necessary
    if len(password_bytes) > 72:
        password_bytes = password_bytes[:72]
        logger.warning("Password truncated to 72 bytes for bcrypt")
    
    # Generate salt and hash
    salt = bcrypt.gensalt(rounds=12)
    hashed = bcrypt.hashpw(password_bytes, salt)
    
    # Return as string
    return hashed.decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify a password against its hash using bcrypt directly
    
    Args:
        plain_password: Plain text password
        hashed_password: Hashed password
        
    Returns:
        True if password matches
    """
    try:
        # Ensure password is bytes
        if isinstance(plain_password, str):
            password_bytes = plain_password.encode('utf-8')
        else:
            password_bytes = plain_password
        
        # Truncate to 72 bytes if necessary
        if len(password_bytes) > 72:
            password_bytes = password_bytes[:72]
        
        # Ensure hash is bytes
        if isinstance(hashed_password, str):
            hashed_password = hashed_password.encode('utf-8')
        
        # Verify
        return bcrypt.checkpw(password_bytes, hashed_password)
    
    except Exception as e:
        logger.error(f"Password verification failed: {e}")
        return False


# ==========================================
# Token Generation & Validation
# ==========================================

def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Create JWT access token
    
    Args:
        data: Payload data to encode
        expires_delta: Optional custom expiration time
        
    Returns:
        Encoded JWT token
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({
        "exp": expire,
        "iat": datetime.utcnow(),
        "type": "access"
    })

    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def create_refresh_token(data: Dict[str, Any]) -> str:
    """
    Create JWT refresh token
    
    Args:
        data: Payload data to encode
        
    Returns:
        Encoded JWT refresh token
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    to_encode.update({
        "exp": expire,
        "iat": datetime.utcnow(),
        "type": "refresh"
    })

    encode_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encode_jwt


def verify_token(token: str, credentials_exception: HTTPException) -> Dict[str, Any]:
    """
    Verify and decode JWT token
    
    Args:
        token: JWT token to verify
        credentials_exception: Exception to raise if verification fails
        
    Returns:
        Decoded token payload
        
    Raises:
        HTTPException: If token is invalid
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        token_type: str = payload.get("type")

        if username is None or token_type != "access":
            raise credentials_exception

        return payload
    
    except JWTError as e:
        logger.info(f"Token Verification failed: {str(e)}")
        raise credentials_exception


# ==========================================
# User Authentication
# ==========================================

def authenticate_user(db: Session, username: str, password: str) -> Optional[schemas.User]:
    """
    Authenticate user with username and password
    
    Args:
        db: Database session
        username: Username
        password: Plain text password
        
    Returns:
        User object if authentication successful, None otherwise
    """
    from src.api import crud

    user = crud.get_user_by_username(db, username)
    if not user:
        logger.info(f"User not found: {username}")
        return None
    
    # Use verify_password from crud (which uses bcrypt directly)
    if not crud.verify_password(password, user.hashed_password):
        logger.info(f"Password verification failed for user: {username}")
        return None
    
    return user


def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> schemas.User:
    """
    Get current authenticated user from token
    
    Dependency to use in protected endpoints
    
    Args:
        token: JWT token from request
        db: Database session
        
    Returns:
        Current user object
        
    Raises:
        HTTPException: If authentication fails
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"}
    )

    payload = verify_token(token, credentials_exception)
    username: str = payload.get("sub")

    from src.api import crud
    user = crud.get_user_by_username(db, username)

    if user is None:
        raise credentials_exception
    
    return user


def get_current_active_user(
    current_user: schemas.User = Depends(get_current_user)
) -> schemas.User:
    """
    Get current active user (not disabled)
    
    Args:
        current_user: User from get_current_user dependency
        
    Returns:
        Active user object
        
    Raises:
        HTTPException: If user is inactive
    """
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )
    return current_user


def require_role(required_role: str):
    """
    Decorator to require specific user role
    
    Usage:
        @app.get("/admin")
        async def admin_endpoint(user: User = Depends(require_role("admin"))):
            ...
    
    Args:
        required_role: Required role name
        
    Returns:
        Dependency function
    """
    def role_checker(current_user: schemas.User = Depends(get_current_active_user)):
        if current_user.role != required_role and current_user.role != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient permissions. Required role: {required_role}"
            )
        return current_user
    
    return role_checker


class APIKeyAuth:
    """
    Alternative authentication using API keys
    """

    def __init__(self):
        self.api_keys = {}

    def generate_api_key(self, user_id: int) -> str:
        """Generate new API key for user"""
        api_key = secrets.token_urlsafe(32)
        self.api_keys[api_key] = user_id
        return api_key
    
    def validate_api_key(self, api_key: str) -> Optional[int]:
        """Validate API key and return user ID"""
        return self.api_keys.get(api_key)
    
    def revoke_api_key(self, api_key: str) -> bool:
        """Revoke API key"""
        if api_key in self.api_keys:
            del self.api_keys[api_key]
            return True
        return False