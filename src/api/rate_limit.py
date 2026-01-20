# """
# Rate Limiting Module

# Implements request rate limiting to prevent API abuse.
# """

# from slowapi import Limiter
# from slowapi.util import get_remote_address
# from fastapi import Request, HTTPException, status
# from typing import Callable
# import time
# from collections import defaultdict
# import redis
# import os
# from src.utils import logger
# from slowapi.errors import RateLimitExceeded
# from fastapi.responses import JSONResponse

# # ==========================================
# # In-Memory Rate Limiter (Simple)
# # ==========================================

# class InMemoryRateLimiter:
#     """
#     Simple in-memory rate limiter
    
#     Good for single-instance deployments
#     """
#     def __init__(self):
#         self.requests = defaultdict(list)
#         self.cleanup_interval = 60
#         self.last_cleanup = time.time()

#     def is_allowed(
#             self,
#             key: str,
#             max_requests: int,
#             window_second: int 
#     ) -> bool:
#         """
#         Check if request is allowed
        
#         Args:
#             key: Identifier (e.g., IP address or user ID)
#             max_requests: Maximum requests allowed
#             window_seconds: Time window in seconds
            
#         Returns:
#             True if request is allowed
#         """
#         current_time = time.time()

#         if current_time - self.last_cleanup > self.cleanup_interval:
#             self._cleanup_old_entries(current_time)

#         timestamps = self.requests[key]

#         cutoff_time = current_time - window_second
#         timestamps = [ts for ts in timestamps if ts > cutoff_time]

#         if len(timestamps) >= max_requests:
#             return False

#         timestamps.append(current_time)
#         self.requests[key] = timestamps

#         return True
    
#     def _cleanup_old_entries(self, current_time: float):
#         """Remove old entries to prevent memory growth"""
#         cutoff_time = current_time - 3600

#         keys_to_remove = []
#         for key, timestamps in self.requests.items():
#             timestamps = [ts for ts in timestamps if ts > cutoff_time]
#             if not timestamps:
#                 keys_to_remove.append(key)
#             else:
#                 self.requests[key] = timestamps
        
#         for key in keys_to_remove:
#             del self.requests[key]
        
#         self.last_cleanup = current_time
#         logger.debug(f"Rate limiter cleanup: removed {len(keys_to_remove)} keys")

# # ==========================================
# # Redis-Based Rate Limiter (Production)
# # ==========================================

# class RedisrateLimiter:
#     """
#     Redis-based rate limiter
    
#     Good for multi-instance deployments (distributed)
#     """

#     def __init__(self, redis_url: str = None):
#         """
#         Initialize Redis connection
        
#         Args:
#             redis_url: Redis connection URL
#         """
#         redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")

#         try:
#             self.redis_client  = redis.from_url(redis_url, decode_responses=True)
#             self.redis_client.ping()
#             logger.info("Connected to Redis for rate limiting")

#         except Exception as e:
#             logger.warning(f"Redis connection failed: {e}. Falling back to in-memory limiter")
#             self.redis_client = None
#             self.fallback_limiter = InMemoryRateLimiter()
    
#     def is_allowed(
#             self,
#             key: str,
#             max_requests: int,
#             window_seconds: int
#     ) -> bool:
#         """
#         Check if request is allowed using Redis
        
#         Args:
#             key: Identifier (e.g., IP address or user ID)
#             max_requests: Maximum requests allowed
#             window_seconds: Time window in seconds
            
#         Returns:
#             True if request is allowed
#         """
#         if self.redis_client is None:
#             return self.fallback_limiter.is_allowed(key, max_requests, window_seconds)
        
#         try:
#             redis_key = f"rate_limit:{key}"

#             current = self.redis_client.get(redis_key)

#             if current is None:
#                 pipe = self.redis_client.pipeline()
#                 pipe.set(redis_key, 1)
#                 pipe.expire(redis_key, window_seconds)
#                 pipe.execute()
#                 return True
            
#             current_count = int(current)

#             if current_count >= max_requests:
#                 return False
            
#             self.redis_client.incr(redis_key)
#             return True
        
#         except Exception as e:
#             logger.error(f"Redis rate limit error: {e}")
#             return True
        
# # ==========================================
# # FastAPI Integration
# # ==========================================

# REDIS_URL = os.getenv("REDIS_URL")

# if REDIS_URL:
#     rate_limiter = RedisrateLimiter(REDIS_URL)
#     logger.info("Using Redis-based rate limiting")
# else:
#     rate_limiter = InMemoryRateLimiter()
#     logger.info("Using in-memory rate limiting")

# limiter = Limiter(
#     key_func=get_remote_address,
#     default_limits=["100/hour"],
#     storage_uri=REDIS_URL if REDIS_URL else "memory://"
# )

# # ==========================================
# # Custom Rate Limit Decorators
# # ==========================================

# def custom_rate_limit(max_requests: int, window_seconds: int):
#     """
#     Custom rate limit decorator
    
#     Usage:
#         @custom_rate_limit(max_requests=10, window_seconds=60)
#         async def my_endpoint():
#             ...
    
#     Args:
#         max_requests: Maximum requests allowed
#         window_seconds: Time window in seconds
#     """
#     def decorator(func: Callable) -> Callable:
#         async def wrapper(request: Request, *args, **kwargs):
#             identifier = get_remote_address(request)

#             if not rate_limiter.is_allowed(identifier, max_requests, window_seconds):
#                 raise HTTPException(
#                     status_code=status.HTTP_429_TOO_MANY_REQUESTS,
#                     detail=f"Rate limit exceeded. Max {max_requests} requests per {window_seconds} seconds."
#                 )
            
#             return await func(request, *args, **kwargs)
#         return wrapper
#     return decorator

# def user_rate_limit(max_requests: int, window_seconds: int):
#     """
#     User-based rate limit (uses user ID instead of IP)
    
#     Usage:
#         @user_rate_limit(max_requests=100, window_seconds=3600)
#         async def my_endpoint(current_user: User = Depends(get_current_user)):
#             ...
#     """
#     def decorator(func: Callable) -> Callable:
#         async def wrapper(*args, current_user=None, **kwargs):
#             if current_user is None:
#                 raise HTTPException(
#                     status_code=status.HTTP_401_UNAUTHORIZED,
#                     detail="Authentication required"
#                 )
#             identifier = f"user:{current_user.id}"

#             if not rate_limiter.is_allowed(identifier, max_requests, window_seconds):
#                 raise HTTPException(
#                     status_code=status.HTTP_429_TOO_MANY_REQUESTS,
#                     detail=f"Rate limit exceeded. Max {max_requests} requests per {window_seconds} seconds."
#                 )
#             return await func(*args, current_user=current_user, **kwargs)
#         return wrapper
#     return decorator

# # ==========================================
# # Rate Limit Info Headers
# # ==========================================

# def add_rate_limit_headers(response, limit: int, remaining: int, reset_time: int):
#     """
#     Add rate limit info to response headers
    
#     Args:
#         response: FastAPI response object
#         limit: Rate limit
#         remaining: Remaining requests
#         reset_time: Reset timestamp
#     """
#     response.headers["X-RateLimit-Limit"] = str(limit)
#     response.headers["X-RateLimit-Remaining"] = str(remaining)
#     response.headers["X-RateLimit-Reset"] = str(reset_time)

# async def _rate_limit_exceeded_handler(request, exc: RateLimitExceeded):
#     return JSONResponse(
#         status_code=429,
#         content={
#             "detail": "Rate limit exceeded",
#             "limit": str(exc.limit)
#         },
#     )
    

"""
Rate Limiting Module

Implements request rate limiting to prevent API abuse.
Updated to support testing mode.
"""

from slowapi import Limiter
from slowapi.util import get_remote_address
from fastapi import Request, HTTPException, status
from typing import Callable
import time
from collections import defaultdict
import redis
import os
from src.utils import logger
from slowapi.errors import RateLimitExceeded
from fastapi.responses import JSONResponse


# ==========================================
# Testing Mode Check
# ==========================================

def is_testing_mode() -> bool:
    """Check if running in testing mode"""
    return os.getenv("TESTING", "false").lower() == "true" or \
           os.getenv("DISABLE_RATE_LIMIT", "false").lower() == "true"


# ==========================================
# In-Memory Rate Limiter (Simple)
# ==========================================

class InMemoryRateLimiter:
    """
    Simple in-memory rate limiter
    
    Good for single-instance deployments
    """
    def __init__(self):
        self.requests = defaultdict(list)
        self.cleanup_interval = 60
        self.last_cleanup = time.time()

    def clear(self):
        """Clear all rate limit data - useful for testing"""
        self.requests.clear()
        self.last_cleanup = time.time()

    def is_allowed(
            self,
            key: str,
            max_requests: int,
            window_seconds: int 
    ) -> bool:
        """
        Check if request is allowed
        
        Args:
            key: Identifier (e.g., IP address or user ID)
            max_requests: Maximum requests allowed
            window_seconds: Time window in seconds
            
        Returns:
            True if request is allowed
        """
        # Always allow in testing mode
        if is_testing_mode():
            return True
        
        current_time = time.time()

        if current_time - self.last_cleanup > self.cleanup_interval:
            self._cleanup_old_entries(current_time)

        timestamps = self.requests[key]

        cutoff_time = current_time - window_seconds
        timestamps = [ts for ts in timestamps if ts > cutoff_time]

        if len(timestamps) >= max_requests:
            return False

        timestamps.append(current_time)
        self.requests[key] = timestamps

        return True
    
    def _cleanup_old_entries(self, current_time: float):
        """Remove old entries to prevent memory growth"""
        cutoff_time = current_time - 3600

        keys_to_remove = []
        for key, timestamps in self.requests.items():
            timestamps = [ts for ts in timestamps if ts > cutoff_time]
            if not timestamps:
                keys_to_remove.append(key)
            else:
                self.requests[key] = timestamps
        
        for key in keys_to_remove:
            del self.requests[key]
        
        self.last_cleanup = current_time
        logger.debug(f"Rate limiter cleanup: removed {len(keys_to_remove)} keys")


# ==========================================
# Redis-Based Rate Limiter (Production)
# ==========================================

class RedisRateLimiter:
    """
    Redis-based rate limiter
    
    Good for multi-instance deployments (distributed)
    """

    def __init__(self, redis_url: str = None):
        """
        Initialize Redis connection
        
        Args:
            redis_url: Redis connection URL
        """
        redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")

        try:
            self.redis_client = redis.from_url(redis_url, decode_responses=True)
            self.redis_client.ping()
            logger.info("Connected to Redis for rate limiting")

        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Falling back to in-memory limiter")
            self.redis_client = None
            self.fallback_limiter = InMemoryRateLimiter()
    
    def is_allowed(
            self,
            key: str,
            max_requests: int,
            window_seconds: int
    ) -> bool:
        """
        Check if request is allowed using Redis
        
        Args:
            key: Identifier (e.g., IP address or user ID)
            max_requests: Maximum requests allowed
            window_seconds: Time window in seconds
            
        Returns:
            True if request is allowed
        """
        # Always allow in testing mode
        if is_testing_mode():
            return True
        
        if self.redis_client is None:
            return self.fallback_limiter.is_allowed(key, max_requests, window_seconds)
        
        try:
            redis_key = f"rate_limit:{key}"

            current = self.redis_client.get(redis_key)

            if current is None:
                pipe = self.redis_client.pipeline()
                pipe.set(redis_key, 1)
                pipe.expire(redis_key, window_seconds)
                pipe.execute()
                return True
            
            current_count = int(current)

            if current_count >= max_requests:
                return False
            
            self.redis_client.incr(redis_key)
            return True
        
        except Exception as e:
            logger.error(f"Redis rate limit error: {e}")
            return True


# ==========================================
# Custom Limiter that respects testing mode
# ==========================================

class TestableLimit:
    """Wrapper that disables rate limiting in testing mode"""
    def __init__(self, limit_string: str):
        self.limit_string = limit_string
    
    def __call__(self, func):
        if is_testing_mode():
            # In testing mode, just return the function unchanged
            return func
        else:
            # In production, apply the actual limit
            return limiter.limit(self.limit_string)(func)
    
    def __str__(self):
        return self.limit_string


# ==========================================
# FastAPI Integration
# ==========================================

REDIS_URL = os.getenv("REDIS_URL")

if REDIS_URL:
    rate_limiter = RedisRateLimiter(REDIS_URL)
    logger.info("Using Redis-based rate limiting")
else:
    rate_limiter = InMemoryRateLimiter()
    logger.info("Using in-memory rate limiting")

# Create limiter - will be disabled in testing mode
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["100/hour"] if not is_testing_mode() else [],
    storage_uri=REDIS_URL if REDIS_URL else "memory://",
    enabled=not is_testing_mode()  # Disable in testing mode
)

if is_testing_mode():
    logger.warning("⚠️  RATE LIMITING DISABLED - TESTING MODE")


# ==========================================
# Custom Rate Limit Decorators
# ==========================================

def custom_rate_limit(max_requests: int, window_seconds: int):
    """
    Custom rate limit decorator
    
    Usage:
        @custom_rate_limit(max_requests=10, window_seconds=60)
        async def my_endpoint():
            ...
    
    Args:
        max_requests: Maximum requests allowed
        window_seconds: Time window in seconds
    """
    def decorator(func: Callable) -> Callable:
        async def wrapper(request: Request, *args, **kwargs):
            # Skip rate limiting in testing mode
            if is_testing_mode():
                return await func(request, *args, **kwargs)
            
            identifier = get_remote_address(request)

            if not rate_limiter.is_allowed(identifier, max_requests, window_seconds):
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Rate limit exceeded. Max {max_requests} requests per {window_seconds} seconds."
                )
            
            return await func(request, *args, **kwargs)
        return wrapper
    return decorator


def user_rate_limit(max_requests: int, window_seconds: int):
    """
    User-based rate limit (uses user ID instead of IP)
    
    Usage:
        @user_rate_limit(max_requests=100, window_seconds=3600)
        async def my_endpoint(current_user: User = Depends(get_current_user)):
            ...
    """
    def decorator(func: Callable) -> Callable:
        async def wrapper(*args, current_user=None, **kwargs):
            # Skip rate limiting in testing mode
            if is_testing_mode():
                return await func(*args, current_user=current_user, **kwargs)
            
            if current_user is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Authentication required"
                )
            identifier = f"user:{current_user.id}"

            if not rate_limiter.is_allowed(identifier, max_requests, window_seconds):
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Rate limit exceeded. Max {max_requests} requests per {window_seconds} seconds."
                )
            return await func(*args, current_user=current_user, **kwargs)
        return wrapper
    return decorator


# ==========================================
# Rate Limit Info Headers
# ==========================================

def add_rate_limit_headers(response, limit: int, remaining: int, reset_time: int):
    """
    Add rate limit info to response headers
    
    Args:
        response: FastAPI response object
        limit: Rate limit
        remaining: Remaining requests
        reset_time: Reset timestamp
    """
    response.headers["X-RateLimit-Limit"] = str(limit)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Reset"] = str(reset_time)


async def _rate_limit_exceeded_handler(request, exc: RateLimitExceeded):
    """Handle rate limit exceeded errors"""
    return JSONResponse(
        status_code=429,
        content={
            "detail": "Rate limit exceeded",
            "limit": str(exc.limit)
        },
    )