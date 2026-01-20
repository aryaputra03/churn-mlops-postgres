"""
Database Configuration and Session Management

SQLAlchemy setup for PostgreSQL/SQLite database.
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
from typing import Generator
import os

from src.utils import logger
# ==========================================
# Database Configuration
# ==========================================

load_dotenv()

# Get database URL from environment or use SQLite as fallback
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///./churn_api.db"
)

DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "20"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))
DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))


# ==========================================
# Engine Configuration
# ==========================================
# Handle PostgreSQL URL format (for Heroku/Railway)
if "supabase" in DATABASE_URL or "pooler.supabase.com" in DATABASE_URL:
    logger.info("Using Supabase PostgreSQL")

    connect_arg = {
        "sslmode": "require",
        "connect_timeout": 10 
    }

    engine = create_engine(
        DATABASE_URL,
        poolclass=QueuePool,
        pool_size=DB_POOL_SIZE,
        max_overflow=DB_MAX_OVERFLOW,
        pool_timeout=DB_POOL_TIMEOUT,
        pool_recycle=DB_POOL_RECYCLE,
        pool_pre_ping=True,
        connect_args=connect_arg,
        echo=False
    )

elif "postgresql" in DATABASE_URL:
    logger.info("Using PostgreSQL")

    engine = create_engine(
        DATABASE_URL,
        poolclass=QueuePool,
        pool_size=DB_POOL_SIZE,
        max_overflow=DB_MAX_OVERFLOW,
        pool_timeout=DB_POOL_TIMEOUT,
        pool_recycle=DB_POOL_RECYCLE,
        pool_pre_ping=True
    )

else:
    logger.warning("Using SQLite (not recommended for production)")

    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=False
    )

# ==========================================
# Session Configuration
# ==========================================

# Create SessionLocal class
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Create Base class for models
Base = declarative_base()

# ==========================================
# Database Dependency
# ==========================================

def get_db() -> Generator[Session, None, None]:
    """
    Database session dependency for FastAPI
    
    Usage:
        @app.get("/items")
        def read_items(db: Session = Depends(get_db)):
            ...
    
    Yields:
        Database session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==========================================
# Database Initialization
# ==========================================

def init_db():
    """
    Initialize database tables
    
    Creates all tables defined in models
    """
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")

        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            logger.info("Database connection test successful")
    except Exception as e:
        logger.error(f"Failed to create database tables: {e}")
        raise

def drop_db():
    """
    Drop all database tables
    
    WARNING: This will delete all data!
    """
    try:
        Base.metadata.drop_all(bind=engine)
        logger.warning("All database tables dropped")
    except Exception as e:
        logger.error(f"Failed to drop database tables: {e}")
        raise

def reset_db():
    """
    Reset database - drop and recreate all tables
    
    USE WITH CAUTION! This will delete all data.
    """
    drop_db()
    init_db()

# ==========================================
# Health Check
# ==========================================

def check_db_connection() -> bool:
    """
    Check if database connection is working
    
    Returns:
        True if connection is successful
    """
    try:
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()
        return True
    except Exception as e:
        logger.error(f"Database connection check failed: {e}")
        return False
    
# ==========================================
# Connection Pool Monitoring
# ==========================================

def get_pool_status():
    """
    Get current connection pool status
    
    Returns:
        Dictionary with pool statistics
    """
    pool = engine.pool
    return {
        "pool_size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "total_connection": pool.size() + pool.overflow()
    }

def log_pool_status():
    """Log connection pool status"""
    status = get_pool_status()
    logger.info(f"Connection Pool Status: {status}")