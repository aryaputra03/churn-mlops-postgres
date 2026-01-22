# """
# Database Configuration and Session Management

# SQLAlchemy setup for PostgreSQL/SQLite database.
# """

# from sqlalchemy import create_engine
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker, Session
# from sqlalchemy.pool import QueuePool, NullPool
# from dotenv import load_dotenv
# from typing import Generator
# import os
# from urllib.parse import quote_plus

# from src.utils import logger
# # ==========================================
# # Database Configuration
# # ==========================================

# load_dotenv()

# # Get database URL from environment or use SQLite as fallback
# DATABASE_URL = os.getenv(
#     "DATABASE_URL",
#     "sqlite:///./churn_api.db"
# )

# DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "20"))
# DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))
# DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
# DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))


# # ==========================================
# # Engine Configuration
# # ==========================================
# # Handle PostgreSQL URL format (for Heroku/Railway)
# if "supabase" in DATABASE_URL or "pooler.supabase.com" in DATABASE_URL:
#     logger.info("Using Supabase PostgreSQL")

#     connect_arg = {
#         "sslmode": "require",
#         "connect_timeout": 10 
#     }

#     engine = create_engine(
#         DATABASE_URL,
#         poolclass=QueuePool,
#         pool_size=DB_POOL_SIZE,
#         max_overflow=DB_MAX_OVERFLOW,
#         pool_timeout=DB_POOL_TIMEOUT,
#         pool_recycle=DB_POOL_RECYCLE,
#         pool_pre_ping=True,
#         connect_args=connect_arg,
#         echo=False
#     )

# elif "postgresql" in DATABASE_URL:
#     logger.info("Using PostgreSQL")

#     engine = create_engine(
#         DATABASE_URL,
#         poolclass=QueuePool,
#         pool_size=DB_POOL_SIZE,
#         max_overflow=DB_MAX_OVERFLOW,
#         pool_timeout=DB_POOL_TIMEOUT,
#         pool_recycle=DB_POOL_RECYCLE,
#         pool_pre_ping=True
#     )

# else:
#     logger.warning("Using SQLite (not recommended for production)")

#     engine = create_engine(
#         DATABASE_URL,
#         connect_args={"check_same_thread": False},
#         echo=False
#     )

# # ==========================================
# # Session Configuration
# # ==========================================

# # Create SessionLocal class
# SessionLocal = sessionmaker(
#     autocommit=False,
#     autoflush=False,
#     bind=engine
# )

# # Create Base class for models
# Base = declarative_base()

# # ==========================================
# # Database Dependency
# # ==========================================

# def get_db() -> Generator[Session, None, None]:
#     """
#     Database session dependency for FastAPI
    
#     Usage:
#         @app.get("/items")
#         def read_items(db: Session = Depends(get_db)):
#             ...
    
#     Yields:
#         Database session
#     """
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()

# # ==========================================
# # Database Initialization
# # ==========================================

# def init_db():
#     """
#     Initialize database tables
    
#     Creates all tables defined in models
#     """
#     try:
#         Base.metadata.create_all(bind=engine)
#         logger.info("Database tables created successfully")

#         with engine.connect() as conn:
#             result = conn.execute("SELECT 1")
#             logger.info("Database connection test successful")
#     except Exception as e:
#         logger.error(f"Failed to create database tables: {e}")
#         raise

# def drop_db():
#     """
#     Drop all database tables
    
#     WARNING: This will delete all data!
#     """
#     try:
#         Base.metadata.drop_all(bind=engine)
#         logger.warning("All database tables dropped")
#     except Exception as e:
#         logger.error(f"Failed to drop database tables: {e}")
#         raise

# def reset_db():
#     """
#     Reset database - drop and recreate all tables
    
#     USE WITH CAUTION! This will delete all data.
#     """
#     drop_db()
#     init_db()

# # ==========================================
# # Health Check
# # ==========================================

# def check_db_connection() -> bool:
#     """
#     Check if database connection is working
    
#     Returns:
#         True if connection is successful
#     """
#     try:
#         db = SessionLocal()
#         db.execute("SELECT 1")
#         db.close()
#         return True
#     except Exception as e:
#         logger.error(f"Database connection check failed: {e}")
#         return False
    
# # ==========================================
# # Connection Pool Monitoring
# # ==========================================

# def get_pool_status():
#     """
#     Get current connection pool status
    
#     Returns:
#         Dictionary with pool statistics
#     """
#     pool = engine.pool
#     return {
#         "pool_size": pool.size(),
#         "checked_in": pool.checkedin(),
#         "checked_out": pool.checkedout(),
#         "overflow": pool.overflow(),
#         "total_connection": pool.size() + pool.overflow()
#     }

# def log_pool_status():
#     """Log connection pool status"""
#     status = get_pool_status()
#     logger.info(f"Connection Pool Status: {status}")

"""
Database Configuration and Session Management

PostgreSQL (Supabase) setup for production.
"""

from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator
import os
from urllib.parse import quote_plus

from src.utils import logger

# ==========================================
# Database Configuration
# ==========================================

def get_database_url() -> str:
    """
    Get database URL from environment with fallback
    
    Priority:
    1. DATABASE_URL (full connection string)
    2. SUPABASE_DB_URL (Supabase connection string)
    3. Individual components (SUPABASE_HOST, etc.)
    4. SQLite fallback (development only)
    """
    # Option 1: Full DATABASE_URL
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        if database_url.startswith("postgres://"):
            database_url = database_url.replace("postgres://", "postgresql://", 1)
        logger.info("Using DATABASE_URL from environment")
        return database_url
    
    # Option 2: SUPABASE_DB_URL (connection pooling)
    supabase_url = os.getenv("SUPABASE_DB_URL")
    if supabase_url:
        if supabase_url.startswith("postgres://"):
            supabase_url = supabase_url.replace("postgres://", "postgresql://", 1)
        logger.info("Using SUPABASE_DB_URL from environment")
        return supabase_url
    
    # Option 3: Build from components
    supabase_host = os.getenv("SUPABASE_HOST")
    supabase_password = os.getenv("SUPABASE_PASSWORD")
    supabase_project_ref = os.getenv("SUPABASE_PROJECT_REF")
    
    if all([supabase_host, supabase_password, supabase_project_ref]):
        # URL encode password to handle special characters
        encoded_password = quote_plus(supabase_password)
        
        # Use connection pooling (port 6543) for production
        database_url = (
            f"postgresql://postgres.{supabase_project_ref}:{encoded_password}"
            f"@{supabase_host}:6543/postgres"
        )
        logger.info("Built DATABASE_URL from Supabase components")
        return database_url
    
    # Option 4: SQLite fallback (DEVELOPMENT ONLY)
    logger.warning("No PostgreSQL config found! Using SQLite (NOT for production)")
    return "sqlite:///./churn_api.db"


DATABASE_URL = get_database_url()

# Log database type (hide password)
if DATABASE_URL.startswith("postgresql"):
    safe_url = DATABASE_URL.split("@")[1] if "@" in DATABASE_URL else "***"
    logger.info(f"Using PostgreSQL: {safe_url}")
else:
    logger.info(f"Using SQLite (development mode)")

# ==========================================
# Engine Configuration
# ==========================================

if DATABASE_URL.startswith("sqlite"):
    # SQLite specific settings (development)
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=False,
        pool_pre_ping=True
    )
    logger.info("SQLite engine configured")
    
else:
    # PostgreSQL specific settings (production)
    engine = create_engine(
        DATABASE_URL,
        pool_size=10,              
        max_overflow=20,           
        pool_timeout=30,           
        pool_recycle=3600,         
        pool_pre_ping=True,        
        echo=False,                
    )
    
    logger.info("PostgreSQL engine configured")
    
    # Add event listener to set session timezone
    @event.listens_for(engine, "connect")
    def set_timezone(dbapi_conn, connection_record):
        """Set timezone to UTC for all connections"""
        cursor = dbapi_conn.cursor()
        cursor.execute("SET timezone='UTC'")
        cursor.close()

# ==========================================
# Session Configuration
# ==========================================

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    expire_on_commit=False
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
    except Exception as e:
        db.rollback()
        logger.error(f"Database session error: {e}")
        raise
    finally:
        db.close()

# ==========================================
# Database Initialization
# ==========================================

def init_db():
    """
    Initialize database tables
    
    Creates all tables defined in models.
    Safe to run multiple times (won't drop existing data).
    """
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created/verified successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to create database tables: {e}")
        raise


def drop_db():
    """
    Drop all database tables
    
    WARNING: This will delete ALL data!
    Only use in development/testing.
    """
    try:
        if not DATABASE_URL.startswith("sqlite"):
            confirm = input("WARNING: Drop ALL tables in PostgreSQL? Type 'YES' to confirm: ")
            if confirm != "YES":
                logger.info("Operation cancelled")
                return False
        
        Base.metadata.drop_all(bind=engine)
        logger.warning("All database tables dropped")
        return True
    except Exception as e:
        logger.error(f"Failed to drop database tables: {e}")
        raise

# ==========================================
# Health Check
# ==========================================

def check_db_connection() -> bool:
    """
    Check if database connection is working
    
    Returns:
        True if connection successful, False otherwise
    """
    try:
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()
        logger.info("Database connection successful")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


def get_db_info() -> dict:
    """
    Get database information
    
    Returns:
        Dictionary with database details
    """
    try:
        db = SessionLocal()
        
        if DATABASE_URL.startswith("postgresql"):

            result = db.execute("SELECT version()")
            version = result.fetchone()[0]
            
            result = db.execute("SELECT current_database()")
            database_name = result.fetchone()[0]
            
            result = db.execute("SELECT current_user")
            user = result.fetchone()[0]
            
            db.close()
            
            return {
                "type": "PostgreSQL",
                "version": version.split(",")[0],
                "database": database_name,
                "user": user,
                "host": DATABASE_URL.split("@")[1].split(":")[0] if "@" in DATABASE_URL else "unknown"
            }
        else:
            result = db.execute("SELECT sqlite_version()")
            version = result.fetchone()[0]
            db.close()
            
            return {
                "type": "SQLite",
                "version": version,
                "database": DATABASE_URL.replace("sqlite:///", ""),
                "user": "local",
                "host": "local"
            }
    except Exception as e:
        logger.error(f"Failed to get database info: {e}")
        return {
            "type": "Unknown",
            "error": str(e)
        }


# ==========================================
# Startup Check
# ==========================================

def verify_database_setup():
    """
    Verify database is properly configured
    Should be called on application startup
    """
    logger.info("=" * 60)
    logger.info("DATABASE CONFIGURATION CHECK")
    logger.info("=" * 60)
    
    # Get database info
    db_info = get_db_info()
    logger.info(f"Database Type: {db_info.get('type')}")
    logger.info(f"Version: {db_info.get('version', 'unknown')}")
    logger.info(f"Database: {db_info.get('database', 'unknown')}")
    logger.info(f"User: {db_info.get('user', 'unknown')}")
    logger.info(f"Host: {db_info.get('host', 'unknown')}")
    
    # Test connection
    if check_db_connection():
        logger.info("Database connection: OK")
    else:
        logger.error("Database connection: FAILED")
        raise Exception("Database connection failed!")
    
    # Initialize tables
    init_db()
    
    logger.info("=" * 60)
    logger.info("DATABASE READY")
    logger.info("=" * 60)

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

if os.getenv("SKIP_DB_VERIFY") != "true":
    try:
        verify_database_setup()
    except Exception as e:
        logger.error(f"Database verification failed: {e}")