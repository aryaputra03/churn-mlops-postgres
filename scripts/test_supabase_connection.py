"""
Test Supabase Connection

Quick script to test Supabase database connection.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parents))

from src.api.database import engine, get_pool_status
from src.utils import logger
from dotenv import load_dotenv
import os

def test_connection():
    """Test database connection"""

    logger.info("=" * 60)
    logger.info("TESTING SUPABASE CONNECTION")
    logger.info("=" * 60)

    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    logger.info(f"Database URL: {database_url[:40]}...")

    try:
        logger.info("\nConnecting to database...")
        with engine.connect() as conn:
            result = conn.execute("SELECT version()")
            version = result.fetchone()[0]
            logger.info("Connected successfully!")
            logger.info(f"PostgreSQL Version: {version}")

            result = conn.execute("SELECT current_database()")
            db_name = result.fetchone()[0]
            logger.info(f"Current Database: {db_name}")

            result = conn.execute("SELECT current_user")
            user = result.fetchone()[0]
            logger.info(f"Current User: {user}")

        pool_status = get_pool_status()
        logger.info("Connection Pool Status:")
        for key, value in pool_status.items():
            logger.info(f"   {key}: {value}")
        
        logger.info("\n" + "=" * 60)
        logger.info("CONNECTION TEST SUCCESSFUL!")
        logger.info("=" * 60)

        return True
    
    except Exception as e:
        logger.error("\n" + "=" * 60)
        logger.error(f"CONNECTION TEST FAILED!")
        logger.error("=" * 60)
        logger.error(f"\nError: {str(e)}")
        logger.error("\nPlease check:")
        logger.error("1. DATABASE_URL in .env file is correct")
        logger.error("2. Supabase project is active")
        logger.error("3. Database password is correct")
        logger.error("4. Network connectivity to Supabase")

        import traceback
        traceback.print_exc()

        return False
    
if __name__ == "__main__":
    sucess = test_connection()
    sys.exit(0 if sucess else 1)