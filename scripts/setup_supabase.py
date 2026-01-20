"""
Setup Supabase Database

Script to initialize Supabase database with all required tables.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.database import init_db, engine, get_pool_status
from src.api import crud, schemas
from src.api.auth import get_password_hash
from src.utils import logger
from dotenv import load_dotenv
import os

def create_initial_users(db):
    """Create initial admin and test users"""
    logger.info("Creating initial users...")

    try:
        admin_exists = crud.get_user_by_username(db, "admin")
        if not admin_exists:
            admin = crud.create_user(
                db=db,
                username="admin",
                email="admin@churnpredict.com",
                password="Admin123!",
                full_name="System Administrator",
                role="admin"
            )
            logger.info(f"Admin user created: {admin.username}")
        else:
            logger.info("Admin user already exists")
    except Exception as e:
        logger.error(f"Failed to create admin user: {str(e)}")

    try:
        test_exists = crud.get_user_by_username(db, "testuser")
        if not test_exists:
            test_user = crud.create_user(
                db=db,
                username="testuser",
                email="test@churnpredict.com",
                password="Test1234!",
                full_name="Test User",
                role="user"
            )
            logger.info(f"Test user created: {test_user.username}")
        else:
            logger.info("Test user already exists")
    except Exception as e:
        logger.error(f"Failed to create test user: {str(e)}")

def main():
    """Main setup function"""
    logger.info("=" * 60)
    logger.info("SUPABASE DATABASE SETUP")
    logger.info("=" * 60)

    load_dotenv()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.info("DATABASE_URL not set in environment variables!")
        logger.info("Please set it in .env file")
        sys.exit(1)
    
    if "supabase" not in database_url:
        logger.warning("DATABASE_URL doesn't appear to be Supabase")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(0)
    
    logger.info(f"Database URL: {database_url[:30]}...")

    try:
        logger.info("Testing database connection...")
        with engine.connect() as conn:
            result = conn.execute("SELECT version()")
            version = result.fetchone()[0]
            logger.info(f"Connected to: {version}")
        
        pool_status = get_pool_status()
        logger.info(f"Connection Pool: {pool_status}")

        logger.info("Creating database tables...")
        init_db()
        logger.info("All tables created successfully")

        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        logger.info("Created tables:")
        for table in tables:
            logger.info(f"   - {table}")
        
        from src.api.database import SessionLocal
        db = SessionLocal()
        try:
            logger.info("Creating initial users...")
            create_initial_users(db)
        finally:
            db.close()
        
        logger.info("\n" + "=" * 60)
        logger.info("SUPABASE DATABASE SETUP COMPLETED!")
        logger.info("=" * 60)
        logger.info("\nNext steps:")
        logger.info("1. Update your .env file with Supabase credentials")
        logger.info("2. Run: uvicorn src.api.main:app --reload")
        logger.info("3. Access API docs: http://localhost:8000/docs")
        logger.info("\nDefault credentials:")
        logger.info("  Admin - username: admin, password: Admin123!")
        logger.info("  Test  - username: testuser, password: Test1234!")
        logger.info("\nIMPORTANT: Change default passwords in production!")

    except Exception as e:
        logger.error("\n" + "=" * 60)
        logger.error(f"SETUP FAILED: {str(e)}")
        logger.error("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()