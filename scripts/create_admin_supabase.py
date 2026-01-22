"""
Create Admin User in Supabase PostgreSQL

Run this after setting up Supabase to create your first admin user.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.database import init_db, SessionLocal
from src.api import crud
from src.utils import logger
import getpass


def create_admin():
    """Create admin user interactively"""
    logger.info("=" * 60)
    logger.info("CREATE ADMIN USER - SUPABASE POSTGRESQL")
    logger.info("=" * 60)

    supabase_url = os.getenv("SUPABASE_DB_URL")
    if not supabase_url:
        logger.error("SUPABASE_DB_URL not set!")
        logger.info("Set it in .env file or:")
        logger.info('  export SUPABASE_DB_URL="postgresql://..."')
        return 1
    
    logger.info("Initializing database...")
    init_db()

    print("\nEnter admin details:")
    username = input("Username: ").strip()
    email = input("Email: ").strip()
    password = getpass.getpass("Confirm Password: ").strip()
    password_confirm = getpass.getpass("Confirm Password: ").strip()
    full_name = input("Full Name (optional): ").strip() or None

    if not username or not email or not password:
        logger.error("Username, email, and password are required!")
        return 1
    if password != password_confirm:
        logger.error("Passwords do not match!")
        return 1
    if len(password) < 8:
        logger.error("Password must be at least 8 characters!")
        return 1
    
    db = SessionLocal()
    try:
        existing = crud.get_user_by_username(db, username)
        if existing:
            logger.error(f"Username '{username}' already exists!")
            return 1
        
        existing_email = crud.get_user_by_email(db, email)
        if existing_email:
            logger.error(f"Email '{email}' already registered!")
            return 1
        
        logger.info(f"\nCreating admin user '{username}'...")
        admin = crud.create_user(
            db=db,
            username=username,
            email=email,
            password=password,
            full_name=full_name,
            role="admin"
        )
        logger.info("\n" + "=" * 60)
        logger.info("ADMIN USER CREATED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"Username: {admin.username}")
        logger.info(f"Email: {admin.email}")
        logger.info(f"Role: {admin.role}")
        logger.info(f"ID: {admin.id}")
        logger.info("=" * 60)
        logger.info("\nIMPORTANT: Save these credentials securely!")

        return 0
    
    except Exception as e:
        logger.error(f"\nFailed to create admin user: {e}")
        db.rollback()
        return 1
    
    finally:
        db.close()

if __name__ == "__main__":
    sys.exit(create_admin())

