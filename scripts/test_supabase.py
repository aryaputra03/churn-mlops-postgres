"""
Test Supabase PostgreSQL Connection

Quick script to verify your Supabase setup is working.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.database import (
    check_db_connection,
    get_db_info,
    init_db,
    SessionLocal
)

from src.api import crud
from src.utils import logger


def test_connection():
    """Test basic database connection"""
    logger.info("=" * 60)
    logger.info("TEST 1: Connection Test")
    logger.info("=" * 60)

    if check_db_connection():
        logger.info("Connection successful!")
        return True
    else:
        logger.info("Connection failed!")
        return False

def test_database_info():
    """Get and display database information"""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 2: Database Information")
    logger.info("=" * 60)

    info = get_db_info()
    for key, value, in info.items():
        logger.info(f"{key}: {value}")

    return True

def test_table_creation():
    """Test creating tables"""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 3: Table Creation")
    logger.info("=" * 60)

    try:
        init_db()
        logger.info("Tables created successfully!")
        return True
    except Exception as e:
        logger.info(f"Table creation failed: {e}")
        return False

def test_crud_operations():
    """Test CRUD operations"""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 4: CRUD Operations")
    logger.info("=" * 60)

    db = SessionLocal()

    try:
        logger.info("Creating test user...")
        user = crud.create_user(
            db=db,
            username="test_supabase_user",
            email="test@supabase.com",
            password="TestPass123!",
            full_name="Supabase Test User"
        )  
        logger.info(f"User created: {user.username} (ID: {user.id})")

        logger.info("Reading user...")
        retrieved = crud.get_user_by_username(db, "test_supabase_user")
        logger.info(f"User retrieved: {retrieved.email}")

        logger.info("Updating user...")
        updated = crud.update_user(
            db=db,
            user_id=user.id,
            full_name="Updated Name"
        )
        logger.info(f"User updated: {updated.full_name}")

        logger.info("Deleting user...")
        deleted = crud.delete_user(db, user.id)
        logger.info(f"User deleted: {deleted}")

        return True
    
    except Exception as e:
        logger.error(f"CRUD test failed: {e}")
        db.rollback()
        return False
    
    finally:
        db.close()

def test_prediction_logging():
    """Test prediction logging"""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 5: Prediction Logging")
    logger.info("=" * 60)

    db = SessionLocal()

    try:
        user = crud.create_user(
            db=db,
            username="pred_test_user",
            email="pred@test.com",
            password="TestPass123!"
        )
        
        logger.info("Logging test prediction...")
        pred_log = crud.create_prediction_log(
            db=db,
            customer_id="TEST001",
            prediction=1,
            probability=0.75,
            input_data={"test": "data"},
            user_id=user.id
        )
        logger.info(f"Prediction logged: ID {pred_log.id}")

        logger.info("Retrieving predictions...")
        predictions = crud.get_prediction(db, user_id=user.id)
        logger.info(f"Retrieved {len(predictions)} predictions")

        crud.delete_user(db, user.id)
        
        return True
    
    except Exception as e:
        logger.error(f"Prediction logging test failed: {e}")
        db.rollback()
        return False
    
    finally:
        db.close()

def main():
    """Run all tests"""
    logger.info("SUPABASE POSTGRESQL CONNECTION TEST")
    logger.info("=" * 60)

    supabase_url = os.getenv("SUPABASE_DB_URL")
    if not supabase_url:
        logger.error("SUPABASE_DB_URL not set in environment!")
        logger.info("Set it in .env file or export it:")
        logger.info('  export SUPABASE_DB_URL="postgresql://..."')
        return 1
    
    tests = [
        ("Connection", test_connection),
        ("Database Info", test_database_info),
        ("Table Creation", test_table_creation),
        ("CRUD Operations", test_crud_operations),
        ("Prediction Logging", test_prediction_logging),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            logger.error(f"Test '{name}' crashed: {e}")
            results.append((name, False))

    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)

    for name, result in results:
        status = "PASS" if result else "FAIL"
        logger.info(f"{name:20s}: {status}")

    passed = sum(1 for _, r in results if r)
    total = len(results)

    logger.info("=" * 60)
    logger.info(f"Results: {passed}/{total} tests passed")
    logger.info("=" * 60)

    if passed == total:
        logger.info("\nALL TESTS PASSED!")
        logger.info("Your Supabase setup is working correctly!")
        return 0
    else:
        logger.error("\nSOME TESTS FAILED!")
        logger.error("Please check the errors above and fix them.")
        return 1
    
if __name__ == "__main__":
    sys.exit(main())