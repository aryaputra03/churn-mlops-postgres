"""
Migration Script: SQLite → PostgreSQL (Supabase)

This script helps migrate existing data from SQLite to Supabase PostgreSQL.
Run this ONCE during migration.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.api.schemas import User, PredictionLog, ModelMetrics, Customer
from src.api.database import Base
from src.utils import logger
from tqdm import tqdm


def migrate_data(sqlite_url: str, postgres_url: str):
    """
    Migrate data from SQLite to PostgreSQL
    
    Args:
        sqlite_url: SQLite database URL
        postgres_url: PostgreSQL (Supabase) database URL
    """
    logger.info("=" * 60)
    logger.info("MIGRATION: SQLite → PostgreSQL (Supabase)")
    logger.info("=" * 60)

    logger.info("Connecting to SQLite (source)...")
    sqlite_engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})
    SqliteSession = sessionmaker(bind=sqlite_engine)
    
    logger.info("Connecting to PostgreSQL (destination)...")
    postgres_engine = create_engine(postgres_url, pool_pre_ping=True)
    PostgresSession = sessionmaker(bind=postgres_engine)
    
    # Create tables in PostgreSQL
    logger.info("Creating tables in PostgreSQL...")
    Base.metadata.create_all(bind=postgres_engine)
    logger.info("Tables created")
    
    # Migrate each table
    tables_to_migrate = [
        (User, "Users"),
        (Customer, "Customers"),
        (ModelMetrics, "Model Metrics"),
        (PredictionLog, "Prediction Logs"),
    ]
    
    for model, name in tables_to_migrate:
        logger.info(f"\nMigrating {name}...")
        
        sqlite_session = SqliteSession()
        postgres_session = PostgresSession()
        
        try:
            records = sqlite_session.query(model).all()
            total = len(records)
            
            if total == 0:
                logger.info(f"  No records found in {name}")
                continue
            
            logger.info(f"  Found {total} records")
            
            migrated = 0
            for record in tqdm(records, desc=f"  Migrating {name}"):
                record_dict = {
                    c.name: getattr(record, c.name) 
                    for c in record.__table__.columns
                }
                
                new_record = model(**record_dict)
                postgres_session.add(new_record)
                migrated += 1
                
                if migrated % 100 == 0:
                    postgres_session.commit()
            
            postgres_session.commit()
            logger.info(f"Migrated {migrated} {name}")
            
        except Exception as e:
            logger.error(f"Error migrating {name}: {e}")
            postgres_session.rollback()
            raise
        
        finally:
            sqlite_session.close()
            postgres_session.close()
    
    logger.info("\n" + "=" * 60)
    logger.info("MIGRATION COMPLETED SUCCESSFULLY!")
    logger.info("=" * 60)


def verify_migration(postgres_url: str):
    """
    Verify migration was successful
    
    Args:
        postgres_url: PostgreSQL database URL
    """
    logger.info("\n" + "=" * 60)
    logger.info("VERIFICATION")
    logger.info("=" * 60)
    
    postgres_engine = create_engine(postgres_url, pool_pre_ping=True)
    PostgresSession = sessionmaker(bind=postgres_engine)
    session = PostgresSession()
    
    try:
        # Count records in each table
        tables = [
            (User, "Users"),
            (Customer, "Customers"),
            (ModelMetrics, "Model Metrics"),
            (PredictionLog, "Prediction Logs"),
        ]
        
        for model, name in tables:
            count = session.query(model).count()
            logger.info(f"{name}: {count} records")
        
        logger.info("=" * 60)
        logger.info("VERIFICATION COMPLETED")
        logger.info("=" * 60)
        
    finally:
        session.close()


def main():
    """Main migration function"""
    sqlite_url = input("Enter SQLite database path (e.g., sqlite:///./churn_api.db): ").strip()
    if not sqlite_url:
        sqlite_url = "sqlite:///./churn_api.db"
    
    postgres_url = os.getenv("SUPABASE_DB_URL")
    if not postgres_url:
        postgres_url = input("Enter Supabase PostgreSQL URL: ").strip()
    
    if not postgres_url:
        logger.error("PostgreSQL URL required!")
        return 1