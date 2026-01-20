"""
Script to create admin user
"""
from src.api.database import SessionLocal
from src.api import crud
import sys

def create_admin():
    """Create admin user"""
    db = SessionLocal()

    try:
        existing_admin = crud.get_user_by_username(db, "admin")
        if existing_admin:
            print("Admin user already exists!")
            return
        
        admin = crud.create_user(
            db=db,
            username="admin",
            email="admin@example.com",
            password="Admin123!",
            full_name="System Administrator",
            role="admin"
        )

        print("Admin user created successfully!")
        print(f"   Username: {admin.username}")
        print(f"   Email: {admin.email}")
        print(f"   Role: {admin.role}")
        print("")
        print("IMPORTANT: Change the default password!")
    
    except Exception as e:
        print(f"Error creating admin: {str(e)}")
        sys.exit(1)
    
    finally:
        db.close()

if __name__=="__main__":
    create_admin()
