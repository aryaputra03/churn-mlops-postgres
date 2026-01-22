"""
Interactive Prediction Client

Usage:
    python scripts/prediction_client.py
"""

import requests
import json
import sys
from typing import Optional
from getpass import getpass
from datetime import datetime

class ChurnPredictionClient:
    """Client for Churn Prediction API"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.token: Optional[str] = None
        self.username: Optional[str] = None

    def login(self, username: str, password: str) -> bool:
        """Login and save token"""
        try:
            response = requests.post(
                f"{self.base_url}/auth/token",
                data={"username": username, "password": password}
            )
            
            if response.status_code == 200:
                data = response.json()
                self.token = data["access_token"]
                self.username = username
                print(f"Logged in as: {username}")
                return True
            else:
                print(f"Login failed: {response.json()['detail']}")
                return False
        except Exception as e:
            print(f"Connection error: {e}")
            return False   

    def predict(self, customer_data: dict) -> Optional[dict]:
        """Make single prediction"""
        if not self.token:
            print("Please login first!")
            return None
        
        try:
            response = requests.post(
                f"{self.base_url}/predict",
                headers={"Authorization": f"Bearer {self.token}"},
                json=customer_data
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Prediction failed: {response.json()}")
                return None
        except Exception as e:
            print(f"Error: {e}")
            return None 
        
    def batch_predict(self, customers: list) -> Optional[list]:
        """Make batch prediction"""
        if not self.token:
            print("Please login first!")
            return None
        
        try:
            response = requests.post(
                f"{self.base_url}/predict/batch",
                headers={"Authorization": f"Bearer {self.token}"},
                json={"customers": customers}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Batch prediction failed: {response.json()}")
                return None
        except Exception as e:
            print(f"Error: {e}")
            return None
        
    def get_history(self, limit: int = 20) -> Optional[list]:
        """Get prediction history"""
        if not self.token:
            print("Please login first!")
            return None
        
        try:
            response = requests.get(
                f"{self.base_url}/predictions/history?limit={limit}",
                headers={"Authorization": f"Bearer {self.token}"}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Failed to get history: {response.json()}")
                return None
        except Exception as e:
            print(f"Error: {e}")
            return None
    
    def display_prediction(self, result: dict):
        """Pretty print prediction result"""
        print("\n" + "=" * 60)
        print("PREDICTION RESULT")
        print("=" * 60)
        print(f"Customer ID: {result['customer_id']}")
        print(f"Timestamp: {result['timestamp']}")
        print()
        
        if result['prediction'] == 1:
            print("    CHURN PREDICTED!")
            print(f"   Churn Probability: {result['churn_probability']:.2%}")
            print(f"   Confidence: {'HIGH' if result['churn_probability'] > 0.7 else 'MEDIUM' if result['churn_probability'] > 0.5 else 'LOW'}")
            print()
            print("    Recommended Actions:")
            if result['churn_probability'] > 0.7:
                print("   • Contact customer immediately")
                print("   • Offer retention discount")
                print("   • Schedule retention call")
            else:
                print("   • Monitor customer activity")
                print("   • Consider targeted offers")
        else:
            print("    CUSTOMER WILL STAY")
            print(f"   Stay Probability: {result['no_churn_probability']:.2%}")
            print()
            print("   Recommended Actions:")
            print("   • Maintain current service level")
            print("   • Consider upsell opportunities")
        
        print("=" * 60)

def interactive_mode():
    """Interactive prediction mode"""
    print("=" * 60)
    print("   CHURN PREDICTION CLIENT")
    print("=" * 60)
    
    # Initialize client
    base_url = input("API URL (default: http://localhost:8000): ").strip()
    if not base_url:
        base_url = "http://localhost:8000"
    
    client = ChurnPredictionClient(base_url)
    
    # Login
    print("\n--- LOGIN ---")
    username = input("Username: ")
    password = getpass("Password: ")
    
    if not client.login(username, password):
        sys.exit(1)
    
    # Main loop
    while True:
        print("\n" + "=" * 60)
        print("MENU")
        print("=" * 60)
        print("1. Make Single Prediction")
        print("2. View Prediction History")
        print("3. Quick Test Prediction")
        print("4. Logout & Exit")
        
        choice = input("\nSelect option (1-4): ").strip()
        
        if choice == "1":
            # Single prediction
            print("\n--- CUSTOMER DATA ---")
            customer_data = {
                "customer_id": input("Customer ID: "),
                "gender": input("Gender (Male/Female): "),
                "tenure": int(input("Tenure (months): ")),
                "monthly_charges": float(input("Monthly Charges ($): ")),
                "total_charges": float(input("Total Charges ($): ")),
                "contract": input("Contract (Month-to-month/One year/Two year): "),
                "payment_method": input("Payment Method: "),
                "internet_service": input("Internet Service (DSL/Fiber optic/No): ")
            }
            
            print("\nMaking prediction...")
            result = client.predict(customer_data)
            
            if result:
                client.display_prediction(result)
        
        elif choice == "2":
            # View history
            limit = input("How many recent predictions? (default: 20): ").strip()
            limit = int(limit) if limit else 20
            
            print(f"\nFetching last {limit} predictions...")
            history = client.get_history(limit)
            
            if history:
                print("\n" + "=" * 60)
                print(f"PREDICTION HISTORY ({len(history)} records)")
                print("=" * 60)
                
                for i, pred in enumerate(history, 1):
                    result = "CHURN" if pred['prediction'] == 1 else "STAY"
                    prob = pred['probability']
                    print(f"{i}. Customer: {pred['customer_id']}")
                    print(f"   Result: {result} (prob: {prob:.2%})")
                    print(f"   Time: {pred['created_at']}")
                    print()
        
        elif choice == "3":
            # Quick test
            print("\n--- QUICK TEST ---")
            test_data = {
                "customer_id": "TEST_QUICK",
                "gender": "Female",
                "tenure": 12,
                "monthly_charges": 70.35,
                "total_charges": 844.20,
                "contract": "Month-to-month",
                "payment_method": "Electronic check",
                "internet_service": "Fiber optic"
            }
            
            print("Using test customer data:")
            print(json.dumps(test_data, indent=2))
            print("\nMaking prediction...")
            
            result = client.predict(test_data)
            if result:
                client.display_prediction(result)
        
        elif choice == "4":
            print("\n  Goodbye!")
            break
        
        else:
            print("   Invalid option!")

if __name__ == "__main__":
    try:
        interactive_mode()
    except KeyboardInterrupt:
        print("\n\n  Interrupted by user. Goodbye!")
        sys.exit(0)