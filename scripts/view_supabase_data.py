"""
View Supabase Data Directly

Usage:
    python scripts/view_supabase_data.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import logger

def get_engine():
    """Get database engine"""
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        logger.error("SUPABASE_DB_URL not set!")
        sys.exit(1)
    
    return create_engine(db_url)

def view_recent_predictions(limit: int = 20):
    """View recent predictions"""
    engine = get_engine()
    
    query = text(f"""
        SELECT 
            pl.id,
            pl.customer_id,
            CASE WHEN pl.prediction = 1 THEN 'CHURN' ELSE 'STAY' END as result,
            ROUND((pl.probability * 100)::numeric, 2) as churn_prob_pct,
            pl.input_data->>'contract' as contract,
            pl.input_data->>'tenure' as tenure,
            u.username as predicted_by,
            pl.created_at
        FROM prediction_logs pl
        JOIN users u ON pl.user_id = u.id
        ORDER BY pl.created_at DESC
        LIMIT {limit}
    """)
    
    df = pd.read_sql(query, engine)
    
    print("\n" + "=" * 100)
    print(f"   RECENT PREDICTIONS (Last {limit})")
    print("=" * 100)
    print(df.to_string(index=False))
    print("=" * 100)
    
    return df

def view_statistics():
    """View summary statistics"""
    engine = get_engine()
    
    query = text("""
        SELECT 
            COUNT(*) as total_predictions,
            SUM(CASE WHEN prediction = 1 THEN 1 ELSE 0 END) as churn_predictions,
            SUM(CASE WHEN prediction = 0 THEN 1 ELSE 0 END) as stay_predictions,
            ROUND((AVG(prediction) * 100)::numeric, 2) as churn_rate_pct,
            ROUND((AVG(probability) * 100)::numeric, 2) as avg_churn_prob_pct,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT user_id) as active_users,
            MIN(created_at) as first_prediction,
            MAX(created_at) as last_prediction
        FROM prediction_logs
    """)
    
    df = pd.read_sql(query, engine)
    
    print("\n" + "=" * 60)
    print("   OVERALL STATISTICS")
    print("=" * 60)
    
    stats = df.iloc[0]
    print(f"Total Predictions: {stats['total_predictions']:,}")
    print(f"Churn Predictions: {stats['churn_predictions']:,}")
    print(f"Stay Predictions: {stats['stay_predictions']:,}")
    print(f"Churn Rate: {stats['churn_rate_pct']}%")
    print(f"Avg Churn Probability: {stats['avg_churn_prob_pct']}%")
    print(f"Unique Customers: {stats['unique_customers']:,}")
    print(f"Active Users: {stats['active_users']:,}")
    print(f"First Prediction: {stats['first_prediction']}")
    print(f"Last Prediction: {stats['last_prediction']}")
    print("=" * 60)

def view_daily_trend(days: int = 7):
    """View daily trend"""
    engine = get_engine()
    
    query = text(f"""
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as total,
            SUM(CASE WHEN prediction = 1 THEN 1 ELSE 0 END) as churned,
            ROUND(AVG(prediction) * 100, 2) as churn_rate_pct
        FROM prediction_logs
        WHERE created_at >= CURRENT_DATE - INTERVAL '{days} days'
        GROUP BY DATE(created_at)
        ORDER BY date DESC
    """)
    
    df = pd.read_sql(query, engine)
    
    print("\n" + "=" * 60)
    print(f"   DAILY TREND (Last {days} days)")
    print("=" * 60)
    print(df.to_string(index=False))
    print("=" * 60)

def view_high_risk_customers():
    """View high-risk customers"""
    engine = get_engine()
    
    query = text("""
        SELECT
            customer_id,
            ROUND((probability * 100)::numeric, 2) AS churn_prob_pct,
            input_data->>'contract' AS contract,
            input_data->>'tenure' AS tenure,
            input_data->>'monthly_charges' AS monthly_charges,
            created_at
        FROM prediction_logs
        WHERE probability > 0.7
        AND created_at >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY probability DESC
        LIMIT 10;

    """)
    
    df = pd.read_sql(query, engine)
    
    print("\n" + "=" * 80)
    print("    HIGH-RISK CUSTOMERS (>70% churn probability)")
    print("=" * 80)
    if len(df) > 0:
        print(df.to_string(index=False))
    else:
        print("No high-risk customers in the last 7 days")
    print("=" * 80)

def main():
    """Main menu"""
    while True:
        print("\n" + "=" * 60)
        print("   SUPABASE DATA VIEWER")
        print("=" * 60)
        print("1. View Recent Predictions")
        print("2. View Statistics")
        print("3. View Daily Trend")
        print("4. View High-Risk Customers")
        print("5. Exit")
        
        choice = input("\nSelect option (1-5): ").strip()
        
        try:
            if choice == "1":
                limit = input("How many? (default: 20): ").strip()
                limit = int(limit) if limit else 20
                view_recent_predictions(limit)
            
            elif choice == "2":
                view_statistics()
            
            elif choice == "3":
                days = input("How many days? (default: 7): ").strip()
                days = int(days) if days else 7
                view_daily_trend(days)
            
            elif choice == "4":
                view_high_risk_customers()
            
            elif choice == "5":
                print("\n   Goodbye!")
                break
            
            else:
                print("   Invalid option!")
        
        except Exception as e:
            logger.error(f"Error: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
