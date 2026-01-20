"""
Utility Functions Module

Common utility functions used across the ML pipeline including
data loading, saving, directory setup, and sample data generation.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Optional, List
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def setup_directories(directories: Optional[List[str]] = None) -> None:
    """
    Create necessary project directories
    
    Args:
        directories: List of directory paths to create
                    If None, creates default directories
    """
    if directories is None:
        directories = [
            'data/raw',
            'data/processed',
            'models',
            'logs',
            'plots',
            'metrics'
        ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.info(f"Directory ensured: {directory}")

def load_data(file_path: str, **kwargs) -> pd.DataFrame:
    """
    Load data from CSV file with error handling
    
    Args:
        file_path: Path to CSV file
        **kwargs: Additional arguments for pd.read_csv
        
    Returns:
        DataFrame containing the data
        
    Raises:
        FileNotFoundError: If file doesn't exist
        pd.errors.EmptyDataError: If file is empty
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Data file {file_path} not found.")
    
    logger.info(f"Loading data from {file_path}")

    try:
        df = pd.read_csv(file_path, **kwargs)
        logger.info(f"Data loaded: {df.shape[0]} rows x {df.shape[1]} columns")

        logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        logger.info(f"Missing values: {df.isnull().sum().sum()}")

        return df
    except pd.errors.EmptyDataError:
        logger.error(f"x empty data file: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def save_data(df: pd.DataFrame, file_path: str, **kwargs) -> None:
    """
    Save DataFrame to CSV file
    
    Args:
        df: DataFrame to save
        file_path: Output file path
        **kwargs: Additional arguments for df.to_csv
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Saving data to {file_path}")

    try:
        df.to_csv(file_path, index=False, **kwargs)
        file_size = file_path.stat().st_size/ 1024**2
        logger.info(f"Data saved successfully: {df.shape[0]} rows x {df.shape[1]} columns size: {file_size:.2f} MB")
    
    except Exception as e:
        logger.error(f"Error saving data: {str(e)}")
        raise

def generate_sample_data(output_path: str, n_samples: int = 1000) -> None:
    """
    Generate synthetic customer churn dataset for demonstration
    
    Creates a realistic dataset with:
    - Customer demographics (gender)
    - Service information (tenure, contract type)
    - Charges (monthly, total)
    - Payment method
    - Internet service type
    - Churn label (target)
    
    Args:
        output_path: Where to save the generated dataset
        n_samples: Number of samples to generate
    """
    logger.info(f"generating sample data with {n_samples} samples")

    np.random.seed(42)

    customer_ids = [f'C{i:05d}' for i in range(n_samples)]

    gender = np.random.choice(['Male', 'Female'], n_samples)

    tenure = np.random.randint(0, 72, n_samples)

    contract_types = ['Month-to-month', 'One year', 'Two year']
    contract_probs = [0.5, 0.3, 0.2]
    contract = np.random.choice(contract_types, n_samples, p=contract_probs)

    payment_method = [
        'Electronic check',
        'Mailed check',
        'Bank transfer (automatic)',
        'Credit card (automatic)'
    ]

    payment = np.random.choice(payment_method, n_samples)

    internet_services = ['DSL', 'Fiber optic', 'No']
    internet = np.random.choice(internet_services, n_samples, p=[0.4, 0.4, 0.2])

    base_charge = np.random.uniform(20, 50, n_samples)
    internet_charge = np.where(
        internet == 'Fiber optic',
        np.random.uniform(30, 50, n_samples),
        np.where(internet == 'DSL', np.random.uniform(10, 30, n_samples), 0)
    )

    monthly_charges = base_charge + internet_charge + np.random.normal(0, 5, n_samples)
    monthly_charges = np.clip(monthly_charges, 18.25, 118.75)

    total_charges = monthly_charges * tenure + np.random.normal(0, 100, n_samples)
    total_charges = np.maximum(total_charges, monthly_charges)

    churn_probability = (
        (73 - tenure) / 73 * 0.25 +
        (monthly_charges - 18.25)/100 * 0.15 +
        (contract == 'Month-to-month') * 0.25 +
        (payment == 'Electronic check') * 0.15 +
        np.random.uniform(0, 0.2, n_samples)
    )

    churn = (churn_probability > 0.5).astype(int)

    data = {
        "customerID": customer_ids,
        "gender": gender,
        "tenure": tenure,
        "Contract": contract,
        "PaymentMethod": payment,
        "InternetService": internet,
        "MonthlyCharges": np.round(monthly_charges, 2),
        "TotalCharges": np.round(total_charges, 2),
        "Churn": churn
    }

    df = pd.DataFrame(data)

    n_missing = int(n_samples * 0.02)
    missing_indices = np.random.choice(n_samples, n_missing, replace=False)
    df.loc[missing_indices, "TotalCharges"] = np.nan

    save_data(df, output_path)

    churn_rate = df['Churn'].mean()
    logger.info("Sample data generation complete")
    logger.info(f"Total samples: {n_samples:,}")
    logger.info(f"Churn rate: {churn_rate:.2%}")
    logger.info(f"Missing values: {df.isnull().sum().sum()}")
    logger.info(f"Features: {list(df.columns)}")

def save_metrics(metrics: dict, output_path: str)->None:
    """
    Save metrics to JSON file
    
    Args:
        metrics: Dictionary of metrics
        output_path: Path to save JSON file
    """
    output_path = Path(output_path)

    if output_path.exists() and output_path.is_dir():
        raise ValueError(
            f"Invalid output path: {output_path} is a directory, expected a file"
        )
    
    
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    logger.info(f"Mterics saved to {output_path}")

def load_metrics(input_path: str) -> dict:
    """
    Load metrics from JSON file
    
    Args:
        input_path: Path to JSON file
        
    Returns:
        Dictionary of metrics
    """
    with open(input_path, 'r') as f:
        metrics = json.load(f)
    
    logger.info(f"Metrics loaded from {input_path}")
    return metrics

def print_metrics(metrics: dict, title: str = 'Metrics') -> None:
    """
    Pretty print metrics to console
    
    Args:
        metrics: Dictionary of metrics
        title: Title to display
    """
    print("\n" + "="*60)
    print(f"{title}")
    print("="*60)
    for key, value in metrics.items():
        if isinstance(value, (int, float)) and key != 'confusion_matrix':
            print(f"{key:20s}:{value:.4f}")
        elif key == 'confusion_matrix':
            print(f"\n{key}:")
            if isinstance(value, list):
                for row in value:
                    print(f"  {row}")
    print("="*60 + "\n")

def validate_dataframe(
        df = pd.DataFrame(),
        required_columns: Optional[List[str]] = None,
        allow_missing: bool = False
) -> bool:
    """
    Validate DataFrame structure and content
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        allow_missing: Whether to allow missing values
        
    Returns:
        True if valid
        
    Raises:
        ValueError: If validation fails
    """
    if df is None or df.empty:
        raise ValueError("DataFrame is empty or None.")
    
    if required_columns:
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
    
    if not allow_missing and df.isnull.any().any():
        null_count = df.isnull().sum()
        null_cols = null_count[null_count > 0]
        raise ValueError(f"DataFrame contains missing values in columns: {null_cols.to_dict()}")
    
    logger.info("Dataframe validation passed.")
    return True

class Timer:
    """
    Simple context manager for timing code execution
    
    Example:
        >>> with Timer("Data loading"):
        >>>     df = pd.read_csv("data.csv")
    """
    def __init__(self, name: str = 'Operation'):
        self.name = name
        self.start_time = None

    def __enter__(self):
        import time
        self.start_time = time.time()
        logger.info(f"Starting: {self.name}")
        return self
    
    def __exit__(self, *args):
        import time
        elapsed = time.time() - self.start_time
        logger.info(f"Completed: {self.name} in {elapsed:.2f} seconds")

        