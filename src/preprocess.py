"""
Data Preprocessing Module

Handles data cleaning, feature engineering, encoding, and scaling.
Prepares raw data for model training.
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler, MinMaxScaler
import argparse
import sys
from src.config import Config
from src.utils import load_data, save_data, logger, Timer

class DataPreprocessor:
    """
    Data preprocessing pipeline
    
    Handles all preprocessing steps including:
    - Missing value imputation
    - Categorical encoding
    - Feature scaling
    - Feature engineering
    """
    def __init__(self, config: Config):
        """
        Initialize preprocessor
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.label_encoders = {}
        self.scaler = None
        self.feature_names = []

        scale_method = config.preprocess.get('scale_method', 'standard')
        if scale_method == 'standard':
            self.scaler = StandardScaler()
        elif scale_method == 'minmax':
            self.scaler = MinMaxScaler()
        else:
            logger.warning(f"Unknown scaling method {scale_method}, defaulting to standard scaler.")
            self.scaler = StandardScaler()
    
    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute complete preprocessing pipeline
        
        Args:
            df: Input raw dataframe
            
        Returns:
            Processed dataframe ready for training
        """
        logger.info("Starting preprocessing pipeline.")

        df = df.copy()

        with Timer("Missing Value Imputation"):
            df = self.handle_missing_values(df)

        with Timer("Feature Engineering"):
            df = self.feature_engineering(df)

        with Timer("Categorical Encoding"):
            categorical_features = self.config.preprocess['categorical_features']
            for col in categorical_features:
                if col in df.columns:
                    df = self.encode_categorical(df, col)

        with Timer("Scale Numerical Features"):
            numerical_features = self.config.preprocess['numerical_features']
            df = self.scale_numerical(df, numerical_features)

        target = self.config.preprocess['target']
        if target in df.columns:
            df[target] = self.encode_target(df[target])
        
        logger.info("Preprocessing completed.")
        return df
    
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in the dataset
        
        Strategy from config: 'median', 'mean', 'mode', or 'drop'
        """
        strategy = self.config.preprocess.get('handling_missing', 'median')
        logger.info(f"Handling missing values using strategy: {strategy}")

        missing_before = df.isnull().sum().sum()
        logger.info(f"Total missing values before: {missing_before}")

        if 'TotalCharges' in df.columns:
            df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
        
        if strategy == 'drop':
            df = df.dropna()
        elif strategy == 'median':
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

            categorical_cols = df.select_dtypes(include=['object']).columns
            df[categorical_cols] = df[categorical_cols].fillna(df[categorical_cols].mode().iloc[0])
        
        elif strategy == 'mean':
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())

            categorical_cols = df.select_dtypes(include=['object']).columns
            df[categorical_cols] = df[categorical_cols].fillna(df[categorical_cols].mode().iloc[0])

        missing_after = df.isnull().sum().sum()
        logger.info(f"  Missing values after: {missing_after}")

        return df
    
    def feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create new features from existing ones
        """
        fe_config = self.config.preprocess.get('feature_engineering', {})

        if not fe_config:
            return df
        
        logger.info("Create Engineered Feature")

        if fe_config.get("create_tenure_bins", False) and 'tenure' in df.columns:
            df['tenure_group'] = pd.cut(
                df['tenure'],
                bins=[0, 12, 24, 48, 72],
                labels=['0-1yr', '1-2yr', '2-4yr', '4-6yr']
            )
            logger.info("Created Tenure Group")

        if fe_config.get("create_charge_ratio", False):
            if 'MonthlyCharges' in df.columns and 'TotalCharges' in df.columns:
                df['charge_ratio'] = df['TotalCharges']/(df['MonthlyCharges']+1e-6)
                logger.info("Created Charge Ratio Tenure")
        
        return df
    
    def encode_categorical(self, df: pd.DataFrame, columns: str) -> pd.DataFrame:
        """
        Encode categorical column using LabelEncoder
        
        Args:
            df: DataFrame
            column: Column name to encode
            
        Returns:
            DataFrame with encoded column
        """
        if columns not in df.columns:
            logger.warning(f"Columns not found: {columns}")
            return df
        
        le = LabelEncoder()

        try:
            df[columns] = le.fit_transform(df[columns].astype(str))
            self.label_encoders[columns] = le
            logger.info(f"Encoded: {columns} ({len(le.classes_)} classes)")
        except Exception as e:
            logger.error(f"Failed to encode {columns}: {str(e)}")
        
        return df
    
    def scale_numerical(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """
        Scale numerical columns using configured scaler
        
        Args:
            df: DataFrame
            columns: List of column names to scale
            
        Returns:
            DataFrame with scaled columns
        """
        existing_cols = [col for col in columns if col in df.columns]

        if not existing_cols:
            logger.warning("No numerical columns found to scale")
            return df
        
        logger.info("Scalling numerical Columns Found to Scale")

        try:
            df[existing_cols] = self.scaler.fit_transform(df[existing_cols])
            logger.info(f"Scaled {len(existing_cols)} features")
        except Exception as e:
            logger.error(f"Scaling failed: {str(e)}")
        
        return df
    
    def encode_target(self, target_series: pd.Series)->pd.Series:
        """
        Encode target variable
        
        Args:
            target_series: Target column
            
        Returns:
            Encoded target
        """
        if target_series.dtype == "object":
            le = LabelEncoder()
            encoded = le.fit_transform(target_series)
            self.label_encoders['target'] = le
            logger.info(f"Target encoded: {list(le.classes_)}")
            return encoded
        return target_series
    
    def inverse_transform_target(self, encoded_value: np.ndarray) -> np.ndarray:
        """
        Inverse transform encoded target values
        
        Args:
            encoded_values: Encoded target values
            
        Returns:
            Original target values
        """
        if 'target' in self.label_encoders:
            return self.label_encoders['target'].inverse_transform(encoded_value)
        return encoded_value
    

def main():
    """
    Main preprocessing function
    
    Loads raw data, applies preprocessing, and saves processed data.
    """
    parser = argparse.ArgumentParser(
        description="Preprocess customer churn data"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="params.yml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Override input data path"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Override output data path"
    )

    args = parser.parse_args()

    try:
        logger.info("=" * 60)
        logger.info("DATA PREPROCESSING")
        logger.info("=" * 60)

        config = Config(args.config)

        raw_path = args.input or config.data['raw_path']
        processed_path  = args.output or config.data['processed_path']

        logger.info(f"Input: {raw_path}")
        df = load_data(raw_path)

        logger.info("\nRaw Data Summary:")
        logger.info(f"  Shape: {df.shape}")
        logger.info(f"  Columns: {list(df.columns)}")
        logger.info(f"  Dtypes:\n{df.dtypes.value_counts()}")

        processor = DataPreprocessor(config)
        df_processed = processor.preprocess(df)

        logger.info("\nProcessed Data Summary:")
        logger.info(f"  Shape: {df_processed.shape}")
        logger.info(f"  Columns: {list(df_processed.columns)}")

        logger.info(f"\nOutput: {processed_path}")
        save_data(df_processed, processed_path)

        logger.info("\n" + "=" * 60)
        logger.info("PREPROCESSING COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)

        return 0
    
    except Exception as e:
        logger.error("\n" + "=" * 60)
        logger.error(f"PREPROCESSING FAILED: {str(e)}")
        logger.error("=" * 60)
        import traceback
        traceback.print_exc()
        return 1
    
if __name__ == '__main__':
    sys.exit(main())
    