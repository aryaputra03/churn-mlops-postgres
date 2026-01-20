"""
ML Service for Loading and Running Model
"""

import joblib
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, Dict, Any
from sklearn.preprocessing import LabelEncoder, StandardScaler
from src.config import Config
from src.utils import logger

class MLService:
    """
    Service class for ML model operations
    """
    def __init__(self, model_path: str = None):
        """
        Initialize ML service
        
        Args:
            model_path: Path to model file
        """
        self.model = None
        self.model_path = model_path
        self.config = None
        self.model_info = {}
        self.label_encoders = {}
        self.scaler = None

        try:
            self.load_model()
        except Exception as e:
            logger.warning(f"Could not load model on init: {str(e)}")
    
    def load_model(self, model_path: str = None) -> None:
        """
        Load ML model from disk
        
        Args:
            model_path: Optional path to model file
        """
        try:
            self.config = Config("params.yml")

            if model_path is None:
                model_path = self.model_path or self.config.evaluate['model_path']
            
            model_path = Path(model_path)
            
            if not model_path.exists():
                raise FileNotFoundError(f"Model file not found: {model_path}")

            logger.info(f"Loading model from {model_path}")
            self.model = joblib.load(model_path)
            self.model_path = str(model_path)

            preprocessor_path = model_path.parent / "preprocessor.pkl"
            if preprocessor_path.exists():
                logger.info(f"Loading preprocessor from {preprocessor_path}")
                preprocessor = joblib.load(preprocessor_path)
                self.label_encoders = preprocessor.get('label_encoders', {})
                self.scaler = preprocessor.get('scaler')
                logger.info("Preprocessor loaded successfully")
            else:
                logger.warning("No saved preprocessor found, initializing new one")
                self._initialize_preprocessors()

            self.model_info = {
                "model_type": type(self.model).__name__,
                "model_version": "1.0.0",
                "model_path": str(model_path),
                "loaded_at": pd.Timestamp.now().isoformat()
            }
            
            logger.info("Model loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load model: {str(e)}")
            raise
    
    def _initialize_preprocessors(self):
        """Initialize label encoders and scaler with expected mappings"""
        self.label_encoders = {
            'gender': LabelEncoder().fit(['Female', 'Male']),
            'Contract': LabelEncoder().fit(['Month-to-month', 'One year', 'Two year']),
            'PaymentMethod': LabelEncoder().fit([
                'Bank transfer (automatic)',
                'Credit card (automatic)',
                'Electronic check',
                'Mailed check'
            ]),
            'InternetService': LabelEncoder().fit(['DSL', 'Fiber optic', 'No']),
            'tenure_group': LabelEncoder().fit(['0-1yr', '1-2yr', '2-4yr', '4-6yr'])
        }

        scale_method = self.config.preprocess.get('scale_method', 'standard')
        if scale_method == 'standard':
            self.scaler = StandardScaler()
        else:
            from sklearn.preprocessing import MinMaxScaler
            self.scaler = MinMaxScaler()
        
        representative_data = np.array([
            [0, 18.25, 18.25, 0.5],      
            [72, 118.75, 8564.75, 100],
            [36, 65.0, 2500.0, 40]
        ])
        self.scaler.fit(representative_data)
        
        logger.info("Preprocessors initialized")
    
    def is_model_loaded(self) -> bool:
        """Check if model is loaded"""
        return self.model is not None
    
    def preprocess_input(self, data: pd.DataFrame) -> np.ndarray:
        """
        Preprocess input data to match model expectations
        
        Args:
            data: Input dataframe
            
        Returns:
            Preprocessed numpy array ready for prediction
        """
        try:
            df = data.copy()
            
            logger.info(f"Input columns: {df.columns.tolist()}")
            logger.info(f"Input shape: {df.shape}")

            if 'customer_id' in df.columns:
                df = df.drop(columns=['customer_id'])

            column_mapping = {
                'gender': 'gender',
                'tenure': 'tenure',
                'monthly_charges': 'MonthlyCharges',
                'total_charges': 'TotalCharges',
                'contract': 'Contract',
                'payment_method': 'PaymentMethod',
                'internet_service': 'InternetService'
            }

            df = df.rename(columns=column_mapping)
            logger.info(f"After rename: {df.columns.tolist()}")

            if 'TotalCharges' in df.columns:
                df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
                df['TotalCharges'] = df['TotalCharges'].fillna(df['MonthlyCharges'])

            logger.info("Creating engineered features...")

            if 'tenure' in df.columns:
                df['tenure_group'] = pd.cut(
                    df['tenure'],
                    bins=[0, 12, 24, 48, 72],
                    labels=['0-1yr', '1-2yr', '2-4yr', '4-6yr']
                )
                logger.info(f"Created tenure_group: {df['tenure_group'].unique()}")

            if 'MonthlyCharges' in df.columns and 'TotalCharges' in df.columns:
                df['charge_ratio'] = df['TotalCharges'] / (df['MonthlyCharges'] + 1e-6)
                logger.info(f"Created charge_ratio (sample): {df['charge_ratio'].iloc[0]:.4f}")

            if 'PaymentMethod' in df.columns:
                payment_mapping = {
                    'bank transfer': 'Bank transfer (automatic)',
                    'bank transfer (automatic)': 'Bank transfer (automatic)',
                    'credit card': 'Credit card (automatic)',
                    'credit card (automatic)': 'Credit card (automatic)',
                    'electronic check': 'Electronic check',
                    'mailed check': 'Mailed check'
                }
                df['PaymentMethod'] = df['PaymentMethod'].str.lower().map(payment_mapping)

                if df['PaymentMethod'].isna().any():
                    logger.warning(f"Unknown payment methods: {data['payment_method'].unique()}")
                    df['PaymentMethod'] = df['PaymentMethod'].fillna('Electronic check')

            categorical_cols = ['gender', 'Contract', 'PaymentMethod', 'InternetService', 'tenure_group']
            for col in categorical_cols:
                if col in df.columns:
                    try:
                        original_values = df[col].unique()
                        logger.info(f"Encoding {col}: {original_values}")
                        
                        df[col] = self.label_encoders[col].transform(df[col].astype(str))
                        logger.info(f"Encoded {col} successfully")
                    except Exception as e:
                        logger.error(f"Error encoding {col}: {str(e)}")
                        logger.error(f"Values: {df[col].unique()}")
                        logger.error(f"Expected classes: {self.label_encoders[col].classes_}")
                        raise

            numerical_cols = ['tenure', 'MonthlyCharges', 'TotalCharges', 'charge_ratio']
            existing_numerical = [col for col in numerical_cols if col in df.columns]
            
            if existing_numerical and self.scaler is not None:
                logger.info(f"Scaling numerical features: {existing_numerical}")
                df[existing_numerical] = self.scaler.transform(df[existing_numerical])

            expected_order = ['gender', 'tenure', 'MonthlyCharges', 'TotalCharges', 
                             'Contract', 'PaymentMethod', 'InternetService',
                             'tenure_group', 'charge_ratio']

            missing_cols = set(expected_order) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Missing columns after preprocessing: {missing_cols}")

            df = df[expected_order]
            logger.info(f"Final preprocessed shape: {df.shape}")
            logger.info(f"Sample values: {df.iloc[0].tolist()}")
            
            return df.values
            
        except Exception as e:
            logger.error(f"Preprocessing failed: {str(e)}")
            logger.error(f"DataFrame info: {df.head() if 'df' in locals() else 'N/A'}")
            raise
    
    def predict(self, data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """
        Make predictions
        
        Args:
            data: Input dataframe
            
        Returns:
            predictions, probabilities
        """
        if not self.is_model_loaded():
            raise RuntimeError("Model not loaded. Call load_model() first.")
        
        try:
            logger.info(f"Starting prediction for {len(data)} samples")

            X = self.preprocess_input(data)
            logger.info(f"Preprocessed data shape: {X.shape}")

            predictions = self.model.predict(X)
            probabilities = self.model.predict_proba(X)
            
            logger.info(f"Predictions: {predictions}")
            logger.info(f"Probabilities shape: {probabilities.shape}")
            
            return predictions, probabilities
            
        except Exception as e:
            logger.error(f"Prediction failed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get model information"""
        if not self.is_model_loaded():
            return {
                "model_type": "Not loaded",
                "model_version": "N/A",
                "features": [],
                "trained_at": None,
                "accuracy": None
            }
        
        features = []
        if hasattr(self.model, 'feature_names_in_'):
            features = self.model.feature_names_in_.tolist()
        else:
            features = ['gender', 'tenure', 'MonthlyCharges', 'TotalCharges', 
                       'Contract', 'PaymentMethod', 'InternetService',
                       'tenure_group', 'charge_ratio']
        
        return {
            "model_type": self.model_info.get("model_type", "Unknown"),
            "model_version": self.model_info.get("model_version", "1.0.0"),
            "features": features,
            "trained_at": self.model_info.get("loaded_at"),
            "accuracy": None
        }