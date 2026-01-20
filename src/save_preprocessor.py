"""
Save Preprocessor After Training

Run this after training to save the fitted preprocessor that matches
the preprocessing done during training.
"""

import joblib
import pandas as pd
from pathlib import Path
from sklearn.preprocessing import LabelEncoder, StandardScaler
from src.config import Config
from src.utils import logger, load_data

def create_and_save_preprocessor():
    """Create and save preprocessor with fitted encoders and scaler from training data"""
    
    config = Config("params.yml")
    model_path = Path(config.evaluate['model_path'])
    preprocessor_path = model_path.parent / "preprocessor.pkl"

    processed_path = config.data['processed_path']
    
    if Path(processed_path).exists():
        logger.info(f"Loading processed data from {processed_path}")
        df = load_data(processed_path)

        numerical_cols = config.preprocess.get('numerical_features', ['tenure', 'MonthlyCharges', 'TotalCharges'])

        if 'charge_ratio' in df.columns:
            numerical_cols.append('charge_ratio')

        scale_method = config.preprocess.get('scale_method', 'standard')
        if scale_method == 'standard':
            scaler = StandardScaler()
        else:
            from sklearn.preprocessing import MinMaxScaler
            scaler = MinMaxScaler()

        existing_numerical = [col for col in numerical_cols if col in df.columns]
        if existing_numerical:
            scaler.fit(df[existing_numerical])
            logger.info(f"Scaler fitted on columns: {existing_numerical}")
            logger.info(f"Scaler mean: {scaler.mean_}")
            logger.info(f"Scaler scale: {scaler.scale_}")
        else:
            logger.warning("No numerical columns found, using representative data")
            representative_data = pd.DataFrame({
                'tenure': [0, 72, 36],
                'MonthlyCharges': [18.25, 118.75, 65.0],
                'TotalCharges': [18.25, 8564.75, 2500.0]
            })
            scaler.fit(representative_data[existing_numerical] if existing_numerical else representative_data)
    else:
        logger.warning(f"Processed data not found at {processed_path}, using default values")
        scale_method = config.preprocess.get('scale_method', 'standard')
        if scale_method == 'standard':
            scaler = StandardScaler()
        else:
            from sklearn.preprocessing import MinMaxScaler
            scaler = MinMaxScaler()
        
        representative_data = pd.DataFrame({
            'tenure': [0, 72, 36],
            'MonthlyCharges': [18.25, 118.75, 65.0],
            'TotalCharges': [18.25, 8564.75, 2500.0],
            'charge_ratio': [0.5, 100, 40]
        })
        scaler.fit(representative_data)

    label_encoders = {
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
    
    logger.info("Label encoders created:")
    for key, encoder in label_encoders.items():
        logger.info(f"  {key}: {encoder.classes_}")
    
    preprocessor = {
        'label_encoders': label_encoders,
        'scaler': scaler
    }
    
    preprocessor_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(preprocessor, preprocessor_path)
    logger.info(f"Preprocessor saved to {preprocessor_path}")
    
    return preprocessor_path

if __name__ == '__main__':
    create_and_save_preprocessor()