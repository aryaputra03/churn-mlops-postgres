"""
Tests for preprocessing module
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.preprocess import DataPreprocessor
from src.config import Config

@pytest.fixture
def sample_config():
    """Fixture for configuration"""
    return Config('params.yml')

@pytest.fixture
def sample_data():
    """Fixture for sample churn data"""
    return pd.DataFrame({
        'customerID': ['C001', 'C002', 'C003', 'C004'],
        'gender': ['Male', 'Female', 'Male', 'Female'],
        'tenure': [10, 20, 30, 5],
        'MonthlyCharges': [50.0, 75.0, 100.0, 40.0],
        'TotalCharges': [500.0, 1500.0, 3000.0, 200.0],
        'Contract': ['Month-to-month', 'One year', 'Two year', 'Month-to-month'],
        'PaymentMethod': ['Electronic check', 'Mailed check', 'Bank transfer', 'Credit card'],
        'InternetService': ['DSL', 'Fiber optic', 'No', 'DSL'],
        'Churn': [1, 0, 0, 1]
    })

def test_preprocessor_initialization(sample_config):
    """Test preprocessor initialization"""
    preprocessor = DataPreprocessor(sample_config)

    assert preprocessor.config is not None
    assert isinstance(preprocessor.label_encoders, dict)
    assert preprocessor.scaler is not None

def test_preprocessor_basic(sample_config, sample_data):
    """Test basic preprocessing pipeline"""
    preprocessor = DataPreprocessor(sample_config)
    df_preprocessed = preprocessor.preprocess(sample_data)

    assert df_preprocessed is not None
    assert len(df_preprocessed) == len(sample_data)
    assert 'Churn' in df_preprocessed.columns

def test_handle_missing_value(sample_config):
    """Test missing value handling"""
    data = pd.DataFrame({
        'TotalCharges': [100.0, None, 300.0, np.nan],
        'tenure': [10, 20, 30, None],
        'gender': ['Male', 'Female', None, 'Male']
    })
    preprocessor = DataPreprocessor(sample_config)
    df_clean = preprocessor.handle_missing_values(data)

    assert df_clean['TotalCharges'].isna().sum() == 0
    assert df_clean['tenure'].isna().sum() == 0


def test_encode_categorical(sample_config, sample_data):
    """Test categorical encoding"""
    preprocessor = DataPreprocessor(sample_config)
    df_encoded = preprocessor.encode_categorical(sample_data.copy(), 'gender')

    assert df_encoded['gender'].dtype in [np.int32, np.int64]
    assert 'gender' in preprocessor.label_encoders
    assert len(preprocessor.label_encoders['gender'].classes_) == 2

def test_scale_numerical(sample_config, sample_data):
    """Test numerical feature scaling"""
    preprocessor = DataPreprocessor(sample_config)

    numerical_feature = ['tenure','MonthlyCharges']
    df_scaled = preprocessor.scale_numerical(sample_data.copy(), numerical_feature)

    for col in numerical_feature:
        assert abs(df_scaled[col].mean()) < 1.0
        assert abs(df_scaled[col].std() - 1.0) < 1.0

def test_feature_engineering(sample_config, sample_data):
    """Test feature engineering"""
    preprocessor = DataPreprocessor(sample_config)
    df_eng = preprocessor.feature_engineering(sample_data.copy())

    if sample_config.preprocess.get('feature_engineering', {}).get('create_tenure_bins'):
        assert 'tenure_group' in df_eng.columns
    
    if sample_config.preprocess.get('feature_engineering', {}).get('create_charge_ratio'):
        assert 'charge_ratio' in df_eng.columns

def test_encode_target(sample_config, sample_data):
    """Test target encoding"""
    preprocessor = DataPreprocessor(sample_config)

    target_int = pd.Series([0, 1, 0, 1])
    encoded_int = preprocessor.encode_target(target_int)
    assert np.array_equal(encoded_int, target_int)

    target_str = pd.Series(['No', 'Yes', 'No', 'Yes'])
    encoded_int = preprocessor.encode_target(target_str)
    assert encoded_int.dtype in [np.int32, np.int64]
    assert 'target' in preprocessor.label_encoders

def test_preprocess_preserves_samples(sample_config, sample_data):
    """Test that preprocessing preserves number of samples"""
    preprocessor = DataPreprocessor(sample_config)
    df_processed = preprocessor.preprocess(sample_data)

    assert len(df_processed) == len(sample_data)

def test_preprocess_with_missing_data(sample_config):
    """Test preprocessing with missing values"""
    data = pd.DataFrame({
        'customerID': ['C001', 'C002', 'C003'],
        'gender': ['Male', None, 'Female'],
        'tenure': [10, 20, None],
        'MonthlyCharges': [50.0, None, 100.0],
        'TotalCharges': [500.0, 1500.0, 3000.0],
        'Contract': ['Month-to-month', 'One year', 'Two year'],
        'PaymentMethod': ['Electronic check', 'Mailed check', None],
        'InternetService': ['DSL', 'Fiber optic', 'No'],
        'Churn': [1, 0, 0]
    })
    preprocessor = DataPreprocessor(sample_config)
    df_preprocessed = preprocessor.preprocess(data)

    assert df_preprocessed is not None
    assert len(df_preprocessed) > 0



