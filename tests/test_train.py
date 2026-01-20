"""
Tests for training module
"""

import pytest
import numpy as np
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from sklearn.datasets import make_classification
from src.train import ModelTrainer, prepare_data
from src.config import Config
import pandas as pd

@pytest.fixture
def sample_config():
    """Fixture for configuration"""
    return Config('params.yml')

@pytest.fixture
def sample_training_data():
    """Fixture for training data"""
    X, y = make_classification(
        n_samples=200,
        n_features=10,
        n_informative=7,
        n_redundant=2,
        n_classes=2,
        random_state=42
    )
    return X, y

@pytest.fixture
def sample_dataframe():
    """Fixture for sample processed dataframe"""
    np.random.seed(42)
    n_samples = 100

    return pd.DataFrame({
        'customerID': [f'C{i:03d}' for i in range(n_samples)],
        'feature1': np.random.randn(n_samples),
        'feature2': np.random.randn(n_samples),
        'feature3': np.random.randint(0, 3, n_samples),
        'feature4': np.random.rand(n_samples),
        'Churn': np.random.randint(0, 2, n_samples)
    })

def test_trainer_initialization(sample_config):
    """Test trainer initialization"""
    trainer = ModelTrainer(sample_config)

    assert trainer.config is not None
    assert trainer.model is None
    assert trainer.model_type == sample_config.train['model_type']

def test_initialization_random_forest(sample_config):
    """Test random forest initialization"""
    sample_config.train['model_type'] = 'random_forest'
    trainer = ModelTrainer(sample_config)
    model = trainer.initialize_model()

    assert model is not None
    assert hasattr(model, 'fit')
    assert hasattr(model, 'predict')

def test_initialization_logistic_regression(sample_config):
    """Test logistic regression initialization"""
    sample_config.train['model_type'] = 'logistic_regression'
    trainer = ModelTrainer(sample_config)
    model = trainer.initialize_model()

    assert model is not None
    assert hasattr(model, 'fit')

def test_train_model(sample_config, sample_training_data):
    """Test model training"""
    X,y = sample_training_data
    
    trainer = ModelTrainer(sample_config)
    trainer.train(X,y)

    assert trainer.model is not None
    assert hasattr(trainer.model, 'predict')

def test_model_prediction(sample_config, sample_training_data):
    """Test model predictions"""
    X, y = sample_training_data

    trainer = ModelTrainer(sample_config)
    trainer.train(X,y)

    predictions = trainer.model.predict(X)

    assert len(predictions) == len(y)
    assert set(predictions).issubset({0,1})

def test_model_accuracy(sample_config, sample_training_data):
    """Test model achieves reasonable accuracy"""
    X, y = sample_training_data

    trainer = ModelTrainer(sample_config)
    trainer.train(X,y)

    score = trainer.model.score(X,y)

    assert score > 0.65, "Model should achieve at least 65% accuracy on training data"

def test_feature_importances(sample_config, sample_training_data):
    """Test feature importance extraction"""
    X, y = sample_training_data

    sample_config.train['model_type'] = 'random_forest'
    trainer = ModelTrainer(sample_config)
    trainer.train(X,y)

    feature_names = [f'feature_{i}' for i in range(X.shape[1])]
    importance_df = trainer.get_feature_importance(feature_names)
    
    assert importance_df is not None
    assert len(importance_df) == X.shape[1]
    assert 'feature' in importance_df.columns
    assert 'importance' in importance_df.columns

def test_save_and_load_model(sample_config, sample_training_data, tmp_path):
    """Test model saving and loading"""
    X, y = sample_training_data
    trainer = ModelTrainer(sample_config)
    trainer.train(X,y)

    model_path = tmp_path / "test_model.pkl"
    trainer.save_model(str(model_path))

    assert model_path.exists()

    loaded_model = ModelTrainer.load_model(str(model_path))

    prediction_original = trainer.model.predict(X)
    prediction_loaded = loaded_model.predict(X)

    np.testing.assert_array_equal(prediction_original, prediction_loaded)

def test_prepare_data(sample_config, sample_dataframe):
    """Test data preparation"""
    X_train, X_val, X_test, y_train, y_val, y_test, feature_names = prepare_data(
        sample_dataframe,
        sample_config
    )

    total_sample = len(sample_dataframe)
    assert len(X_train) + len(X_val) + len(X_test) == total_sample
    assert len(y_train) == len(X_train)
    assert len(y_val) == len(X_val)
    assert len(y_test) == len(X_test)

def test_model_training_with_class_weight(sample_config, sample_training_data):
    """Test training with class weights"""
    X, y = sample_training_data

    sample_config.train['class_weight'] = 'balanced'
    trainer = ModelTrainer(sample_config)
    trainer.train(X,y)

    assert trainer.model is not None

    prediction = trainer.model.predict(X)
    assert len(prediction) == len(y)

def test_invalid_model_test(sample_config):
    """Test error handling for invalid model type"""
    sample_config.train['model_type'] = 'invalid_model'
    trainer = ModelTrainer(sample_config)

    with pytest.raises(ValueError):
        trainer.initialize_model()








