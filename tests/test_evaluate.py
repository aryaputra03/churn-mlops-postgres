"""
Tests for evaluation module
"""

import pytest
import numpy as np
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.evaluate import calculate_metrics, check_thresholds, save_confusion_matrix_plot
from src.config import Config

@pytest.fixture
def sample_config():
    """Fixture for configuration"""
    return Config('params.yml')

def test_calculate_metrics_perfect():
    """Test metrics with perfect predictions"""
    y_true = np.array([0, 1, 0, 1, 0, 1])
    y_pred = np.array([0, 1, 0, 1, 0, 1])

    metrics = calculate_metrics(y_true, y_pred)

    assert metrics['accuracy'] == 1.0
    assert metrics['precision'] == 1.0
    assert metrics['recall'] == 1.0
    assert metrics['f1_score'] == 1.0
    assert 'confusion_matrix' in metrics

def test_calculate_metrics_with_error():
    """Test metrics with some errors"""
    y_true = np.array([0, 1, 0, 1, 0, 1, 0, 1])
    y_pred = np.array([0, 1, 0, 0, 0, 1, 1, 1])

    metrics = calculate_metrics(y_true, y_pred)

    assert 0 <= metrics['accuracy'] <= 1
    assert metrics['accuracy'] < 1.0
    assert 0 <= metrics['precision'] <= 1
    assert 0 <= metrics['recall'] <= 1
    assert 0 <= metrics['f1_score'] <= 1

def test_calculate_metrics_with_probability():
    """Test metrics calculation with probabilities"""
    y_true = np.array([0, 1, 0, 1])
    y_pred = np.array([0, 1, 0, 1])
    y_pred_proba = np.array([
        [0.9, 0.1],
        [0.2, 0.8],
        [0.85, 0.15],
        [0.1, 0.9]
    ])

    metrics = calculate_metrics(y_true, y_pred, y_pred_proba)

    assert 'roc_auc' in metrics
    assert 0 <= metrics['roc_auc'] <= 1

def test_metrics_contain_required_field():
    """Test that all required metrics are present"""
    y_true = np.array([0, 1, 0, 1])
    y_pred = np.array([0, 1, 1, 1])

    metrics = calculate_metrics(y_true, y_pred)

    required_metrics = ['accuracy', 'precision', 'recall', 'f1_score', 'confusion_matrix', 'pre_class']
    for metric in required_metrics:
        assert metric in metrics

def test_confusion_metrics_shape():
    """Test confusion matrix has correct shape"""
    y_true = np.array([0, 1, 0, 1, 0, 1])
    y_pred = np.array([0, 1, 1, 1, 0, 0])

    metrics = calculate_metrics(y_true, y_pred)

    cm = metrics["confusion_matrix"]
    assert len(cm) == 2
    assert len(cm[0]) == 2

def test_confusion_matrix_values():
    """Test confusion matrix values are correct"""
    y_true = np.array([0, 0, 1, 1])
    y_pred = np.array([1, 0, 1, 0])

    metrics = calculate_metrics(y_true, y_pred)
    cm = np.array(metrics["confusion_matrix"])

    assert cm[0,0] == 1
    assert cm[1,0] == 1
    assert cm[0,1] == 1
    assert cm[1,1] == 1

def test_check_thresholds_pass(sample_config):
    """Test threshold checking when thresholds are met"""
    metrics = {
        'accuracy': 0.85,
        'f1_score': 0.80
    }
    sample_config.evaluate['min_accuracy'] = 0.75
    sample_config.evaluate['min_f1_score'] = 0.70

    result = check_thresholds(metrics, sample_config)

    assert result is True

def test_check_thresholds_fail(sample_config):
    """Test threshold checking when thresholds are not met"""
    metrics = {
        'accuracy': 0.55,
        'f1_score': 0.50
    }
    sample_config.evaluate['min_accuracy'] = 0.75
    sample_config.evaluate['min_f1_score'] = 0.70

    result = check_thresholds(metrics, sample_config)

    assert result is False

def test_save_confusion_matrix_plot(tmp_path):
    """Test saving confusion matrix plot"""
    cm = np.array([[50,10], [5,35]])
    output_path = tmp_path/'cm_plot.json'

    save_confusion_matrix_plot(cm, str(output_path))

    assert output_path.exists()

    import json
    with open(output_path) as f:
        plot_data = json.load(f)
    
    assert len(plot_data) == 4
    assert all('actual' in entry for entry in plot_data)
    assert all('predicted' in entry for entry in plot_data)
    assert all('count' in entry for entry in plot_data)

def test_metrics_with_single_class():
    """Test metrics when only one class is present"""
    y_true = np.array([0, 0, 0, 0])
    y_pred = np.array([0, 0, 0, 0])

    metrics = calculate_metrics(y_true, y_pred)

    assert metrics is not None
    assert 'accuracy' in metrics

def test_metric_with_all_wrong():
    """Test metrics with completely wrong predictions"""
    y_true = np.array([0,0,0,0])
    y_pred = np.array([1,1,1,1])

    metrics = calculate_metrics(y_true, y_pred)

    assert metrics['accuracy'] == 0.0
    assert 0 <= metrics['precision'] <= 1
    assert 0 <= metrics['recall'] <= 1