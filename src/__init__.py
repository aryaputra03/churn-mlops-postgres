"""
Churn Classification MLOps Package

A complete machine learning operations project for customer churn prediction
using Docker, DVC, and CI/CD automation.
"""

__version__ = "0.1.0"
__author__ = "Stavanger"
__email__ = "Aryaganendra45@gmail.com"

__all__ = [
    'config',
    'evaluate',
    'predict',
    'preprocess',
    'train',
    'utils'
]

def get_version():
    "Get Package Version"
    return __version__

def get_info():
    "Get package information"
    return {
        'name' : 'churn-classification-mlops',
        'version' : __version__,
        'author' : __author__,
        'email' : __email__,
        'description': 'MLOps project for customer churn prediction'
    }