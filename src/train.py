"""
Model Training Module

Handles model training, hyperparameter configuration, and model persistence.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from catboost import CatBoostClassifier
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.model_selection import train_test_split, cross_val_score
import joblib
import argparse
import sys
from pathlib import Path
from src.config import Config
from src.utils import load_data, logger, Timer

class ModelTrainer:
    """
    Model training and management class
    
    Supports multiple model types with configurable hyperparameters.
    """

    def __init__(self, config: Config):
        """
        Initialize trainer
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.model = None
        self.model_type = config.train.get('model_type', 'random_forest')
    
    def initialize_model(self):
        """
        Initialize model based on configuration
        
        Returns:
            Initialized sklearn model
        """
        train_config = self.config.train

        logger.info(f"Initializing {self.model_type} model...")

        if self.model_type == 'random_forest':
            self.model = RandomForestClassifier(
                n_estimators=train_config.get('n_estimators', 100),
                max_depth=train_config.get('max_depth', None),
                min_samples_split=train_config.get('min_samples_split', 2),
                min_samples_leaf=train_config.get('min_samples_leaf', 1),
                max_features=train_config.get('max_features', 'sqrt'),
                random_state=train_config.get('random_state', 42),
                # n_jobs=train_config.get('n_jobs', -1),
                class_weight=train_config.get('class_weight', None),
                verbose=train_config.get('verbose', 0)
            )
        elif self.model_type == 'logistic_regression':
            self.model = LogisticRegression(
                max_iter=train_config.get('max_iter', 1000),
                random_state=train_config.get('random_state', 42),
                # n_jobs=train_config.get('n_jobs', -1),
                class_weight=train_config.get('class_weight', None)
            )
        elif self.model_type == 'decision_tree':
            self.model = DecisionTreeClassifier(
                max_depth=train_config.get('max_depth', None),
                min_samples_split=train_config.get('min_samples_split', 2),
                min_samples_leaf=train_config.get('min_samples_leaf', 1),
                random_state=train_config.get('random_state', 42),
                class_weight=train_config.get('class_weight', None)
            )
        elif self.model_type == 'xgboost':
            self.model = XGBClassifier(
                tree_method=train_config.get('tree_method','gpu_hist'),
                predictor=train_config.get('predictor','gpu_predictor'),
                n_estimators=train_config.get('n_estimators',500),
                max_depth=train_config.get('max_depth', 6),
                learning_rate=train_config.get('learning_rate', 0.05)
            )
        elif self.model_type == 'lightgbm':
            self.model = LGBMClassifier(
                device=train_config.get('device', 'gpu'),
                gpu_platform_id=train_config.get('gpu_platform_id', 0),
                gpu_device_id=train_config.get('gpu_device_id', 0),
                n_estimators=train_config.get('n_estimators', 500),
                learning_rate=train_config.get('learning_rate', 0.05),
                num_leaves=train_config.get('num_leaves', 31),
                max_depth=train_config.get('max_depth', -1),
            )
        elif self.model_type == 'catboost':
            self.model = CatBoostClassifier(
                task_type=train_config.get('task_type', 'GPU'),
                devices=train_config.get('devices', "0"),
                iterations=train_config.get('iterations', 500),
                learning_rate=train_config.get('learning_rate', 0.05),
                depth=train_config.get('depth', 6)
            )
        else:
            raise ValueError(f"Unsupported model type: {self.model_type}")
        
        logger.info("  Model parameters:")
        for key, value in self.model.get_params().items():
            if value is not None:
                logger.info(f"    {key}: {value}")
        
        return self.model
    
    def train(self, X_train: np.ndarray, y_train: np.ndarray)-> 'ModelTrainer':
        """
        Train the model
        
        Args:
            X_train: Training features
            y_train: Training labels
            
        Returns:
            Self for method chaining
        """
        if self.model is None:
            self.initialize_model()
        
        logger.info(f"\nTraining {self.model_type}...")
        logger.info(f"  Training samples: {len(X_train):,}")
        logger.info(f"  Features: {X_train.shape[1]}")
        logger.info(f"  Classes: {len(np.unique(y_train))}")

        with Timer('Model training'):
            self.model.fit(X_train, y_train)
        
        logger.info("Training completed!")

        return self
    
    def evaluate_training(self, X_train: np.ndarray, y_train: np.ndarray, X_val: np.ndarray = None, y_val: np.ndarray = None):
        """
        Evaluate model on training and validation sets
        
        Args:
            X_train: Training features
            y_train: Training labels
            X_val: Validation features (optional)
            y_val: Validation labels (optional)
        """
        logger.info("\nTraining Evaluation:")

        train_score = self.model.score(X_train, y_train)
        logger.info(f"  Training accuracy: {train_score:.4f}")

        if X_val is not None and y_val is not None:
            val_score = self.model.score(X_val, y_val)
            logger.info(f"  Validation accuracy: {val_score:.4f}")

            if train_score - val_score > 0.1:
                logger.warning("Possible overfitting detected!")

            logger.info("\nCross-validation (5-fold):")
            cv_scores = cross_val_score(self.model, X_train, y_train, cv=5)
            logger.info(f"  CV Mean: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")

    def get_feature_importance(self, feature_names: list = None) -> pd.DataFrame:
        """
        Get feature importance (for tree-based models)
        
        Args:
            feature_names: List of feature names
            
        Returns:
            DataFrame with feature importance
        """
        if not hasattr(self.model, 'feature_importances_'):
            logger.warning("Model does not support feature importance")
            return None
        
        importance = self.model.feature_importances_

        if feature_names is None:
            feature_names = [f"feature_{i}"for i in range(len(importance))]
        
        df_importance = pd.DataFrame(
            {'feature': feature_names,
            'importance': importance}
        ).sort_values('importance', ascending=False)

        logger.info("\nTop 10 Important Features:")
        for idx, row in df_importance.head(10).iterrows():
            logger.info(f"  {row['feature']:20s}: {row['importance']:.4f}")

        return df_importance
    
    def save_model(self, path: str):
        """
        Save trained model to disk
        
        Args:
            path: Path to save model
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        joblib.dump(self.model, path)
        file_size = path.stat().st_size/1024**2
        logger.info(f"Model saved to {path} ({file_size:.2f} MB)")

    @staticmethod
    def load_model(path: str):
        """
        Load trained model from disk
        
        Args:
            path: Path to model file
            
        Returns:
            Loaded model
        """
        logger.info(f"Loading model from {path}")
        model = joblib.load(path)
        logger.info("Model loaded successfully")
        return model
    
def prepare_data(df: pd.DataFrame, config: Config):
        """
        Prepare data for training
        
        Args:
            df: Processed dataframe
            config: Configuration object
            
        Returns:
            X_train, X_val, X_test, y_train, y_val, y_test
        """
        target = config.preprocess['target']

        exclude_cols = ['customerID', target]
        feature_cols = [col for col in df.columns if col not in exclude_cols]

        X = df[feature_cols].values
        y = df[target].values

        test_size = config.data['test_size']
        val_size = config.data.get('val_size', 0.1)
        random_state = config.data['random_state']

        X_tempt, X_test, y_temp, y_test = train_test_split(
            X, y,
            random_state=random_state,
            test_size=test_size,
            stratify=y
        )

        val_ratio = val_size / (1 - test_size)
        X_train, X_val, y_train, y_val = train_test_split(
            X_tempt, y_temp,
            test_size=val_ratio,
            random_state=random_state,
            stratify=y_temp
        )

        logger.info("\nData Split:")
        logger.info(f"  Training:   {len(X_train):,} samples ({len(X_train)/len(X)*100:.1f}%)")
        logger.info(f"  Validation: {len(X_val):,} samples ({len(X_val)/len(X)*100:.1f}%)")
        logger.info(f"  Test:       {len(X_test):,} samples ({len(X_test)/len(X)*100:.1f}%)")
        logger.info(f"  Features:   {X.shape[1]}")

        train_dist = np.bincount(y_train) /len(y_train)*100
        logger.info("\nClass Distribution (Training):")
        for i,pct in enumerate(train_dist):
            logger.info(f"  Class {i}: {pct:.1f}%")
        
        return X_train, X_val, X_test, y_train, y_val, y_test, feature_cols

def save_preprocessor(preprocessor, model_path: str):
    """
    Save preprocessor alongside model
    
    Args:
        preprocessor: Fitted preprocessor object
        model_path: Path where model is saved
    """
    from src.utils import logger
    
    model_dir = Path(model_path).parent
    preprocessor_path = model_dir / "preprocessor.pkl"
    
    joblib.dump(preprocessor, preprocessor_path)
    logger.info(f"Preprocessor saved to {preprocessor_path}")

def main():
    """
    Main training function
    """
    parser = argparse.ArgumentParser(
        description="Train customer churn prediction model"
    )
    parser.add_argument(
        "--config",
        type=str,
        default='params.yml',
        help='Path to configuration file'
    )

    args = parser.parse_args()

    try:
        logger.info("=" * 60)
        logger.info("MODEL TRAINING")
        logger.info("=" * 60)

        config = Config(args.config)

        processed_path = config.data['processed_path']
        logger.info(f"\nLoading processed data from {processed_path}")
        df = load_data(processed_path)

        X_train, X_val, X_test, y_train, y_val, y_test, feature_names = prepare_data(df, config)

        trainer = ModelTrainer(config)
        trainer.train(X_train, y_train)

        trainer.get_feature_importance(feature_names)

        test_score = trainer.model.score(X_test, y_test)
        logger.info(f"\nFinal Test Accuracy: {test_score:.4f}")

        model_path = config.evaluate['model_path']
        trainer.save_model(model_path)

        logger.info("\n" + "=" * 60)
        logger.info("TRAINING COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error("\n" + "=" * 60)
        logger.error(f"TRAINING FAILED: {str(e)}")
        logger.error("=" * 60)
        import traceback
        traceback.print_exc()
        return 1

if  __name__ == '__main__':
    sys.exit(main())
