"""
Training pipeline that constructs a sklearn Pipeline (preprocessing + estimator),
trains, evaluates and persists the model.

Main entrypoint: run_training(config_path: str = "params.yml", override_model_output: Optional[str] = None)
"""
from typing import Optional
from pathlib import Path
import joblib
import pandas as pd

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split, cross_val_score

from src.config import Config
from src.preprocess import DataPreprocessor
from src.train import ModelTrainer
from src.utils import load_data, logger, Timer


class PreprocessorWrapper(BaseEstimator, TransformerMixin):
    """
    sklearn-compatible wrapper around your DataPreprocessor.
    This wrapper expects raw pandas.DataFrame on fit/transform and returns numeric matrix.
    """
    def __init__(self, config: Config, fitted_preprocessor: Optional[DataPreprocessor] = None):
        self.config = config
        self.preprocessor = fitted_preprocessor
        self.feature_names_ = None

    def fit(self, X: pd.DataFrame, y=None):
        if self.preprocessor is None:
            self.preprocessor = DataPreprocessor(self.config)
            processed = self.preprocessor.preprocess(X.copy())
        else:
            processed = self.preprocessor.preprocess(X.copy())

        target = self.config.preprocess.get('target')
        if target and target in processed.columns:
            feature_df = processed.drop(columns=[target], errors='ignore')
        else:
            feature_df = processed

        self.feature_names_ = feature_df.columns.tolist()
        return self

    def transform(self, X: pd.DataFrame):
        if self.preprocessor is None:
            raise RuntimeError("PreprocessorWrapper is not fitted.")
        processed = self.preprocessor.preprocess(X.copy())
        target = self.config.preprocess.get('target')
        if target and target in processed.columns:
            processed = processed.drop(columns=[target], errors='ignore')
        return processed.values

    def fit_transform(self, X: pd.DataFrame, y=None, **fit_params):
        self.fit(X, y)
        return self.transform(X)


def run_training(config_path: str = "params.yml", override_model_output: Optional[str] = None) -> int:
    """
    Execute the training pipeline:
      - load raw data
      - fit DataPreprocessor (to obtain encoded target)
      - build sklearn Pipeline(preprocess_wrapper, estimator)
      - train, evaluate, and save pipeline
    """
    try:
        logger.info("=" * 60)
        logger.info("TRAINING PIPELINE")
        logger.info("=" * 60)

        config = Config(config_path)
        config.validate()

        raw_path = config.data.get('raw_path')
        if raw_path is None:
            raise ValueError("raw_path not set in config.data")

        logger.info(f"Loading raw data from: {raw_path}")
        df_raw = load_data(raw_path)

        logger.info("Fitting DataPreprocessor to raw data (to obtain encoded target)")
        preprocessor = DataPreprocessor(config)
        df_processed = preprocessor.preprocess(df_raw.copy())

        target_col = config.preprocess.get('target')
        if target_col is None or target_col not in df_processed.columns:
            raise ValueError("Target column missing after preprocessing")

        y = df_processed[target_col].values  # encoded target

        pre_wrapper = PreprocessorWrapper(config=config, fitted_preprocessor=preprocessor)

        trainer = ModelTrainer(config)
        trainer.initialize_model()
        estimator = trainer.model

        pipeline = Pipeline(steps=[
            ("preprocess", pre_wrapper),
            ("model", estimator)
        ])

        test_size = config.data.get('test_size', 0.2)
        val_size = config.data.get('val_size', 0.0)
        random_state = config.data.get('random_state', 42)

        logger.info("Splitting data for train/test (stratified on encoded target)")
        X_train_raw, X_test_raw, y_train, y_test = train_test_split(
            df_raw, y, test_size=test_size, random_state=random_state, stratify=y
        )

        X_val_raw = None
        y_val = None
        if val_size and val_size > 0:
            val_ratio = val_size / (1 - test_size)
            X_train_raw, X_val_raw, y_train, y_val = train_test_split(
                X_train_raw, y_train, test_size=val_ratio, random_state=random_state, stratify=y_train
            )

        logger.info("Fitting pipeline on raw training data")
        with Timer("Pipeline fit"):
            pipeline.fit(X_train_raw, y_train)

        logger.info("Evaluating model")
        train_score = pipeline.score(X_train_raw, y_train)
        logger.info(f"  Training score: {train_score:.4f}")

        if X_val_raw is not None:
            val_score = pipeline.score(X_val_raw, y_val)
            logger.info(f"  Validation score: {val_score:.4f}")

        test_score = pipeline.score(X_test_raw, y_test)
        logger.info(f"  Test score: {test_score:.4f}")

        cv = config.train.get('cv', 0)
        if cv and cv > 1:
            try:
                logger.info(f"Running cross-validation (cv={cv})")
                cv_scores = cross_val_score(pipeline, df_raw, y, cv=cv)
                logger.info(f"  CV Mean: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
            except Exception as e:
                logger.warning(f"Cross-validation failed: {e}")

        model_output = override_model_output or config.evaluate.get('model_path')
        if model_output is None:
            raise ValueError("evaluate.model_path must be set in config or provided as override")

        model_output_path = Path(model_output)
        model_output_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(pipeline, model_output_path)
        logger.info(f"Pipeline saved to: {model_output_path}")

        try:
            feature_names = pre_wrapper.feature_names_
            if feature_names:
                features_path = model_output_path.with_suffix(".features.txt")
                with open(features_path, "w", encoding="utf8") as f:
                    for feat in feature_names:
                        f.write(f"{feat}\n")
                logger.info(f"Feature names saved to: {features_path}")
        except Exception:
            logger.debug("Could not save features list")

        logger.info("\nTRAINING PIPELINE COMPLETED SUCCESSFULLY")
        return 0

    except Exception as e:
        logger.exception("TRAINING PIPELINE FAILED: %s", e)
        return 1


if __name__ == "__main__":
    import sys
    cfg = "params.yml"
    out = None
    cfg = sys.argv[1] if len(sys.argv) > 1 else cfg
    out = sys.argv[2] if len(sys.argv) > 2 else out
    raise SystemExit(run_training(cfg, override_model_output=out))
