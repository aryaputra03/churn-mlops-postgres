"""
Model Evaluation Module

Evaluates trained model performance with comprehensive metrics.
"""
import numpy as np
import json
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report
)
import argparse
import sys
from pathlib import Path
from src.config import Config
from src.utils import load_data, save_metrics, print_metrics, logger, Timer
from src.train import ModelTrainer, prepare_data

def calculate_metrics(
        y_true: np.ndarray,
        y_pred: np.ndarray,
        y_pred_prob: np.ndarray = None
) -> dict:
    """
    Calculate comprehensive evaluation metrics
    
    Args:
        y_true: True labels
        y_pred: Predicted labels
        y_pred_prob: Predicted probabilities
        
    Returns:
        Dictionary of metrics
    """
    metrics = {
        'accuracy' : float(accuracy_score(y_true, y_pred)),
        'precision' : float(precision_score(y_true, y_pred, average='weighted', zero_division=0)),
        'recall' : float(recall_score(y_true, y_pred, average='weighted', zero_division=0)),
        'f1_score' : float(f1_score(y_true, y_pred, average='weighted', zero_division=0))
    }

    if y_pred_prob is not None and len(np.unique(y_true)) == 2:
        try:
            metrics['roc_auc'] = float(roc_auc_score(y_true, y_pred_prob[:,1]))
        except Exception as e:
            logger.warning(f"Could not calculate ROC AUC {e}")
    
    cm = confusion_matrix(y_true, y_pred)
    metrics['confusion_matrix'] = cm.tolist()
    
    report = classification_report(y_true, y_pred, output_dict=True, zero_division=0)
    metrics['pre_class'] = report

    return metrics

def save_confusion_matrix_plot(cm: np.ndarray, output_path: str):
    """
    Save confusion matrix as JSON for DVC plots
    
    Args:
        cm: Confusion matrix
        output_path: Path to save plot data
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plot_data = []
    labels = ['Not Churned', 'Churned']

    for i, actual in enumerate(labels):
        for j, predicted in enumerate(labels):
            plot_data.append({
                'actual': actual,
                'predicted': predicted,
                'count' : int(cm[i, j])
            })

    with open(output_path, 'w') as f:
        json.dump(plot_data,f, indent=2)

    logger.info(f"Confusion matrix plot saved to {output_path}")

def check_thresholds(metrics: dict, config: Config) -> bool:
    """
    Check if metrics meet minimum thresholds
    
    Args:
        metrics: Dictionary of metrics
        config: Configuration object
        
    Returns:
        True if all thresholds met
    """
    eval_config = config.evaluate
    
    min_accuracy = eval_config.get('min_accuracy', 0.0)
    min_f1 = eval_config.get('min_f1_score', 0.0)

    accuracy = metrics['accuracy']
    f1_score = metrics['f1_score']

    logger.info("Threshold Check")

    passed = True

    if accuracy < min_accuracy:
        logger.warning(f"Accuracy {accuracy:.4f} < threshold {min_accuracy:.4f}")
        passed = False
    else:
        logger.info(f"Accuracy {accuracy:.4f} >= threshold {min_accuracy:.4f}")
    
    if f1_score< min_f1:
        logger.warning(f"f1 score {f1_score:.4f} < threshold {min_f1:.4f}")
        passed = False
    else:
        logger.info(f"f1 score {f1_score:.4f} >= threshold {min_f1:.4f}")

    return passed

def main():
    """
    Main evaluation function
    """
    parser = argparse.ArgumentParser(
        description='Evaluate Customer Churn Prediction Model'
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
        logger.info("MODEL EVALUATION")
        logger.info("=" * 60)

        config = Config(args.config)

        processed_path = config.data['processed_path']
        logger.info(f"\nLoading data from {processed_path}")
        df = load_data(processed_path)

        X_train, X_val, X_test, y_train, y_val, y_test, feature_names = prepare_data(df, config)

        model_path = config.evaluate['model_path']
        logger.info(f"\nLoading model from {model_path}")
        model = ModelTrainer.load_model(model_path)

        logger.info("\nMaking predictions...")
        with Timer('Prediction'):
            y_pred = model.predict(X_test)
            y_pred_prob = model.predict_proba(X_test)
        
        logger.info("\nCalculating metrics...")
        metrics = calculate_metrics(y_test, y_pred, y_pred_prob)

        print_metrics(metrics, "Model Evaluation Metrics")

        cm = np.array(metrics['confusion_matrix'])
        logger.info("\nConfusion Matrix:")
        logger.info("                  Predicted")
        logger.info("              Not Churn  Churn")
        logger.info(f"Actual Not Churn  {cm[0,0]:6d}  {cm[0,1]:6d}")
        logger.info(f"       Churn      {cm[1,0]:6d}  {cm[1,1]:6d}")

        metrics_path = config.evaluate['metrics_path']
        save_metrics(metrics, metrics_path)

        if config.evaluate.get("save_confusion_matrix", False):
            cm_plot_path = "plots/confusion_matrix.json"
            save_confusion_matrix_plot(cm, cm_plot_path)

        threshold_met = check_thresholds(metrics, config)

        if not threshold_met:
            logger.warning("\nWarning: Model does not meet minimum thresholds!")

        logger.info("\n" + "=" * 60)
        logger.info("EVALUATION COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)

        return 0 if threshold_met else 2
    
    except Exception as e:
        logger.error("\n" + "=" * 60)
        logger.error(f"EVALUATION FAILED: {str(e)}")
        logger.error("=" * 60)
        import traceback
        traceback.print_exc()
        return 1
    
if __name__ == '__main__':
    sys.exit(main())