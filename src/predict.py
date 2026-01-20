"""
Prediction/Inference Module

Makes predictions on new data using trained model.
"""

import pandas as pd
import numpy as np
import argparse
import sys
from pathlib import Path
from src.config import Config
from src.utils import load_data, Timer, logger
from src.train import ModelTrainer

def predict_batch(
        model,
        input_data: pd.DataFrame,
        batch_size: int = 1000,
        output_probabilities: bool = True
) -> pd.DataFrame:
    """
    Make predictions in batches
    
    Args:
        model: Trained model
        input_data: Input features
        batch_size: Batch size for prediction
        output_probabilities: Whether to output probabilities
        
    Returns:
        DataFrame with predictions
    """
    n_samples = len(input_data)
    n_batches = (n_samples + batch_size -1) //  batch_size

    logger.info(f"Making predictions ({n_samples:,} samples, {n_batches} batches)...")
    
    all_prediction = []
    all_probabilities = []

    for i in range(n_batches):
        start_idx = i * batch_size
        end_idx = min((i+1)*batch_size, n_samples)

        batch = input_data.iloc[start_idx:end_idx]

        batch_pred = model.predict(batch)
        all_prediction.extend(batch_pred)

        if output_probabilities:
            batch_proba = model.predict_proba(batch)
            all_probabilities.extend(batch_proba)

        if (i + 1) % 10 == 0 or (i + 1) == n_batches:
            logger.info(f"Processed {end_idx:,} / {n_samples:,} samples")
    
    results = pd.DataFrame({
        'prediction' : all_prediction
    }
    )
    if output_probabilities:
        proba_array = np.array(all_probabilities)
        for i in range(proba_array.shape[1]):
            results[f'probability_class_{i}'] = proba_array[:,i]

    return results

def main():
    """
    Main prediction function
    """
    parser = argparse.ArgumentParser(
        description="Make prediction on new data"
    )
    parser.add_argument(
        "--config",
        type=str,
        default='params.yml',
        help="Path to configuration file"
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to input data CSV"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="prediction.csv",
        help="Path to save predictions"
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="Override model path from config"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for predictions"
    )

    args = parser.parse_args()

    try:
        logger.info("=" * 60)
        logger.info("PREDICTION")
        logger.info("=" * 60)

        config = Config(args.config)

        logger.info(f"\nLoading input data from {args.input}")
        input_data = load_data(args.input)

        target_col = config.preprocess.get('target', 'Churn')
        if target_col in input_data.columns:
            logger.info(f"Removing target column: {target_col}")
            input_data = input_data.drop(columns=[target_col])
        
        if 'customerID' in input_data.columns:
            customer_ids = input_data['customerID']
            input_data = input_data.drop(columns=['customerID'])
        else:
            customer_ids = None
        
        model_path = args.model or config.evaluate['model_path']
        logger.info(f"\nLoading model from {model_path}")
        model = ModelTrainer.load_model(model_path)

        batch_size = args.batch_size or config.predict.get('batch_size', 1000)
        output_proba = config.predict.get('output_probabilities', True)

        with Timer("Prediction"):
            predictions = predict_batch(
                model,
                input_data,
                batch_size=batch_size,
                output_probabilities=output_proba
            )
        
        if customer_ids is not None:
            predictions.insert(0, 'customerID', customer_ids.values)
        
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        predictions.to_csv(output_path, index=False)
        logger.info(f"\nPredictions saved to {output_path}")

        logger.info("\nSample Predictions (first 10 rows):")
        print(predictions.head(10).to_string(index=False))

        pred_dist = predictions['prediction'].value_counts()
        logger.info("\nPrediction Distribution:")
        for class_label, count in pred_dist.items():
            pct = count / len(predictions)*100
            logger.info(f"Class {class_label}: {count:,} ({pct:.1f}%)")
        
        logger.info("\n" + "=" * 60)
        logger.info("PREDICTION COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)

        return 0
    
    except Exception as e:
        logger.error("\n" + "=" * 60)
        logger.error(f"PREDICTION FAILED: {str(e)}")
        logger.error("=" * 60)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())