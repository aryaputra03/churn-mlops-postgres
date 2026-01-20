"""
Preprocessing pipeline runner.

- loads raw data
- runs DataPreprocessor (src.preprocess.DataPreprocessor)
- saves processed dataframe
- stores preprocessor artefacts (label encoders, scaler, feature_names) to models/preprocessor.joblib
"""

from typing import Optional
from src.config import Config
from src.preprocess import DataPreprocessor
from src.utils import load_data, save_data, logger, Timer

def run_preprocess(
        config: Config, 
        input_path: Optional[str] = None, 
        output_path: Optional[str] = None
        ) -> str:
    """Run full preprocessing and save processed dataframe.

    Args:
        config: Config object
        input_path: optional override for raw input path
        output_path: optional override for processed output path

    Returns:
        path to processed data (string)
    """

    raw_path = input_path or config.data.get('raw_path')
    processed_path = output_path or config.data.get('processed_path')

    if raw_path is None or processed_path is None:
        raise ValueError("raw_path and processed_path must be set in config or provided as parameters")
    
    logger.info(f"Preprocessing: raw={raw_path} -> processed={processed_path}")

    df = load_data(raw_path)

    processor = DataPreprocessor(config)
    with Timer('Preprocessing pipeline'):
        df_processed = processor.preprocess(df)

    save_data(df_processed, processed_path)
    logger.info("Preprocessing finished and saved")
    return processed_path

if __name__ == "__main__":
    import sys
    cfg = "params.yml"
    inp = None
    out = None
    try:
        cfg = sys.argv[1] if len(sys.argv) > 1 else cfg
        run_preprocess(cfg, input_path=inp, output_path=out)
    except Exception as e:
        logger.exception("Preprocess pipeline failed: %s", e)
        raise