```markdown
# Data Directory

This directory contains the datasets for the churn classification project.

## Structure

```
data/
├── raw/          # Raw, unprocessed data
└── processed/    # Processed, cleaned data ready for training
```

## Data Files

### Raw Data
- `churn_data.csv`: Original customer churn dataset
  - Generated synthetically for demo purposes
  - Contains customer demographics, service info, and churn labels

### Processed Data
- `churn_processed.csv`: Preprocessed data after cleaning and feature engineering
  - Missing values handled
  - Categorical features encoded
  - Numerical features scaled
  - Ready for model training

## Data Version Control

All data files are tracked using DVC (Data Version Control):
- `.dvc` files are committed to git
- Actual data files are stored in remote storage
- Use `dvc pull` to download data
- Use `dvc push` to upload data

## Generate Sample Data

To generate sample data for testing:

```bash
python -c "from src.utils import generate_sample_data, setup_directories; setup_directories(); generate_sample_data('data/raw/churn_data.csv', n_samples=1000)"
```

## Privacy & Security

⚠️ **Important**: Never commit actual customer data to git
- Always use DVC for data versioning
- Keep sensitive data in secure remote storage
- Use `.gitignore` to prevent accidental commits
```

---