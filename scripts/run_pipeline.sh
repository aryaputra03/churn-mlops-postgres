set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_step(){
    echo -e "${BLUE}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_error(){
    echo -e "${RED}Error: $1${NC}"
}

print_success(){
    echo -e "${GREEN}$1${NC}"
}

print_warning(){
    echo -e "${YELLOW}$1${NC}"
}

echo ""
print_step "Starting ML Pipeline"
echo ""

print_step "Step 1: Setup Directories"

python -c "from src.utils import setup_directories; setup_directories()"
if [ $? -eq 0 ]; then
    print_success "Directories created"
else
    print_error "Failed to create directories"
    exit 1
fi
echo ""

if [ ! -f "data/raw/churn_data.csv" ]; then
    print_step "Step 2: Generate Sample Data"
    python -c "from src.utils import generate_sample_data; generate_sample_data('data/raw/churn_data.csv', n_samples=1000)"
    if [ $? -eq 0 ]; then
        print_success "Sample data generated"
    else
        print_error "Failed to generate sample data"
        exit 1
    fi
else
    print_warning "Step 2: Sample data already exists, skipping generation"
fi
echo ""

print_step "Step 3: Data Preprocessing"
python -m src.preprocess --config params.yml
if [ $? -eq 0 ]; then
    print_success "Preprocessing completed"
else
    print_error "Preprocessing failed"
    exit 1
fi
echo ""

print_step "Step 4: Model Training"
python -m src.train --config params.yml
if [ $? -eq 0 ]; then
    print_success "Training completed"
else
    print_error "Training failed"
    exit 1
fi
echo ""

print_step "Step 5: Model Evaluation"
python -m src.evaluate --config params.yml
if [ $? -eq 0 ]; then
    print_success "Evaluation completed"
else
    print_error "Evaluation failed"
    exit 1
fi
echo ""

if [ -f "metrics/metrics.json" ]; then
    print_step "Final Metrics"
    cat metrics/metrics.json | python -m json.tool
    echo ""
fi

print_step "Pipeline Completed Successfully!"
echo ""
echo "Generated files:"
echo "  - data/processed/churn_processed.csv"
echo "  - models/churn_model.pkl"
echo "  - metrics/metrics.json"

if [ -f "plots/confusion_matrix.json" ]; then
    echo "  - plots/confusion_matrix.json"
fi
echo ""
echo "Next steps:"
echo "  1. Review metrics/metrics.json"
echo "  2. Make predictions: python -m src.predict --input new_data.csv"
echo "  3. Retrain with different parameters in params.yml"
echo ""
