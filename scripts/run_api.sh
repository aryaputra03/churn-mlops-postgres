# ============================================
# Run FastAPI Server
# ============================================

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================"
echo "Starting FastAPI Server"
echo -e "========================================${NC}"

if [ ! -f "models/churn_model.pkl" ]; then
    echo -e "${GREEN}Model not found. Running pipeline first...${NC}"
    ./scripts/run_pipeline.sh
fi 

if [ -d "venv" ]; then
    source venv/bin/activate
fi

pip install fastapi uvicorn sqlalchemy alembic -q

if [ -f "alembic.ini" ]; then
    echo -e "${GREEN}Running database migrations...${NC}"
    alembic upgrade head
fi

echo -e "${GREEN}Starting server on http://localhost:8000${NC}"
echo -e "${GREEN}API Documentation: http://localhost:8000/docs${NC}"
echo -e "${GREEN}Alternative docs: http://localhost:8000/redoc${NC}"
echo ""

uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload