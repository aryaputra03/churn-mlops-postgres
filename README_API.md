# Churn Prediction API Documentation

## Overview

FastAPI-based REST API untuk prediksi customer churn dengan database logging menggunakan SQLAlchemy.

## Features

✅ **REST API Endpoints** - Complete prediction API
✅ **Database Integration** - SQLAlchemy with SQLite/PostgreSQL
✅ **Request Validation** - Pydantic models
✅ **Interactive Docs** - Swagger UI & ReDoc
✅ **Logging** - Prediction history tracking
✅ **Analytics** - Real-time statistics
✅ **Batch Processing** - Multiple predictions
✅ **CSV Upload** - File-based predictions

---

## Quick Start

### 1. Install Dependencies
```bash
pip install fastapi uvicorn sqlalchemy
```

### 2. Start Server
```bash
# Development mode
uvicorn src.api.main:app --reload

# Production mode
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --workers 4

# Using Make
make api-run
```

### 3. Access API
- **API**: http://localhost:8000
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

---

## Docker Deployment

### SQLite (Development)
```bash
docker-compose up api
```

### PostgreSQL (Production)
```bash
docker-compose up postgres api-postgres
```

---

## Database Setup

### SQLite (Default)
```bash
# Initialize database
make db-init

# Database file: churn_predictions.db
```

### PostgreSQL
```bash
# Set environment variable
export DATABASE_URL="postgresql://user:password@localhost/churn_db"

# Or in docker-compose.yml
environment:
  - DATABASE_URL=postgresql://churn_user:churn_password@postgres:5432/churn_db
```

---

## API Endpoints

### Health & Info
- `GET /` - Root endpoint
- `GET /health` - Health check
- `GET /model/info` - Model information

### Predictions
- `POST /predict` - Single prediction
- `POST /predict/batch` - Batch predictions
- `POST /predict/csv` - CSV file upload

### History & Analytics
- `GET /predictions/history` - All predictions
- `GET /predictions/customer/{id}` - Customer predictions
- `GET /analytics/summary` - Statistics

### Management
- `POST /model/reload` - Reload model

---

## Database Schema

### PredictionLog
```sql
CREATE TABLE prediction_logs (
    id INTEGER PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    prediction INTEGER NOT NULL,
    probability FLOAT NOT NULL,
    input_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Customer
```sql
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    customer_id VARCHAR UNIQUE NOT NULL,
    gender VARCHAR,
    tenure INTEGER,
    monthly_charges FLOAT,
    total_charges FLOAT,
    contract VARCHAR,
    payment_method VARCHAR,
    internet_service VARCHAR,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### ModelMetrics
```sql
CREATE TABLE model_metrics (
    id INTEGER PRIMARY KEY,
    model_version VARCHAR NOT NULL,
    accuracy FLOAT,
    precision FLOAT,
    recall FLOAT,
    f1_score FLOAT,
    roc_auc FLOAT,
    confusion_matrix JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Testing

```bash
# Run API tests
pytest tests/test_api.py -v

# Test with coverage
pytest tests/test_api.py --cov=src.api

# Using Make
make api-test
```

---

## Production Deployment

### Using Gunicorn + Uvicorn Workers
```bash
gunicorn src.api.main:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --timeout 120
```

### Using Docker
```bash
docker build -t churn-api -f docker/Dockerfile.api .
docker run -d -p 8000:8000 --name churn-api churn-api
```

### Environment Variables
```bash
# Database
DATABASE_URL=postgresql://user:pass@host/db

# API Settings
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4

# Model
MODEL_PATH=models/churn_model.pkl
```

---

## Monitoring & Logging

### Application Logs
```bash
# View logs
docker logs churn-api -f

# In application
tail -f logs/api.log
```

### Database Monitoring
```python
# Get statistics
curl http://localhost:8000/analytics/summary

# Response
{
  "total_predictions": 1250,
  "churn_rate": 24.96,
  "recent_predictions_24h": 45
}
```

---

## Security Considerations

### In Production:
1. **Use HTTPS** - Add TLS/SSL certificate
2. **Authentication** - Implement JWT/OAuth2
3. **Rate Limiting** - Prevent abuse
4. **CORS** - Restrict allowed origins
5. **Input Validation** - Already handled by Pydantic
6. **Database** - Use PostgreSQL with proper credentials
7. **Secrets** - Use environment variables

---

## Performance Tips

1. **Use PostgreSQL** for production (better performance)
2. **Enable Uvicorn workers** for concurrent requests
3. **Add Redis** for caching predictions
4. **Database indexing** on customer_id
5. **Async operations** for batch predictions
6. **Load balancer** for horizontal scaling

---

## Troubleshooting

### Model not loading
```bash
# Check model file exists
ls -lh models/churn_model.pkl

# Reload model
curl -X POST http://localhost:8000/model/reload
```

### Database errors
```bash
# Reset database
make db-reset

# Check database file
sqlite3 churn_predictions.db ".tables"
```

### Port already in use
```bash
# Find process
lsof -ti:8000

# Kill process
kill -9 $(lsof -ti:8000)
```

---

## Contact & Support

- **Email**: aryaganendra45@gmail.com
- **GitHub**: @aryaputra03
- **Issues**: [GitHub Issues](https://github.com/aryaputra03/Docker_Churn_Classifier/issues)
```

---

## Summary

Sekarang proyek Anda memiliki **FastAPI REST API lengkap** dengan:

✅ **Complete API Endpoints** - Single, batch, CSV predictions
✅ **SQLAlchemy Integration** - SQLite & PostgreSQL support
✅ **Database Logging** - Track all predictions
✅ **Analytics Dashboard** - Real-time statistics
✅ **Interactive Docs** - Swagger UI & ReDoc
✅ **Docker Ready** - Containerized API service
✅ **Production Ready** - With Gunicorn/Uvicorn
✅ **Comprehensive Tests** - API endpoint testing

### Quick Commands:
```bash
# Run API locally
make api-run

# Run API in Docker
docker-compose up api

# Run with PostgreSQL
docker-compose up postgres api-postgres

# Test API
make api-test

# Access docs
# http://localhost:8000/docs
```

Semua file sudah siap digunakan!