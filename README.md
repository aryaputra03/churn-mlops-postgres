# 🐳 Customer Churn Classification - Complete MLOps Project

[![CI/CD Pipeline](https://github.com/yourusername/churn-classification-mlops/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/yourusername/churn-classification-mlops/actions)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-ready-brightgreen.svg)](https://www.docker.com/)
[![DVC](https://img.shields.io/badge/data-dvc-9cf.svg)](https://dvc.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-00a393.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production-ready MLOps project for customer churn prediction featuring Docker containerization, DVC data versioning, FastAPI REST API, JWT authentication, rate limiting, database integration, and automated CI/CD with GitHub Actions.

---

## 📋 Table of Contents

- [Features](#-features)
- [Project Structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Installation](#-installation)
- [Authentication & Security](#-authentication--security)
- [ML Pipeline Usage](#-ml-pipeline-usage)
- [API Usage](#-api-usage)
- [Docker Workflows](#-docker-workflows)
- [DVC Integration](#-dvc-integration)
- [Database Management](#-database-management)
- [CI/CD Pipeline](#-cicd-pipeline)
- [Configuration](#-configuration)
- [Testing](#-testing)
- [Deployment](#-deployment)
- [Monitoring & Analytics](#-monitoring--analytics)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)

---

## ✨ Features

### Core ML Features
- 🤖 **Complete ML Pipeline**: End-to-end churn prediction workflow
- 🐳 **Docker Containerization**: Fully containerized with multi-stage builds
- 📊 **DVC Integration**: Data and model versioning with DVC
- 🔄 **CI/CD Automation**: Automated testing and deployment with GitHub Actions
- 🧪 **Comprehensive Testing**: Unit tests with pytest and coverage reporting
- 📈 **Metrics Tracking**: Automated evaluation and performance monitoring
- 🔧 **Configurable**: YAML-based configuration for easy experimentation

### API Features
- 🚀 **REST API**: FastAPI-based prediction service
- 🔐 **JWT Authentication**: Secure token-based authentication
- ⚡ **Rate Limiting**: Redis-powered request throttling
- 👥 **User Management**: Registration, login, and role-based access
- 💾 **Database Integration**: SQLAlchemy with SQLite/PostgreSQL support
- 📝 **Request Validation**: Pydantic models for data validation
- 📚 **Interactive Docs**: Automatic Swagger UI & ReDoc generation
- 📊 **Prediction Logging**: Complete history tracking in database
- 📈 **Real-time Analytics**: Statistics and performance monitoring
- 🔄 **Batch Processing**: Support for multiple predictions
- 📁 **CSV Upload**: File-based batch predictions

### Security Features
- 🔒 **Password Hashing**: Bcrypt encryption
- 🎫 **OAuth2 Flow**: Standard authentication protocol
- 🔄 **Token Refresh**: Long-lived sessions
- 👮 **Role-Based Access**: User/Admin permissions
- 🚦 **Request Tracking**: User activity monitoring
- 🛡️ **CORS Protection**: Configurable cross-origin policies

### Technical Stack
- **ML Framework**: Scikit-learn
- **API Framework**: FastAPI, Uvicorn
- **Authentication**: JWT, OAuth2, Bcrypt
- **Rate Limiting**: Redis, SlowAPI
- **Database**: SQLAlchemy (SQLite, PostgreSQL)
- **Containerization**: Docker, Docker Compose
- **Data Versioning**: DVC (supports S3, GDrive, local)
- **CI/CD**: GitHub Actions
- **Testing**: Pytest, Coverage
- **Linting**: Ruff, Black, Flake8

---

## 📁 Project Structure

```
churn-classification-mlops/
├── .github/
│   └── workflows/
│       ├── ci-cd.yml                  # Main CI/CD pipeline
│       └── docker-publish.yml         # Docker image publishing
│
├── data/
│   ├── raw/                           # Raw data (tracked by DVC)
│   ├── processed/                     # Processed data (tracked by DVC)
│   └── README.md
│
├── models/                            # Trained models (tracked by DVC)
│   └── churn_model.pkl
│
├── src/
│   ├── __init__.py
│   ├── config.py                      # Configuration management
│   ├── utils.py                       # Utility functions
│   ├── preprocess.py                  # Data preprocessing
│   ├── train.py                       # Model training
│   ├── evaluate.py                    # Model evaluation
│   ├── predict.py                     # Inference/prediction
│   │
│   └── api/                           # FastAPI application
│       ├── __init__.py
│       ├── main.py                    # API entry point
│       ├── auth.py                    # Authentication logic
│       ├── models.py                  # Pydantic models
│       ├── database.py                # Database configuration
│       ├── crud.py                    # Database operations
│       ├── schemas.py                 # Request/Response schemas
│       ├── security.py                # JWT & password utilities
│       └── rate_limit.py              # Rate limiting logic
│
├── tests/
│   ├── __init__.py
│   ├── test_preprocess.py
│   ├── test_train.py
│   ├── test_evaluate.py
│   ├── test_api.py                    # API endpoint tests
│   ├── test_auth.py                   # Authentication tests
│   └── test_rate_limit.py             # Rate limiting tests
│
├── docker/
│   ├── Dockerfile                     # Production Docker image
│   ├── Dockerfile.dev                 # Development Docker image
│   ├── Dockerfile.api                 # API-specific Docker image
│   └── docker-compose.yml             # Multi-container orchestration
│
├── scripts/
│   ├── setup_dvc.sh                   # DVC initialization script
│   ├── run_pipeline.sh                # Complete pipeline runner
│   ├── docker_build.sh                # Docker build helper
│   ├── docker_compose_run.sh          # Docker Compose helper
│   ├── create_admin.py                # Create admin user
│   └── generate_secret.py             # Generate secret key
│
├── plots/                             # Visualization outputs
│   └── confusion_matrix.json
│
├── .dvc/                              # DVC configuration
│   └── config
│
├── .env.example                       # Environment variables template
├── dvc.yaml                           # DVC pipeline definition
├── dvc.lock                           # DVC pipeline lock file
├── params.yaml                        # Hyperparameters & config
├── metrics.json                       # Model metrics output
│
├── requirements.txt                   # Production dependencies
├── requirements-dev.txt               # Development dependencies
├── setup.py                           # Package setup
├── Makefile                           # Automation commands
├── .gitignore
├── .dockerignore
├── .dvcignore
└── README.md
```

---

## 🛠️ Prerequisites

### Required
- **Python** 3.9 or higher
- **Docker** 20.10 or higher
- **Docker Compose** 2.0 or higher
- **Git** 2.30 or higher
- **Redis** 7.0+ (for rate limiting)

### Optional
- **DVC** 3.30 or higher (for data versioning)
- **PostgreSQL** 13+ (for production database)
- **Make** (for using Makefile commands)

### Windows Users
- **Docker Desktop** with WSL2 backend
- **WSL2** (Ubuntu 20.04 or later recommended)

---

## 🚀 Quick Start

### Complete Setup (ML + API + Auth)

```bash
# 1. Clone repository
git clone https://github.com/yourusername/churn-classification-mlops.git
cd churn-classification-mlops

# 2. Install all dependencies
make install-dev

# 3. Generate secret key for JWT
make generate-secret

# 4. Setup environment variables
cp .env.example .env
# Edit .env with your SECRET_KEY

# 5. Start Redis (required for rate limiting)
make redis-start

# 6. Setup project (initialize DVC, database, etc.)
make setup

# 7. Create admin user
make create-admin

# 8. Generate sample data
make data

# 9. Run ML pipeline
make pipeline

# 10. Start API server
make api-run

# 11. Access API documentation
# Open browser: http://localhost:8000/docs
```

### Docker Quick Start

```bash
# Start all services (ML + API + Database + Redis)
docker-compose up

# Access:
# - API: http://localhost:8000
# - API Docs: http://localhost:8000/docs
# - Redis: localhost:6379
# - Jupyter: http://localhost:8888
```

---

## 💻 Installation

### Local Development Setup

```bash
# 1. Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 2. Upgrade pip
pip install --upgrade pip

# 3. Install all dependencies (including API & Auth)
pip install -r requirements-dev.txt
pip install fastapi uvicorn sqlalchemy pydantic python-jose[cryptography] passlib[bcrypt] python-multipart redis slowapi

# 4. Install Redis
# macOS
brew install redis
brew services start redis

# Ubuntu/Debian
sudo apt-get install redis-server
sudo systemctl start redis

# 5. Verify installation
python -c "import src; import fastapi; import redis; print('✓ Installation successful')"
pytest --version
docker --version
redis-cli ping
```

### Docker Setup

```bash
# Build all images
make docker-build-all

# Or build individually:
docker build -t churn-classifier:latest -f docker/Dockerfile .
docker build -t churn-classifier:dev -f docker/Dockerfile.dev .
docker build -t churn-api:latest -f docker/Dockerfile.api .
```

### Database Setup

```bash
# Initialize SQLite database (default)
make db-init

# Or manually:
python -c "from src.api.database import init_db; init_db()"

# For PostgreSQL (production):
export DATABASE_URL="postgresql://user:password@localhost/churn_db"
make db-init
```

---

## 🔐 Authentication & Security

### Quick Authentication Setup

#### 1. Generate Secret Key
```bash
# Generate cryptographically secure key
make generate-secret

# Or manually:
python -c "import secrets; print(secrets.token_urlsafe(32))"

# Add to .env file
echo "SECRET_KEY=your-generated-key" >> .env
```

#### 2. Start Redis (Required for Rate Limiting)
```bash
# Local Redis
make redis-start

# Or Docker Redis
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Test connection
redis-cli ping  # Should return: PONG
```

#### 3. Create Admin User
```bash
# Using script
python scripts/create_admin.py

# Or using Make
make create-admin

# Follow prompts to set username, email, and password
```

### Authentication Flow

#### User Registration
```bash
curl -X POST "http://localhost:8000/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "johndoe",
    "email": "john@example.com",
    "password": "SecurePass123!",
    "full_name": "John Doe"
  }'
```

**Response:**
```json
{
  "id": 1,
  "username": "johndoe",
  "email": "john@example.com",
  "full_name": "John Doe",
  "role": "user",
  "is_active": true,
  "is_verified": false,
  "request_count": 0,
  "created_at": "2024-01-15T10:00:00Z"
}
```

#### User Login
```bash
curl -X POST "http://localhost:8000/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=johndoe&password=SecurePass123!"
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 1800
}
```

#### Using Access Token
```bash
# Set token as environment variable
export TOKEN="your-access-token"

# Make authenticated request
curl -X POST "http://localhost:8000/predict" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "CUST001",
    "gender": "Male",
    "tenure": 24,
    "monthly_charges": 75.5,
    "total_charges": 1810.0,
    "contract": "One year",
    "payment_method": "Bank transfer",
    "internet_service": "Fiber optic"
  }'
```

#### Get Current User Profile
```bash
curl -X GET "http://localhost:8000/auth/me" \
  -H "Authorization: Bearer $TOKEN"
```

### Rate Limiting

#### Default Rate Limits

| Endpoint | Limit | Description |
|----------|-------|-------------|
| `/` | 60/minute | Root endpoint |
| `/health` | 100/minute | Health check |
| `/auth/register` | 5/hour | New registrations |
| `/auth/token` | 10/minute | Login attempts |
| `/predict` | 30/minute | Single predictions |
| `/predict/batch` | 10/hour | Batch predictions |
| `/predict/csv` | 5/hour | CSV uploads |

#### Rate Limit Headers

All responses include rate limit information:
```
X-RateLimit-Limit: 30
X-RateLimit-Remaining: 25
X-RateLimit-Reset: 1642248000
Retry-After: 60
```

#### Rate Limit Exceeded Response

**HTTP 429 - Too Many Requests:**
```json
{
  "error": "Rate limit exceeded",
  "detail": "30 per 1 minute",
  "retry_after": 60
}
```

### User Roles & Permissions

#### Available Roles
- **user**: Regular user with standard access (default)
- **admin**: Administrator with full system access

#### Role-Based Endpoints

**Admin-Only Endpoints:**
```bash
# List all users (admin only)
curl -X GET "http://localhost:8000/auth/users" \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Update user role (admin only)
curl -X PUT "http://localhost:8000/auth/users/2/role" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"role": "admin"}'

# Deactivate user (admin only)
curl -X PUT "http://localhost:8000/auth/users/2/deactivate" \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

### Security Best Practices

#### 1. Secret Key Management
```bash
# Generate secure key (minimum 32 characters)
python -c "import secrets; print(secrets.token_urlsafe(32))"

# Store in environment variable (production)
export SECRET_KEY="your-production-secret-key-min-32-chars"

# Or in .env file (development)
SECRET_KEY=your-development-secret-key
```

#### 2. Password Requirements
The system enforces strong password policies:
- Minimum 8 characters
- At least one uppercase letter
- At least one lowercase letter
- At least one digit
- At least one special character
- No common/weak passwords

#### 3. HTTPS in Production
```python
# Always use HTTPS in production
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=443,
        ssl_keyfile="/path/to/key.pem",
        ssl_certfile="/path/to/cert.pem"
    )
```

#### 4. CORS Configuration
```python
# In production, specify allowed origins (never use "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)
```

#### 5. Environment Variables
```bash
# Required security variables
SECRET_KEY=your-production-secret-key-min-32-chars
DATABASE_URL=postgresql://user:pass@host/db
REDIS_URL=redis://prod-redis:6379/0

# Optional security settings
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7
RATE_LIMIT_ENABLED=true
CORS_ORIGINS=https://yourdomain.com
```

---

## 📚 ML Pipeline Usage

### Running Individual Steps

#### 1. Data Preprocessing
```bash
# Local
python -m src.preprocess --config params.yaml

# Docker
docker run --rm \
  -v $(pwd)/data:/app/data \
  churn-classifier:latest \
  python -m src.preprocess

# Make
make preprocess
```

#### 2. Model Training
```bash
# Local
python -m src.train --config params.yaml

# Docker
docker run --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/models:/app/models \
  churn-classifier:latest \
  python -m src.train

# Make
make train
```

#### 3. Model Evaluation
```bash
# Local
python -m src.evaluate --config params.yaml

# Docker
docker run --rm \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/models:/app/models \
  churn-classifier:latest \
  python -m src.evaluate

# Make
make evaluate
```

### Running Complete Pipeline

```bash
# Option 1: Using Make
make pipeline

# Option 2: Using DVC
dvc repro

# Option 3: Using Docker Compose
docker-compose up ml-pipeline-full

# Option 4: Using script
./scripts/run_pipeline.sh
```

---

## 🚀 API Usage

### Starting the API Server

```bash
# Development mode (auto-reload)
uvicorn src.api.main:app --reload

# Production mode
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --workers 4

# Using Make
make api-run

# Using Docker
docker-compose up api
```

### API Endpoints Overview

#### Health & Information
```bash
# Root endpoint
curl http://localhost:8000/

# Health check
curl http://localhost:8000/health

# Model information (requires authentication)
curl http://localhost:8000/model/info \
  -H "Authorization: Bearer $TOKEN"
```

#### Authentication Endpoints
```bash
# Register new user
curl -X POST "http://localhost:8000/auth/register" \
  -H "Content-Type: application/json" \
  -d '{"username": "user1", "email": "user@example.com", "password": "SecurePass123!", "full_name": "User One"}'

# Login
curl -X POST "http://localhost:8000/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=user1&password=SecurePass123!"

# Get current user
curl -X GET "http://localhost:8000/auth/me" \
  -H "Authorization: Bearer $TOKEN"

# Refresh token
curl -X POST "http://localhost:8000/auth/refresh" \
  -H "Authorization: Bearer $REFRESH_TOKEN"
```

#### Single Prediction (Authenticated)
```bash
curl -X POST "http://localhost:8000/predict" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "CUST001",
    "gender": "Female",
    "tenure": 12,
    "monthly_charges": 70.35,
    "total_charges": 844.20,
    "contract": "Month-to-month",
    "payment_method": "Electronic check",
    "internet_service": "Fiber optic"
  }'

# Response:
{
  "customer_id": "CUST001",
  "prediction": 1,
  "churn_label": "Will Churn",
  "probability": 0.78,
  "confidence": "high",
  "prediction_id": 1,
  "created_at": "2024-01-15T10:30:00Z"
}
```

#### Batch Predictions (Authenticated)
```bash
curl -X POST "http://localhost:8000/predict/batch" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "customers": [
      {
        "customer_id": "CUST001",
        "gender": "Female",
        "tenure": 12,
        "monthly_charges": 70.35,
        "total_charges": 844.20,
        "contract": "Month-to-month",
        "payment_method": "Electronic check",
        "internet_service": "Fiber optic"
      },
      {
        "customer_id": "CUST002",
        "gender": "Male",
        "tenure": 48,
        "monthly_charges": 45.50,
        "total_charges": 2184.00,
        "contract": "Two year",
        "payment_method": "Bank transfer",
        "internet_service": "DSL"
      }
    ]
  }'
```

#### CSV Upload (Authenticated)
```bash
curl -X POST "http://localhost:8000/predict/csv" \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@customers.csv"
```

#### Prediction History (Authenticated)
```bash
# All predictions for current user
curl http://localhost:8000/predictions/history?limit=10 \
  -H "Authorization: Bearer $TOKEN"

# Customer-specific predictions
curl http://localhost:8000/predictions/customer/CUST001 \
  -H "Authorization: Bearer $TOKEN"

# Analytics summary (admin only)
curl http://localhost:8000/analytics/summary \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

### Interactive API Documentation

Visit these URLs when the API is running:

- **Swagger UI**: http://localhost:8000/docs
  - Interactive testing interface
  - Authentication support (click "Authorize" button)
  - Try all endpoints directly

- **ReDoc**: http://localhost:8000/redoc
  - Clean, organized documentation
  - Detailed schemas and examples

---

## 🐳 Docker Workflows

### Development Workflow

```bash
# Start development environment
docker-compose up ml-dev

# Access Jupyter notebook
# URL: http://localhost:8888

# Run pipeline inside container
docker exec -it churn-ml-dev dvc repro
```

### Production Workflow

```bash
# Build production images
make docker-build-all

# Start all services (API + Redis + Database)
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f api

# Stop services
docker-compose down
```

### Multi-Service Setup

```yaml
# docker-compose.yml includes:
services:
  redis:         # Redis for rate limiting
  postgres:      # PostgreSQL database
  api:           # API with SQLite
  api-postgres:  # API with PostgreSQL
  ml-training:   # ML training service
  ml-dev:        # Development environment
  dvc-pipeline:  # DVC pipeline runner
```

---

## 📊 DVC Integration

### Basic DVC Commands

```bash
# Initialize DVC
dvc init

# Add remote storage
dvc remote add -d myremote s3://my-bucket/dvc-storage

# Pull data from remote
dvc pull

# Run pipeline
dvc repro

# Push data/models to remote
dvc push

# Show pipeline DAG
dvc dag

# Show metrics
dvc metrics show

# Compare experiments
dvc metrics diff
```

### Track New Data

```bash
# Add data file
dvc add data/raw/new_data.csv

# Commit DVC file
git add data/raw/new_data.csv.dvc .gitignore
git commit -m "Add new data"

# Push to remote
dvc push
```

---

## 💾 Database Management

### Database Schema

The API uses these main tables:

#### User
Stores user accounts and authentication
```sql
- id: Primary key
- username: Unique username
- email: Unique email address
- hashed_password: Bcrypt hashed password
- full_name: User's full name
- role: user/admin role
- is_active: Account status
- is_verified: Email verification status
- request_count: Total API requests
- created_at: Registration timestamp
- last_login: Last login timestamp
```

#### PredictionLog
Stores all prediction requests and results
```sql
- id: Primary key
- customer_id: Customer identifier
- user_id: Foreign key to User
- prediction: Churn prediction (0/1)
- probability: Churn probability
- input_data: JSON of input features
- created_at: Timestamp
```

#### Customer
Stores customer information
```sql
- id: Primary key
- customer_id: Unique customer ID
- gender, tenure, monthly_charges, etc.
- is_active: Customer status
- created_at: Registration timestamp
```

#### ModelMetrics
Tracks model performance over time
```sql
- id: Primary key
- model_version: Model identifier
- accuracy, precision, recall, f1_score
- confusion_matrix: JSON format
- created_at: Evaluation timestamp
```

### Database Operations

```bash
# Initialize database
make db-init

# Reset database
make db-reset

# Create admin user
make create-admin

# View database stats
sqlite3 churn_predictions.db "SELECT COUNT(*) FROM users;"
sqlite3 churn_predictions.db "SELECT COUNT(*) FROM prediction_logs;"

# Export predictions
sqlite3 churn_predictions.db ".mode csv" ".output predictions.csv" "SELECT * FROM prediction_logs;"
```

### PostgreSQL Setup (Production)

```bash
# Start PostgreSQL container
docker-compose up postgres

# Connect to database
docker exec -it churn-postgres psql -U churn_user -d churn_db

# Run API with PostgreSQL
export DATABASE_URL="postgresql://churn_user:churn_password@localhost:5432/churn_db"
uvicorn src.api.main:app --reload
```

---

## 🔄 CI/CD Pipeline

### GitHub Actions Workflow

The automated pipeline runs on push and pull requests:

```
Push/PR → Lint → Test → Auth Tests → DVC Pipeline → Docker Build → Integration Tests → Deploy
```

### Pipeline Stages

1. **Lint**: Code quality checks (Ruff, Black, Flake8)
2. **Test**: Unit tests with coverage (Python 3.9, 3.10, 3.11)
3. **Auth Tests**: Authentication and rate limiting tests
4. **DVC Pipeline**: Run ML pipeline in Docker
5. **Docker Build**: Build and push images
6. **Integration Tests**: End-to-end testing
7. **API Tests**: Test all endpoints
8. **Security Scan**: Dependency vulnerability checks

### Setup GitHub Secrets

Required secrets for CI/CD:
```
DOCKER_USERNAME: your-dockerhub-username
DOCKER_PASSWORD: your-dockerhub-token
AWS_ACCESS_KEY_ID: your-aws-key (if using S3)
AWS_SECRET_ACCESS_KEY: your-aws-secret (if using S3)
DATABASE_URL: production database URL
SECRET_KEY: JWT secret key for production
REDIS_URL: Redis connection URL
```

---

## ⚙️ Configuration

### params.yaml Structure

```yaml
data:
  raw_path: data/raw/churn_data.csv
  processed_path: data/processed/churn_processed.csv

preprocess:
  test_size: 0.2
  random_state: 42
  numerical_features:
    - tenure
    - MonthlyCharges
    - TotalCharges
  categorical_features:
    - gender
    - Contract
    - PaymentMethod

train:
  model_type: random_forest
  n_estimators: 100
  max_depth: 10
  min_samples_split: 5
  random_state: 42

api:
  host: 0.0.0.0
  port: 8000
  workers: 4
  reload: false

auth:
  secret_key: ${SECRET_KEY}
  algorithm: HS256
  access_token_expire_minutes: 30
  refresh_token_expire_days: 7

rate_limit:
  enabled: true
  redis_url: ${REDIS_URL}
  default_limit: "60/minute"

database:
  type: sqlite  # or postgresql
  url: sqlite:///churn_predictions.db
```

### Environment Variables (.env)

```bash
# Security
SECRET_KEY=your-production-secret-key-min-32-chars
ALGORITHM=HS256

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4

# Authentication
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7

# Database
DATABASE_URL=sqlite:///churn_predictions.db
# DATABASE_URL=postgresql://user:pass@host:5432/db

# Redis (Rate Limiting)
REDIS_URL=redis://localhost:6379/0

# Model
MODEL_PATH=models/churn_model.pkl

# DVC
DVC_REMOTE_URL=s3://my-bucket/dvc-storage
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret

# CORS
CORS_ORIGINS=https://yourdomain.com

# Rate Limiting
RATE_LIMIT_ENABLED=true
```

---

## 🧪 Testing

### Run All Tests

```bash
# All tests (ML + API + Auth)
make test

# ML pipeline tests only
pytest tests/test_*.py -v --ignore=tests/test_api.py --ignore=tests/test_auth.py

# API tests only
make api-test

# Authentication tests only
make test-auth

# Rate limiting tests only
make test-rate-limit

# With coverage
make test-coverage
```

### Test Specific Components

```bash
# Test preprocessing
pytest tests/test_preprocess.py -v

# Test training
pytest tests/test_train.py -v

# Test evaluation
pytest tests/test_evaluate.py -v

# Test API endpoints
pytest tests/test_api.py -v

# Test authentication
pytest tests/test_auth.py -v

# Test rate limiting
pytest tests/test_rate_limit.py -v

# Test with keyword
pytest tests/ -k "test_predict" -v

# Test specific function
pytest tests/test_auth.py::test_register_user -v
```

### Coverage Report

```bash
# Terminal report
pytest tests/ --cov=src --cov-report=term-missing

# HTML report
pytest tests/ --cov=src --cov-report=html
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
start htmlcov/index.html  # Windows

# XML report (for CI/CD)
pytest tests/ --cov=src --cov-report=xml
```

### Integration Testing

```bash
# Test complete workflow
make test-integration

# Test API with authentication
pytest tests/test_api.py tests/test_auth.py -v

# Test with Docker
docker-compose -f docker-compose.test.yml up --abort-on-container-exit
```

---

## 🚀 Deployment

### Local Production Deployment

```bash
# Using Gunicorn + Uvicorn workers
gunicorn src.api.main:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --timeout 120 \
  --access-logfile logs/access.log \
  --error-logfile logs/error.log \
  --log-level info
```

### Docker Production Deployment

```bash
# Pull latest image
docker pull yourusername/churn-api:latest

# Run container with all required services
docker run -d \
  --name churn-api \
  -p 8000:8000 \
  -v $(pwd)/models:/app/models \
  -e SECRET_KEY=$SECRET_KEY \
  -e DATABASE_URL=postgresql://user:pass@host/db \
  -e REDIS_URL=redis://redis:6379/0 \
  --network churn-network \
  yourusername/churn-api:latest

# Check logs
docker logs churn-api -f

# Health check
curl http://localhost:8000/health
```

### Docker Compose Production

```bash
# Start production stack
docker-compose -f docker-compose.prod.yml up -d

# Scale API workers
docker-compose -f docker-compose.prod.yml up -d --scale api=3

# Check status
docker-compose -f docker-compose.prod.yml ps

# View logs
docker-compose -f docker-compose.prod.yml logs -f

# Stop services
docker-compose -f docker-compose.prod.yml down
```

### Kubernetes Deployment

#### 1. Create Kubernetes Resources

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: churn-api
  labels:
    app: churn-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: churn-api
  template:
    metadata:
      labels:
        app: churn-api
    spec:
      containers:
      - name: churn-api
        image: yourusername/churn-api:latest
        ports:
        - containerPort: 8000
        env:
        - name: SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: churn-secrets
              key: secret-key
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: churn-secrets
              key: database-url
        - name: REDIS_URL
          value: "redis://redis-service:6379/0"
        volumeMounts:
        - name: models
          mountPath: /app/models
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: models
        persistentVolumeClaim:
          claimName: models-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: churn-api-service
spec:
  selector:
    app: churn-api
  ports:
  - port: 80
    targetPort: 8000
  type: LoadBalancer
---
apiVersion: v1
kind: Service
metadata:
  name: redis-service
spec:
  selector:
    app: redis
  ports:
  - port: 6379
    targetPort: 6379
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        ports:
        - containerPort: 6379
        volumeMounts:
        - name: redis-data
          mountPath: /data
      volumes:
      - name: redis-data
        persistentVolumeClaim:
          claimName: redis-pvc
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: models-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: redis-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
```

#### 2. Create Secrets

```bash
# Create secret for sensitive data
kubectl create secret generic churn-secrets \
  --from-literal=secret-key='your-production-secret-key' \
  --from-literal=database-url='postgresql://user:pass@host/db'
```

#### 3. Apply Configuration

```bash
# Apply all resources
kubectl apply -f k8s/

# Check deployment status
kubectl get deployments
kubectl get pods
kubectl get services

# Check logs
kubectl logs -f deployment/churn-api

# Scale deployment
kubectl scale deployment/churn-api --replicas=5
```

### Cloud Deployment Examples

#### AWS ECS

```bash
# Create task definition
aws ecs register-task-definition --cli-input-json file://ecs-task-definition.json

# Create service
aws ecs create-service \
  --cluster churn-cluster \
  --service-name churn-api-service \
  --task-definition churn-api:1 \
  --desired-count 3 \
  --load-balancer targetGroupArn=arn:aws:...,containerName=churn-api,containerPort=8000
```

#### Google Cloud Run

```bash
# Build and push image
gcloud builds submit --tag gcr.io/PROJECT_ID/churn-api

# Deploy
gcloud run deploy churn-api \
  --image gcr.io/PROJECT_ID/churn-api \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars SECRET_KEY=$SECRET_KEY,REDIS_URL=$REDIS_URL
```

#### Azure Container Instances

```bash
# Create container
az container create \
  --resource-group churn-rg \
  --name churn-api \
  --image yourusername/churn-api:latest \
  --dns-name-label churn-api \
  --ports 8000 \
  --environment-variables \
    SECRET_KEY=$SECRET_KEY \
    REDIS_URL=$REDIS_URL
```

---

## 📈 Monitoring & Analytics

### Application Metrics

```bash
# Get API analytics
curl http://localhost:8000/analytics/summary \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Response:
{
  "total_predictions": 1250,
  "total_customers": 450,
  "total_users": 25,
  "active_users": 20,
  "churn_rate": 24.96,
  "recent_predictions_24h": 45,
  "avg_churn_probability": 0.32,
  "model_version": "v1.0.0",
  "model_accuracy": 0.85,
  "uptime_seconds": 86400
}
```

### User Activity Monitoring

```bash
# Get user statistics (admin only)
curl http://localhost:8000/analytics/users \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Response:
{
  "total_users": 25,
  "active_users": 20,
  "new_users_today": 3,
  "top_users": [
    {
      "username": "johndoe",
      "request_count": 150,
      "last_login": "2024-01-15T10:00:00Z"
    }
  ]
}
```

### Database Monitoring

```bash
# Check prediction volume
sqlite3 churn_predictions.db \
  "SELECT DATE(created_at) as date, COUNT(*) as count 
   FROM prediction_logs 
   WHERE created_at >= date('now', '-7 days')
   GROUP BY DATE(created_at)
   ORDER BY date DESC;"

# Check churn rate by time period
sqlite3 churn_predictions.db \
  "SELECT 
    DATE(created_at) as date,
    AVG(prediction) * 100 as churn_rate,
    AVG(probability) as avg_probability,
    COUNT(*) as total_predictions
   FROM prediction_logs 
   GROUP BY DATE(created_at)
   ORDER BY date DESC
   LIMIT 30;"

# Check user activity
sqlite3 churn_predictions.db \
  "SELECT 
    u.username,
    u.request_count,
    u.role,
    COUNT(pl.id) as predictions,
    MAX(pl.created_at) as last_prediction
   FROM users u
   LEFT JOIN prediction_logs pl ON u.id = pl.user_id
   GROUP BY u.id
   ORDER BY predictions DESC;"
```

### Redis Monitoring

```bash
# Check Redis info
redis-cli info stats

# Monitor real-time commands
redis-cli monitor

# Check rate limit keys
redis-cli keys "rate_limit:*"

# Get rate limit info for specific user
redis-cli get "rate_limit:/predict:192.168.1.1"

# Clear all rate limits
redis-cli flushdb
```

### Logging

```bash
# View application logs
tail -f logs/api.log

# View access logs
tail -f logs/access.log

# View error logs
tail -f logs/error.log

# Docker logs
docker logs churn-api -f --tail=100

# Filter logs by level
docker logs churn-api 2>&1 | grep ERROR

# Kubernetes logs
kubectl logs -f deployment/churn-api --all-containers=true
```

### Performance Monitoring

```bash
# Monitor API response times
curl -w "@curl-format.txt" -o /dev/null -s http://localhost:8000/predict \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{...}'

# Create curl-format.txt:
# time_namelookup:  %{time_namelookup}\n
# time_connect:  %{time_connect}\n
# time_appconnect:  %{time_appconnect}\n
# time_pretransfer:  %{time_pretransfer}\n
# time_redirect:  %{time_redirect}\n
# time_starttransfer:  %{time_starttransfer}\n
# ----------\n
# time_total:  %{time_total}\n
```

### Prometheus Integration (Optional)

```python
# Add to src/api/main.py
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI()
Instrumentator().instrument(app).expose(app)

# Access metrics at /metrics
# curl http://localhost:8000/metrics
```

---

## 🐛 Troubleshooting

### Common Issues & Solutions

#### 1. Authentication Issues

**Problem: "Could not validate credentials"**
```bash
# Check token expiration
python -c "
import jwt
token = 'your-token-here'
decoded = jwt.decode(token, options={'verify_signature': False})
print(decoded)
"

# Solution: Login again to get fresh token
curl -X POST "http://localhost:8000/auth/token" \
  -d "username=your-username&password=your-password"
```

**Problem: "Invalid password"**
```bash
# Check password requirements
# - Minimum 8 characters
# - At least one uppercase letter
# - At least one digit
# - At least one special character

# Reset password (admin only)
python scripts/reset_password.py --username johndoe
```

#### 2. Rate Limiting Issues

**Problem: "Rate limit exceeded"**
```bash
# Check current rate limit status
redis-cli get "rate_limit:/predict:your-ip"

# Clear rate limits for testing
redis-cli flushdb

# Disable rate limiting temporarily
export RATE_LIMIT_ENABLED=false
uvicorn src.api.main:app --reload
```

**Problem: Redis connection failed**
```bash
# Check Redis is running
redis-cli ping  # Should return PONG

# Restart Redis
# macOS
brew services restart redis

# Ubuntu
sudo systemctl restart redis

# Docker
docker restart redis
```

#### 3. Model Loading Issues

**Problem: Model file not found**
```bash
# Check model file exists
ls -lh models/churn_model.pkl

# Retrain model
make train

# Reload model via API
curl -X POST http://localhost:8000/model/reload \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

**Problem: Model prediction errors**
```bash
# Check model metrics
python -m src.evaluate

# Validate input data format
curl -X POST http://localhost:8000/predict \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "TEST001", ...}'
```

#### 4. Database Errors

**Problem: Database locked**
```bash
# Close all connections
pkill -f "python.*api"

# Reset database
make db-reset

# For PostgreSQL, check connections
psql -U churn_user -d churn_db -c "SELECT * FROM pg_stat_activity;"
```

**Problem: Migration errors**
```bash
# Reset database and recreate tables
python -c "
from src.api.database import engine, Base
Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)
"

# Recreate admin user
make create-admin
```

#### 5. Docker Issues

**Problem: Container fails to start**
```bash
# Check logs
docker logs churn-api

# Check container status
docker ps -a

# Rebuild without cache
docker-compose build --no-cache

# Remove all containers and start fresh
docker-compose down -v
docker-compose up --build
```

**Problem: Port already in use**
```bash
# Find process using port 8000
lsof -ti:8000

# Kill process
kill -9 $(lsof -ti:8000)

# Or use different port
docker run -p 8001:8000 churn-api:latest
```

**Problem: Volume mounting issues**
```bash
# Check volume permissions
ls -la models/

# Fix permissions
chmod -R 755 models/
chown -R $USER:$USER models/

# Docker on Windows: use absolute paths
docker run -v C:/path/to/models:/app/models churn-api:latest
```

#### 6. API Connection Issues

**Problem: Connection refused**
```bash
# Check if API is running
curl http://localhost:8000/health

# Check Docker network
docker network ls
docker network inspect churn-network

# Restart services
docker-compose restart api

# Check firewall settings
sudo ufw allow 8000
```

**Problem: CORS errors**
```bash
# Update CORS settings in .env
CORS_ORIGINS=http://localhost:3000,https://yourdomain.com

# Or in src/api/main.py
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Only for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

#### 7. DVC Issues

**Problem: DVC pull fails**
```bash
# Check DVC config
dvc remote list
dvc config -l

# Test connection
dvc pull --verbose

# Re-configure remote
dvc remote modify myremote url s3://new-bucket
dvc remote modify myremote --local access_key_id $AWS_KEY
dvc remote modify myremote --local secret_access_key $AWS_SECRET
```

**Problem: DVC cache issues**
```bash
# Clear DVC cache
rm -rf .dvc/cache

# Rebuild cache
dvc pull
dvc repro
```

#### 8. Performance Issues

**Problem: Slow API responses**
```bash
# Check model loading time
python -c "
import time
import pickle
start = time.time()
with open('models/churn_model.pkl', 'rb') as f:
    model = pickle.load(f)
print(f'Load time: {time.time()-start:.2f}s')
"

# Increase workers
uvicorn src.api.main:app --workers 8

# Use Gunicorn
gunicorn src.api.main:app --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker
```

**Problem: High memory usage**
```bash
# Monitor memory
docker stats churn-api

# Limit container memory
docker run --memory="1g" churn-api:latest

# Optimize model size
python scripts/optimize_model.py
```

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
uvicorn src.api.main:app --reload --log-level debug

# Or in params.yaml
api:
  log_level: DEBUG
```

### Getting Help

```bash
# Check system requirements
make check-system

# Run diagnostic script
python scripts/diagnose.py

# View all environment variables
env | grep -E '(SECRET|DATABASE|REDIS|API)'

# Generate support bundle
make support-bundle
```

---

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

### Getting Started

```bash
# 1. Fork the repository
# Click "Fork" on GitHub

# 2. Clone your fork
git clone https://github.com/YOUR_USERNAME/churn-classification-mlops.git
cd churn-classification-mlops

# 3. Add upstream remote
git remote add upstream https://github.com/aryaputra03/churn-classification-mlops.git

# 4. Create a branch
git checkout -b feature/amazing-feature

# 5. Make your changes
# ... edit files ...

# 6. Run tests
make test

# 7. Commit changes
git add .
git commit -m "Add amazing feature"

# 8. Push to your fork
git push origin feature/amazing-feature

# 9. Create Pull Request
# Open PR on GitHub
```

### Development Guidelines

#### Code Style
- Follow PEP 8 for Python code
- Use type hints for function signatures
- Write docstrings for all functions and classes
- Keep functions small and focused

```python
# Good example
def calculate_churn_probability(
    customer_data: Dict[str, Any],
    model: Any
) -> float:
    """
    Calculate churn probability for a customer.
    
    Args:
        customer_data: Dictionary containing customer features
        model: Trained machine learning model
        
    Returns:
        Churn probability as float between 0 and 1
    """
    # Implementation
    pass
```

#### Testing Requirements
- Write tests for all new features
- Maintain test coverage above 80%
- Test both success and error cases
- Use meaningful test names

```python
# Good test example
def test_predict_endpoint_with_valid_data_returns_prediction():
    """Test that predict endpoint returns valid prediction for correct input."""
    # Arrange
    client = TestClient(app)
    valid_data = {...}
    
    # Act
    response = client.post("/predict", json=valid_data)
    
    # Assert
    assert response.status_code == 200
    assert "prediction" in response.json()
```

#### Documentation
- Update README.md for new features
- Add docstrings to all functions
- Include usage examples
- Update API documentation

### Pull Request Process

1. **Update Documentation**: Ensure README and docstrings are updated
2. **Add Tests**: Include tests for new functionality
3. **Pass CI/CD**: All GitHub Actions checks must pass
4. **Code Review**: Address reviewer feedback
5. **Squash Commits**: Clean up commit history before merge

### Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Example:
```
feat(api): add batch prediction endpoint

Add new endpoint for processing multiple predictions in a single request.
Includes rate limiting and authentication.

Closes #123
```

### Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Accept constructive criticism
- Focus on what's best for the project
- Show empathy towards other community members

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

```
MIT License

Copyright (c) 2024 Ganendra Geanza Aryaputra

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## 👥 Authors

**Ganendra Geanza Aryaputra (Stavanger)**

- 🐙 GitHub: [@aryaputra03](https://github.com/aryaputra03)
- 💼 LinkedIn: [Ganendra Geanza Aryaputra](https://www.linkedin.com/in/ganendra-geanza-aryaputra-b8071a194)
- 📧 Email: Aryaganendra45@gmail.com
- 🌐 Portfolio: [Coming Soon]

---

## 🙏 Acknowledgments

- **Scikit-learn Team** - Excellent machine learning framework
- **FastAPI Team** - Amazing web framework for building APIs
- **Docker Team** - Revolutionary containerization technology
- **DVC Team** - Powerful data versioning tools
- **GitHub** - Actions CI/CD platform and hosting
- **Redis Labs** - High-performance data store
- **SQLAlchemy** - Powerful ORM for Python
- **The Open-Source Community** - For inspiration and support

### Special Thanks

- All contributors who have helped improve this project
- Users who provide feedback and report issues
- The MLOps community for sharing best practices

---

## 📞 Contact & Support

### Project Links
- **📦 Repository**: [Docker_Churn_Classifier](https://github.com/aryaputra03/Docker_Churn_Classifier)
- **🐛 Issues**: [GitHub Issues](https://github.com/aryaputra03/Docker_Churn_Classifier/issues)
- **📖 Documentation**: [Wiki](https://github.com/aryaputra03/Docker_Churn_Classifier/wiki)
- **💬 Discussions**: [GitHub Discussions](https://github.com/aryaputra03/Docker_Churn_Classifier/discussions)

### Get Help
- 📧 **Email**: Aryaganendra45@gmail.com
- 💼 **LinkedIn**: [Connect with me](https://www.linkedin.com/in/ganendra-geanza-aryaputra-b8071a194)
- 🐛 **Bug Reports**: [Create an Issue](https://github.com/aryaputra03/Docker_Churn_Classifier/issues/new)
- 💡 **Feature Requests**: [Request a Feature](https://github.com/aryaputra03/Docker_Churn_Classifier/issues/new?labels=enhancement)

### Community
- ⭐ Star this repository if you find it helpful
- 🍴 Fork it to create your own version
- 📢 Share it with the community
- 🤝 Contribute to make it better

---

## 🎯 Project Roadmap

### Version 1.0 (Current) ✅
- ✅ Complete ML pipeline with DVC
- ✅ Docker containerization
- ✅ FastAPI REST API
- ✅ JWT Authentication
- ✅ Rate limiting with Redis
- ✅ Database integration
- ✅ CI/CD with GitHub Actions
- ✅ Comprehensive testing
- ✅ Interactive API documentation

### Version 1.1 (In Progress) 🚧
- 🔄 Advanced monitoring dashboard
- 🔄 Model A/B testing framework
- 🔄 Real-time prediction streaming
- 🔄 Email notifications for alerts
- 🔄 Webhook integration

### Version 2.0 (Planned) 📋
- 📅 Web UI dashboard
- 📅 Advanced analytics features
- 📅 Multi-model ensemble support
- 📅 Automated retraining pipeline
- 📅 Feature importance tracking
- 📅 Model explainability (SHAP/LIME)
- 📅 Data drift detection
- 📅 Model performance degradation alerts

### Version 3.0 (Future) 🔮
- 🔮 Cloud deployment templates (AWS, GCP, Azure)
- 🔮 Kubernetes Helm charts
- 🔮 Multi-tenancy support
- 🔮 GraphQL API
- 🔮 Real-time model serving with TensorFlow Serving
- 🔮 AutoML integration
- 🔮 Mobile app for predictions
- 🔮 Blockchain-based model versioning

---

## 📊 Project Statistics

![GitHub stars](https://img.shields.io/github/stars/aryaputra03/Docker_Churn_Classifier?style=social)
![GitHub forks](https://img.shields.io/github/forks/aryaputra03/Docker_Churn_Classifier?style=social)
![GitHub watchers](https://img.shields.io/github/watchers/aryaputra03/Docker_Churn_Classifier?style=social)
![GitHub issues](https://img.shields.io/github/issues/aryaputra03/Docker_Churn_Classifier)
![GitHub pull requests](https://img.shields.io/github/issues-pr/aryaputra03/Docker_Churn_Classifier)
![GitHub last commit](https://img.shields.io/github/last-commit/aryaputra03/Docker_Churn_Classifier)
![GitHub repo size](https://img.shields.io/github/repo-size/aryaputra03/Docker_Churn_Classifier)
![GitHub language count](https://img.shields.io/github/languages/count/aryaputra03/Docker_Churn_Classifier)
![GitHub top language](https://img.shields.io/github/languages/top/aryaputra03/Docker_Churn_Classifier)

---

## 🌟 Features Showcase

### 🔥 What Makes This Project Special

1. **Production-Ready Architecture**
   - Enterprise-grade security with JWT & OAuth2
   - Scalable microservices design
   - Production-tested Docker containers
   - High-availability setup

2. **Complete MLOps Pipeline**
   - Data versioning with DVC
   - Automated model training
   - Performance monitoring
   - CI/CD integration

3. **Developer-Friendly**
   - Comprehensive documentation
   - Interactive API docs
   - Easy local setup
   - Extensive examples

4. **Security First**
   - Role-based access control
   - Rate limiting protection
   - Password encryption
   - Token-based authentication

---

## 💡 Use Cases

This project can be adapted for various use cases:

### Business Applications
- 🏦 **Banking**: Predict customer account closure
- 📱 **Telecom**: Identify subscriber churn
- 🛒 **E-commerce**: Forecast customer retention
- 💳 **SaaS**: Predict subscription cancellations
- 🏥 **Healthcare**: Predict patient no-shows

### Educational
- 📚 Learn MLOps best practices
- 🎓 Understand production ML systems
- 💻 Practice Docker & Kubernetes
- 🔧 Explore FastAPI development

### Research
- 🔬 Experiment with ML models
- 📊 Analyze customer behavior
- 🧪 Test new algorithms
- 📈 Compare model performance

---

## 🔗 Related Projects

- [MLflow](https://github.com/mlflow/mlflow) - ML lifecycle management
- [Kubeflow](https://github.com/kubeflow/kubeflow) - ML on Kubernetes
- [BentoML](https://github.com/bentoml/BentoML) - ML model serving
- [Evidently](https://github.com/evidentlyai/evidently) - ML monitoring
- [Great Expectations](https://github.com/great-expectations/great_expectations) - Data validation

---

## 📚 Learning Resources

### Recommended Reading
- [Designing Machine Learning Systems](https://www.oreilly.com/library/view/designing-machine-learning/9781098107956/) by Chip Huyen
- [Building Machine Learning Powered Applications](https://www.oreilly.com/library/view/building-machine-learning/9781492045106/) by Emmanuel Ameisen
- [Machine Learning Engineering](http://www.mlebook.com/) by Andriy Burkov

### Online Courses
- [MLOps Specialization](https://www.coursera.org/specializations/machine-learning-engineering-for-production-mlops) - Coursera
- [Full Stack Deep Learning](https://fullstackdeeplearning.com/) - Free course
- [Made With ML](https://madewithml.com/) - MLOps guide

### Tutorials & Blogs
- [ml-ops.org](https://ml-ops.org/) - MLOps principles
- [FastAPI Tutorial](https://fastapi.tiangolo.com/tutorial/) - Official docs
- [Docker Tutorial](https://docs.docker.com/get-started/) - Official docs

---

## 🎓 FAQ (Frequently Asked Questions)

### General Questions

**Q: What is this project about?**
A: This is a production-ready MLOps project for predicting customer churn using machine learning. It includes a complete ML pipeline, REST API with authentication, Docker containerization, and CI/CD automation.

**Q: Who is this project for?**
A: This project is ideal for:
- Data scientists learning MLOps
- ML engineers building production systems
- Students studying machine learning deployment
- Companies needing a churn prediction solution
- Anyone interested in production ML architectures

**Q: What technologies do I need to know?**
A: Basic knowledge of:
- Python programming
- Machine learning concepts
- Docker basics (helpful but not required)
- REST APIs (helpful but not required)

---

### Setup & Installation

**Q: What are the minimum system requirements?**
A: 
- OS: Linux, macOS, or Windows 10/11 with WSL2
- RAM: 8GB minimum, 16GB recommended
- Storage: 10GB free space
- Python: 3.9 or higher
- Docker: 20.10 or higher

**Q: Can I run this without Docker?**
A: Yes! You can run everything locally using Python virtual environments. Follow the "Local Development Setup" section.

**Q: How do I handle Windows-specific issues?**
A: 
- Use WSL2 for best compatibility
- Install Docker Desktop with WSL2 backend
- Use absolute paths for volume mounting
- Run Redis through Docker

**Q: Installation fails with permission errors. What should I do?**
A:
```bash
# Fix file permissions
chmod -R 755 models/ data/

# Fix Python package permissions
pip install --user -r requirements.txt

# Run Docker without sudo (Linux)
sudo usermod -aG docker $USER
# Then logout and login again
```

---

### Authentication & Security

**Q: How do I create my first admin user?**
A:
```bash
# Run the admin creation script
make create-admin

# Or manually:
python scripts/create_admin.py
```

**Q: I forgot my password. How can I reset it?**
A: Currently, admin users can reset passwords using:
```bash
python scripts/reset_password.py --username your-username
```

**Q: How long do access tokens last?**
A: By default:
- Access tokens: 30 minutes
- Refresh tokens: 7 days
You can modify these in `.env` or `params.yaml`

**Q: Can I disable authentication for testing?**
A: Not recommended for security, but you can modify `src/api/main.py` to remove authentication dependencies. Better approach: create a test user.

**Q: How do I get a new token when mine expires?**
A:
```bash
# Use refresh token
curl -X POST "http://localhost:8000/auth/refresh" \
  -H "Authorization: Bearer $REFRESH_TOKEN"

# Or login again
curl -X POST "http://localhost:8000/auth/token" \
  -d "username=your-username&password=your-password"
```

---

### Rate Limiting

**Q: Why am I getting "Rate limit exceeded" errors?**
A: You've hit the request limit for that endpoint. Wait for the reset time (shown in response headers) or contact an admin to increase your limits.

**Q: How do I check my remaining rate limit?**
A: Check the response headers:
```
X-RateLimit-Limit: 30
X-RateLimit-Remaining: 15
X-RateLimit-Reset: 1642248000
```

**Q: Can I disable rate limiting for development?**
A:
```bash
# Set in .env
RATE_LIMIT_ENABLED=false

# Or temporarily:
export RATE_LIMIT_ENABLED=false
uvicorn src.api.main:app --reload
```

**Q: Redis connection fails. What should I do?**
A:
```bash
# Check if Redis is running
redis-cli ping

# Start Redis
make redis-start

# Or use Docker
docker run -d -p 6379:6379 redis:7-alpine
```

---

### ML Pipeline

**Q: How do I train a new model?**
A:
```bash
# Complete pipeline
make pipeline

# Or just training
make train

# Or using DVC
dvc repro
```

**Q: Where are the trained models stored?**
A: Models are saved in `models/churn_model.pkl` and tracked by DVC for versioning.

**Q: Can I use my own dataset?**
A: Yes! Place your CSV file in `data/raw/` and update `params.yaml` with the correct path and feature names.

**Q: How do I add new features to the model?**
A:
1. Update `params.yaml` with new feature names
2. Modify `src/preprocess.py` if needed
3. Update `src/api/schemas.py` for API validation
4. Retrain the model: `make train`

**Q: Model accuracy is low. How can I improve it?**
A:
- Tune hyperparameters in `params.yaml`
- Add more relevant features
- Collect more training data
- Try different algorithms
- Check for data quality issues

---

### API Usage

**Q: How do I test the API endpoints?**
A: Three ways:
1. Interactive docs: http://localhost:8000/docs
2. Using curl (see examples in README)
3. Using Python requests library

**Q: Can I make predictions without authentication?**
A: No, all prediction endpoints require authentication for security and tracking purposes.

**Q: How do I upload a CSV file for batch predictions?**
A:
```bash
curl -X POST "http://localhost:8000/predict/csv" \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@customers.csv"
```

**Q: What's the maximum batch size for predictions?**
A: By default, 1000 customers per request. You can modify this in `src/api/main.py`.

**Q: API returns 422 error. What's wrong?**
A: Validation error. Check:
- All required fields are present
- Field types match the schema
- Field names are correct (case-sensitive)

**Q: How do I get prediction history?**
A:
```bash
# Your predictions
curl "http://localhost:8000/predictions/history?limit=20" \
  -H "Authorization: Bearer $TOKEN"

# All predictions (admin only)
curl "http://localhost:8000/analytics/summary" \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

### Docker & Deployment

**Q: Docker container won't start. What should I check?**
A:
```bash
# Check logs
docker logs churn-api

# Check if port is already in use
lsof -ti:8000

# Rebuild without cache
docker-compose build --no-cache
```

**Q: How do I update the Docker image?**
A:
```bash
# Pull latest code
git pull

# Rebuild
docker-compose build

# Restart
docker-compose up -d
```

**Q: Can I deploy this to AWS/GCP/Azure?**
A: Yes! See the "Cloud Deployment Examples" section for specific instructions for each platform.

**Q: How do I scale the API for production?**
A:
```bash
# Using Docker Compose
docker-compose up -d --scale api=5

# Using Kubernetes
kubectl scale deployment/churn-api --replicas=10
```

**Q: What's the difference between the Dockerfiles?**
A:
- `Dockerfile`: Production image (optimized, minimal)
- `Dockerfile.dev`: Development image (includes dev tools)
- `Dockerfile.api`: API-specific image (includes only API dependencies)

---

### Database

**Q: Can I use PostgreSQL instead of SQLite?**
A: Yes!
```bash
# Update .env
DATABASE_URL=postgresql://user:pass@host:5432/db

# Initialize database
make db-init
```

**Q: How do I backup the database?**
A:
```bash
# SQLite
cp churn_predictions.db churn_predictions_backup.db

# PostgreSQL
pg_dump -U user churn_db > backup.sql
```

**Q: How do I view database contents?**
A:
```bash
# SQLite
sqlite3 churn_predictions.db
.tables
SELECT * FROM users;

# PostgreSQL
psql -U user -d churn_db
\dt
SELECT * FROM users;
```

**Q: Database is locked. How to fix?**
A:
```bash
# Stop all API processes
pkill -f "python.*api"

# Reset database
make db-reset
```

---

### DVC & Data Versioning

**Q: What is DVC and why use it?**
A: DVC (Data Version Control) tracks your data and models like Git tracks code. It enables:
- Version control for large files
- Reproducible experiments
- Collaboration on ML projects
- Remote storage (S3, GDrive, etc.)

**Q: How do I setup DVC with S3?**
A:
```bash
# Configure S3 remote
dvc remote add -d s3remote s3://my-bucket/dvc-storage
dvc remote modify s3remote access_key_id $AWS_KEY
dvc remote modify s3remote secret_access_key $AWS_SECRET

# Push data
dvc push
```

**Q: DVC pull fails. What should I do?**
A:
```bash
# Check remote configuration
dvc remote list
dvc config -l

# Test connection
dvc pull --verbose

# Re-configure if needed
dvc remote modify myremote url s3://new-bucket
```

**Q: How do I track a new data file?**
A:
```bash
# Add file to DVC
dvc add data/raw/new_data.csv

# Commit DVC file to Git
git add data/raw/new_data.csv.dvc .gitignore
git commit -m "Add new data"

# Push to remote storage
dvc push
```

---

### CI/CD

**Q: GitHub Actions workflow fails. Where do I look?**
A:
1. Check Actions tab in your GitHub repo
2. Review failed step logs
3. Common issues:
   - Missing secrets
   - Test failures
   - Docker build errors

**Q: How do I add GitHub secrets?**
A:
1. Go to repository Settings
2. Click "Secrets and variables" → "Actions"
3. Click "New repository secret"
4. Add name and value

**Q: Can I use GitLab CI instead of GitHub Actions?**
A: Yes! Create a `.gitlab-ci.yml` file following similar stages as the GitHub Actions workflow.

---

### Testing

**Q: How do I run tests?**
A:
```bash
# All tests
make test

# Specific test file
pytest tests/test_auth.py -v

# With coverage
make test-coverage
```

**Q: Tests fail with import errors. What's wrong?**
A:
```bash
# Install package in development mode
pip install -e .

# Or add to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

**Q: How do I write tests for new features?**
A: Follow the existing test patterns in `tests/`. Example:
```python
def test_my_new_feature():
    """Test description."""
    # Arrange
    # ... setup
    
    # Act
    # ... execute
    
    # Assert
    # ... verify
```

---

### Troubleshooting

**Q: API is slow. How can I improve performance?**
A:
1. Increase number of workers: `--workers 8`
2. Use Gunicorn for production
3. Add caching for predictions
4. Optimize model size
5. Use connection pooling for database

**Q: High memory usage. What should I do?**
A:
```bash
# Limit Docker container memory
docker run --memory="1g" churn-api

# Monitor usage
docker stats churn-api

# Optimize model
python scripts/optimize_model.py
```

**Q: Where can I find logs?**
A:
```bash
# Application logs
tail -f logs/api.log

# Docker logs
docker logs churn-api -f

# Kubernetes logs
kubectl logs -f deployment/churn-api
```

**Q: How do I report a bug?**
A:
1. Check existing issues on GitHub
2. Create new issue with:
   - Descriptive title
   - Steps to reproduce
   - Expected vs actual behavior
   - System information
   - Error logs

---

### Contributing

**Q: How can I contribute?**
A:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

**Q: What kind of contributions are welcome?**
A:
- Bug fixes
- New features
- Documentation improvements
- Performance optimizations
- Test coverage improvements
- Translation to other languages

**Q: Do I need to sign a CLA?**
A: No, this project uses the MIT license and doesn't require a CLA.

---

### Advanced Usage

**Q: Can I use this for multi-class classification?**
A: Yes! Modify:
1. `src/train.py` for multi-class model
2. `src/api/schemas.py` for response format
3. Update metrics in `src/evaluate.py`

**Q: How do I implement A/B testing for models?**
A: 
1. Train multiple models
2. Modify `src/api/main.py` to route traffic
3. Track performance metrics
4. Use analytics to compare results

**Q: Can I add email notifications?**
A: Yes! Add email functionality:
```python
# In src/api/main.py
from fastapi_mail import FastMail, MessageSchema

# Configure and send emails on events
```

**Q: How do I implement model retraining pipeline?**
A:
1. Set up scheduled job (cron/Airflow)
2. Trigger DVC pipeline: `dvc repro`
3. Evaluate new model
4. Deploy if metrics improve
5. Archive old model

---

### Getting More Help

**Q: Where can I get additional support?**
A:
- 📧 Email: Aryaganendra45@gmail.com
- 🐛 GitHub Issues: [Create an Issue](https://github.com/aryaputra03/Docker_Churn_Classifier/issues)
- 💬 GitHub Discussions: [Join Discussion](https://github.com/aryaputra03/Docker_Churn_Classifier/discussions)
- 💼 LinkedIn: [Connect](https://www.linkedin.com/in/ganendra-geanza-aryaputra-b8071a194)

**Q: Is there a community forum?**
A: Use GitHub Discussions for community questions and discussions.

**Q: Can I hire you for custom implementation?**
A: Yes! Contact via email: Aryaganendra45@gmail.com

---

## 📖 Glossary

### Technical Terms

- **MLOps**: Machine Learning Operations - practices for deploying and maintaining ML models in production
- **DVC**: Data Version Control - Git for data and models
- **FastAPI**: Modern Python web framework for building APIs
- **JWT**: JSON Web Token - secure authentication method
- **OAuth2**: Industry-standard protocol for authorization
- **Redis**: In-memory data store used for rate limiting
- **Docker**: Platform for containerizing applications
- **Kubernetes**: Container orchestration platform
- **CI/CD**: Continuous Integration/Continuous Deployment
- **REST API**: Representational State Transfer Application Programming Interface
- **SQLAlchemy**: Python SQL toolkit and ORM
- **Pydantic**: Data validation using Python type annotations
- **Uvicorn**: ASGI server for Python
- **Gunicorn**: Python WSGI HTTP server

### Business Terms

- **Churn**: When customers stop using a service
- **Churn Rate**: Percentage of customers who leave over a time period
- **Prediction**: ML model output forecasting customer behavior
- **Feature**: Input variable used by the ML model
- **Model**: Trained algorithm that makes predictions
- **Batch Processing**: Processing multiple items at once
- **Real-time Prediction**: Immediate prediction on request

---

## 🎬 Quick Reference

### Essential Commands

```bash
# Setup
make install-dev          # Install dependencies
make setup               # Initialize project
make create-admin        # Create admin user

# Development
make api-run            # Start API server
make pipeline           # Run ML pipeline
make test               # Run tests
make test-coverage      # Run tests with coverage

# Docker
docker-compose up       # Start all services
docker-compose down     # Stop all services
docker-compose logs -f  # View logs

# Database
make db-init           # Initialize database
make db-reset          # Reset database

# Redis
make redis-start       # Start Redis
redis-cli ping         # Test Redis connection

# DVC
dvc pull              # Pull data from remote
dvc repro             # Run pipeline
dvc push              # Push data to remote
```

### Important Files

```
.env                  # Environment variables (create from .env.example)
params.yaml          # ML pipeline configuration
docker-compose.yml   # Docker services configuration
requirements.txt     # Python dependencies
Makefile            # Automation commands
```

### Default Credentials

After running `make create-admin`, you'll set:
- Username: (your choice)
- Email: (your choice)
- Password: (your choice, min 8 chars with uppercase, digit, special char)

### Default Ports

```
8000  - API Server
6379  - Redis
5432  - PostgreSQL (if using)
8888  - Jupyter (development)
```

### Environment Variables Template

```bash
# .env file
SECRET_KEY=your-secret-key-here
DATABASE_URL=sqlite:///churn_predictions.db
REDIS_URL=redis://localhost:6379/0
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7
RATE_LIMIT_ENABLED=true
```

---

## 🚦 Status & Health Checks

### Check System Health

```bash
# API health
curl http://localhost:8000/health

# Expected response:
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:00:00Z",
  "version": "1.0.0",
  "database": "connected",
  "redis": "connected",
  "model": "loaded"
}
```

### Check Component Status

```bash
# Python version
python --version  # Should be 3.9+

# Docker version
docker --version  # Should be 20.10+

# Redis status
redis-cli ping  # Should return PONG

# Database status (SQLite)
sqlite3 churn_predictions.db "SELECT 1;"

# Model exists
ls -lh models/churn_model.pkl

# Check running services
docker-compose ps
```

---

## 🎯 Best Practices

### Development
1. Always work in a virtual environment
2. Follow PEP 8 style guide
3. Write tests for new features
4. Keep dependencies up to date
5. Use type hints in Python code

### Security
1. Never commit `.env` files
2. Use strong passwords
3. Rotate tokens regularly
4. Always use HTTPS in production
5. Keep dependencies updated
6. Review security advisories

### Production
1. Use PostgreSQL instead of SQLite
2. Enable all security features
3. Monitor logs and metrics
4. Set up automated backups
5. Use proper resource limits
6. Implement health checks
7. Use load balancing
8. Set up monitoring and alerting

### Performance
1. Use connection pooling
2. Implement caching where appropriate
3. Optimize database queries
4. Use async operations
5. Scale horizontally when needed
6. Monitor and profile regularly

---

<p align="center">
  <strong>Made with ❤️ for MLOps enthusiasts</strong>
</p>

<p align="center">
  <strong>Star ⭐ this repository if you find it helpful!</strong>
</p>

<p align="center">
  <a href="#-table-of-contents">Back to Top ↑</a>
</p>

---

**Last Updated**: January 2024  
**Version**: 1.0.0  
**Maintainer**: Ganendra Geanza Aryaputra ([@aryaputra03](https://github.com/aryaputra03))