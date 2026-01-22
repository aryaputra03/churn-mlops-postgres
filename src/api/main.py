"""
FastAPI Main Application

REST API for customer churn prediction with database logging.
Updated to use modern FastAPI lifespan events.
"""

from dotenv import load_dotenv
load_dotenv()

from contextlib import asynccontextmanager
from fastapi import (
    FastAPI, HTTPException, Depends, BackgroundTasks, 
    Query, Request, status
)
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from typing import List
import pandas as pd
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordRequestForm
from slowapi.errors import RateLimitExceeded

from src.api.models import (
    PredictionRequest,
    PredictionResponse,
    BatchPredictionRequest,
    HealthResponse,
    ModelInfoResponse,
    PredictionHistoryResponse,
    UserCreate,
    UserResponse,
    Token,
    UserUpdate
)

from src.api.database import get_db, engine, Base
from src.api import crud, schemas
from src.api.auth import (
    authenticate_user,
    create_access_token,
    create_refresh_token,
    get_current_active_user,
    require_role,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from src.api.rate_limit import limiter, _rate_limit_exceeded_handler
from src.api.ml_service import MLService
from src.utils import logger
from sqlalchemy.orm import Session

import os
from src.api.database import init_db, get_pool_status

# ==========================================
# Lifespan Event Handler
# ==========================================

ml_service = MLService()

async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Churn Prediction API v2.0...")
    Base.metadata.create_all(bind=engine)
    
    try:
        init_db()
        logger.info("Database initialized")
        pool_status = get_pool_status()
        logger.info(f"Connection Pool: {pool_status}")
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        raise
    
    try:
        ml_service.load_model()
        logger.info("Model loaded successfully")
    except Exception as e:
        logger.critical(f"Failed to load model: {str(e)}")
        raise RuntimeError(f"Model loading failed: {str(e)}")
    
    logger.info("Application started successfully")
    yield
    
    # Shutdown
    logger.info("Shutting down...")

# ==========================================
# FastAPI App Initialization
# ==========================================

app = FastAPI(
    title="Churn Prediction API",
    description="ML API with Supabase PostgreSQL, Authentication & Rate Limiting",
    version="3.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

CORS_ORIGIN = os.getenv("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGIN,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# Authentication Endpoints
# ==========================================

@app.post(
    "/auth/register", 
    response_model=UserResponse, 
    status_code=status.HTTP_201_CREATED,
    tags=["Authentication"]
)
@limiter.limit("5/hour")
async def register(
    request: Request,
    user: UserCreate,
    db: Session = Depends(get_db)
):
    """
    Register new user
    
    Rate limited to 5 registrations per hour per IP
    """
    if crud.get_user_by_username(db, user.username):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )
    
    if crud.get_user_by_email(db, user.email):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    db_user = crud.create_user(
        db=db,
        username=user.username,
        email=user.email,
        password=user.password,
        full_name=user.full_name
    )

    logger.info(f"New user registered: {user.username}")
    return db_user


@app.post("/auth/token", response_model=Token, tags=["Authentication"])
@limiter.limit("10/minute")
async def login(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    """
    Login and get access token
    
    OAuth2 compatible token login, get an access token for future requests
    Rate limited to 10 attempts per minute
    """
    user = authenticate_user(db, form_data.username, form_data.password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username},
        expires_delta=access_token_expires
    )
    refresh_token = create_refresh_token(data={"sub": user.username})

    logger.info(f"User logged in: {user.username}")

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expire_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }


@app.get("/auth/me", response_model=UserResponse, tags=["Authentication"])
async def read_users_me(
    current_user: schemas.User = Depends(get_current_active_user)
):
    """Get current user information - Requires authentication"""
    return current_user


@app.put("/auth/me", response_model=UserResponse, tags=["Authentication"])
async def update_user_me(
    user_update: UserUpdate,
    current_user: schemas.User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Update current user information - Requires authentication"""
    updated_user = crud.update_user(
        db=db,
        user_id=current_user.id,
        **user_update.model_dump(exclude_unset=True)
    )
    return updated_user


@app.get("/auth/users", response_model=List[UserResponse], tags=["Authentication"])
async def list_users(
    skip: int = 0,
    limit: int = 100,
    current_user: schemas.User = Depends(require_role("admin")),
    db: Session = Depends(get_db)
):
    """List all users (Admin only) - Requires admin role"""
    users = crud.get_users(db, skip=skip, limit=limit)
    return users


# ==========================================
# Health & Info Endpoints
# ==========================================

@app.get("/", response_model=dict, tags=["System"])
@limiter.limit("2/minute")
async def root(request: Request):
    """Root endpoint"""
    return {
        "message": "Churn Prediction API v2.0",
        "features": ["Authentication", "Rate Limiting", "ML Predictions"],
        "docs": "/docs",
        "auth": "/auth/token"
    }


@app.get("/health", response_model=HealthResponse, tags=["System"])
@limiter.limit("100/minute")
async def health_check(request: Request):
    """
    Health check endpoint with database status
    
    Returns API health, model status, and database connection
    """ 
    try:
        with engine.connect() as conn:
            conn.execute("SELECT 1")
            db_status = "connected"
        
        pool_status = get_pool_status()
        model_loaded = ml_service.is_model_loaded()
        return HealthResponse(
            status='healthy' if model_loaded else 'unhealthy',
            model_loaded=model_loaded,
            database= {
                "status": db_status,
                "type": "postgresql" if "postgresql" in str(engine.url) else "sqlite"
            },
            timestamp=datetime.utcnow()
        )
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return HealthResponse(
            status="unhealthy",
            model_loaded=False,
            timestamp=datetime.utcnow(),
            error=str(e)
        )


@app.get("/model/info", response_model=ModelInfoResponse, tags=["System"])
async def model_info():
    """Get model information"""
    try:
        info = ml_service.get_model_info()
        return ModelInfoResponse(**info)
    except Exception as e:
        logger.error(f"Model info error: {str(e)}")
        return ModelInfoResponse(
            model_type="unknown",
            model_version="unknown",
            features=[],
            trained_at=None,
            accuracy=None
        )

@app.get("/db/status")
async def database_status(
    current_user = Depends(require_role("admin"))
):
    """
    Database connection pool status (Admin only)
    
    Returns detailed database connection information
    """
    try:
        pool_status = get_pool_status()

        return {
            "database_type": "postgresql",
            "connection_pool": pool_status,
            "timestamp" : datetime.utcnow()
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get database status: {str(e)}"
        )

# ==========================================
# Protected Prediction Endpoints
# ==========================================

@app.post("/predict", response_model=PredictionResponse, tags=["Predictions"])
@limiter.limit("30/minute")
async def predict_single(
    request: Request,
    pred_request: PredictionRequest,
    background_tasks: BackgroundTasks,
    current_user: schemas.User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    # ✅ Lazy loading
    if not ml_service.is_model_loaded():
        try:
            ml_service.load_model()
        except Exception as e:
            raise HTTPException(
                status_code=503,
                detail=f"Model not available: {str(e)}"
            )
    
    try:
        input_data = pd.DataFrame([pred_request.dict()])
        prediction, probability = ml_service.predict(input_data)

        response = PredictionResponse(
            customer_id=pred_request.customer_id,
            prediction=int(prediction[0]),
            churn_probability=float(probability[0][1]),
            no_churn_probability=float(probability[0][0]),
            timestamp=datetime.utcnow()
        )

        background_tasks.add_task(
            crud.create_prediction_log,
            db=db,
            customer_id=pred_request.customer_id,
            prediction=response.prediction,
            probability=response.churn_probability,
            input_data=pred_request.dict(),
            user_id=current_user.id
        )
        return response
    
    except Exception as e:
        logger.error(f"Prediction error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Prediction failed: {str(e)}"
        )


@app.post(
    "/predict/batch", 
    response_model=List[PredictionResponse],
    tags=["Predictions"]
)
@limiter.limit("10/hour")
async def predict_batch(
    request: Request,
    batch_request: BatchPredictionRequest,
    background_tasks: BackgroundTasks,
    current_user: schemas.User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Predict churn for multiple customers"""
    try:
        customer_data = [customer.dict() for customer in batch_request.customers]
        input_data = pd.DataFrame(customer_data)

        prediction, probabilities = ml_service.predict(input_data)

        responses = []
        for i, customer in enumerate(batch_request.customers):
            response = PredictionResponse(
                customer_id=customer.customer_id,
                prediction=int(prediction[i]),
                churn_probability=float(probabilities[i][1]),
                no_churn_probability=float(probabilities[i][0]),
                timestamp=datetime.utcnow()
            )
            responses.append(response)

            background_tasks.add_task(
                crud.create_prediction_log,
                db=db,
                customer_id=customer.customer_id,
                prediction=response.prediction,
                probability=response.churn_probability,
                input_data=customer.dict(),
                user_id=current_user.id
            )

        return responses
    
    except Exception as e:
        logger.error(f"Batch prediction error: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Batch prediction failed: {str(e)}"
        )


# ==========================================
# History & Analytics Endpoints
# ==========================================

@app.get(
    "/predictions/history", 
    response_model=List[PredictionHistoryResponse],
    tags=["History"]
)
async def get_prediction_history(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, le=1000, ge=1),
    db: Session = Depends(get_db),
    current_user: schemas.User = Depends(get_current_active_user)
):
    """Get prediction history from database"""
    try:
        predictions = crud.get_prediction(
            db, 
            current_user.id, 
            skip=skip, 
            limit=limit
        )
        return predictions
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/predictions/customer/{customer_id}", 
    response_model=List[PredictionHistoryResponse],
    tags=["History"]
)
async def get_customer_prediction(
    customer_id: str,
    db: Session = Depends(get_db)
):
    """Get prediction history for specific customer"""
    try:
        predictions = crud.get_customer_predictions(db, customer_id)
        if not predictions:
            raise HTTPException(status_code=404, detail="Customer not found")
        return predictions
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/summary", tags=["Analytics"])
async def get_analytics_summary(db: Session = Depends(get_db)):
    """Get prediction analytics summary"""
    try:
        stats = crud.get_prediction_statistics(db)
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# Model Management Endpoints
# ==========================================

@app.post("/model/reload", tags=["System"])
async def reload_model():
    """Reload ML model"""
    try:
        ml_service.load_model()
        logger.info("Model loaded successfully")
    except Exception as e:
        logger.critical(f"Failed to load model: {str(e)}")
        raise RuntimeError("Model failed to load, shutting down application")



# ==========================================
# Main Entry Point
# ==========================================

if __name__ == "__main__":
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )