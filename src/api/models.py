"""
Pydantic Models for API Request/Response

Updated to use Pydantic V2 syntax with proper validation
"""

from pydantic import BaseModel, EmailStr, Field, ConfigDict
from typing import Optional, List, Dict, Any
from pydantic import validator
from datetime import datetime
import re


# ==========================================
# Authentication Models
# ==========================================

class UserCreate(BaseModel):
    """User registration model"""
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=8)
    full_name: Optional[str] = None
    
    @validator('password')
    @classmethod
    def validate_password(cls, v: str) -> str:
        """Validate password strength"""
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not re.search(r'\d', v):
            raise ValueError('Password must contain at least one digit')
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
            raise ValueError('Password must contain at least one special character')
        return v
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "username": "johndoe",
                "email": "john@example.com",
                "password": "SecurePass123!",
                "full_name": "John Doe"
            }
        }
    )


class UserUpdate(BaseModel):
    """User update model"""
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    password: Optional[str] = None
    
    @validator('password')
    @classmethod
    def validate_password(cls, v: Optional[str]) -> Optional[str]:
        """Validate password strength if provided"""
        if v is None:
            return v
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not re.search(r'\d', v):
            raise ValueError('Password must contain at least one digit')
        return v


class UserResponse(BaseModel):
    """User response model"""
    id: int
    username: str
    email: str
    full_name: Optional[str] = None
    is_active: bool
    role: str
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)


class Token(BaseModel):
    """JWT token response"""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expire_in: int
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "token_type": "bearer",
                "expire_in": 1800
            }
        }
    )


class TokenData(BaseModel):
    """Token payload data"""
    username: Optional[str] = None


# ==========================================
# Prediction Models - WITH VALIDATION
# ==========================================

class PredictionRequest(BaseModel):
    """Single prediction request with field validation"""
    customer_id: str = Field(..., description="Unique customer identifier")
    gender: str = Field(..., description="Customer gender (Male/Female)")
    tenure: int = Field(..., ge=0, le=100, description="Months as customer")
    monthly_charges: float = Field(..., gt=0, description="Monthly charges")
    total_charges: float = Field(..., ge=0, description="Total charges to date")
    contract: str = Field(..., description="Contract type")
    payment_method: str = Field(..., description="Payment method")
    internet_service: str = Field(..., description="Internet service type")
    
    @validator('gender')
    @classmethod
    def validate_gender(cls, v: str) -> str:
        """Validate gender field"""
        allowed = ['Male', 'Female']
        if v not in allowed:
            raise ValueError(f'Gender must be one of {allowed}, got: {v}')
        return v
    
    @validator('contract')
    @classmethod
    def validate_contract(cls, v: str) -> str:
        """Validate contract field"""
        allowed = ['Month-to-month', 'One year', 'Two year']
        if v not in allowed:
            raise ValueError(f'Contract must be one of {allowed}, got: {v}')
        return v
    
    @validator('internet_service')
    @classmethod
    def validate_internet_service(cls, v: str) -> str:
        """Validate internet service field"""
        allowed = ['DSL', 'Fiber optic', 'No']
        if v not in allowed:
            raise ValueError(f'Internet service must be one of {allowed}, got: {v}')
        return v
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "customer_id": "CUST001",
                "gender": "Male",
                "tenure": 24,
                "monthly_charges": 75.50,
                "total_charges": 1810.00,
                "contract": "One year",
                "payment_method": "Bank transfer (automatic)",
                "internet_service": "Fiber optic"
            }
        }
    )


class PredictionResponse(BaseModel):
    """Prediction response"""
    customer_id: str
    prediction: int = Field(..., description="0=No Churn, 1=Churn")
    churn_probability: float = Field(..., ge=0, le=1)
    no_churn_probability: float = Field(..., ge=0, le=1)
    timestamp: datetime
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "customer_id": "CUST001",
                "prediction": 1,
                "churn_probability": 0.75,
                "no_churn_probability": 0.25,
                "timestamp": "2024-01-15T10:30:00"
            }
        }
    )


class BatchPredictionRequest(BaseModel):
    """Batch prediction request"""
    customers: List[PredictionRequest]
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "customers": [
                    {
                        "customer_id": "CUST001",
                        "gender": "Male",
                        "tenure": 24,
                        "monthly_charges": 75.50,
                        "total_charges": 1810.00,
                        "contract": "One year",
                        "payment_method": "Bank transfer (automatic)",
                        "internet_service": "Fiber optic"
                    }
                ]
            }
        }
    )


# ==========================================
# History & Analytics Models
# ==========================================

class PredictionHistoryResponse(BaseModel):
    """Prediction history response"""
    id: int
    customer_id: str
    prediction: int
    probability: float
    input_data: Dict[str, Any]
    created_at: datetime
    user_id: Optional[int] = None
    
    model_config = ConfigDict(from_attributes=True)

    class Config:
        orm_mode = True


# ==========================================
# Health & Info Models
# ==========================================

class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    model_loaded: bool = Field(alias="model_loaded")
    timestamp: datetime
    error: Optional[str] = None
    
    model_config = ConfigDict(
        populate_by_name=True,
        protected_namespaces=(),  # Allow 'model_' prefix
        json_schema_extra={
            "example": {
                "status": "healthy",
                "model_loaded": True,
                "timestamp": "2024-01-15T10:30:00",
                "error": None
            }
        }
    )


class ModelInfoResponse(BaseModel):
    """Model information response"""
    model_type: str = Field(alias="model_type")
    model_version: str = Field(alias="model_version")
    features: List[str]
    trained_at: Optional[datetime] = None
    accuracy: Optional[float] = None
    
    model_config = ConfigDict(
        populate_by_name=True,
        protected_namespaces=(),  # Allow 'model_' prefix
        json_schema_extra={
            "example": {
                "model_type": "RandomForestClassifier",
                "model_version": "1.0.0",
                "features": ["tenure", "monthly_charges", "total_charges"],
                "trained_at": "2024-01-15T10:30:00",
                "accuracy": 0.85
            }
        }
    )