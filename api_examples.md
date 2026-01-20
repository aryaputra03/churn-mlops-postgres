# 🚀 API Usage Examples

## Base URL
```
http://localhost:8000
```

## 1. Health Check

### Request
```bash
curl http://localhost:8000/health
```

### Response
```json
{
  "status": "healthy",
  "model_loaded": true,
  "timestamp": "2024-01-15T10:30:00Z"
}
```

---

## 2. Single Prediction

### Request
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "C12345",
    "gender": "Male",
    "tenure": 24,
    "monthly_charges": 75.5,
    "total_charges": 1810.0,
    "contract": "One year",
    "payment_method": "Bank transfer",
    "internet_service": "Fiber optic"
  }'
```

### Python Example
```python
import requests

url = "http://localhost:8000/predict"
data = {
    "customer_id": "C12345",
    "gender": "Male",
    "tenure": 24,
    "monthly_charges": 75.5,
    "total_charges": 1810.0,
    "contract": "One year",
    "payment_method": "Bank transfer",
    "internet_service": "Fiber optic"
}

response = requests.post(url, json=data)
print(response.json())
```

### Response
```json
{
  "customer_id": "C12345",
  "prediction": 0,
  "churn_probability": 0.25,
  "no_churn_probability": 0.75,
  "timestamp": "2024-01-15T10:30:00Z"
}
```

---

## 3. Batch Prediction

### Request
```bash
curl -X POST http://localhost:8000/predict/batch \
  -H "Content-Type: application/json" \
  -d '{
    "customers": [
      {
        "customer_id": "C001",
        "gender": "Male",
        "tenure": 24,
        "monthly_charges": 75.5,
        "total_charges": 1810.0,
        "contract": "One year",
        "payment_method": "Bank transfer",
        "internet_service": "Fiber optic"
      },
      {
        "customer_id": "C002",
        "gender": "Female",
        "tenure": 12,
        "monthly_charges": 50.0,
        "total_charges": 600.0,
        "contract": "Month-to-month",
        "payment_method": "Electronic check",
        "internet_service": "DSL"
      }
    ]
  }'
```

### Response
```json
[
  {
    "customer_id": "C001",
    "prediction": 0,
    "churn_probability": 0.25,
    "no_churn_probability": 0.75,
    "timestamp": "2024-01-15T10:30:00Z"
  },
  {
    "customer_id": "C002",
    "prediction": 1,
    "churn_probability": 0.82,
    "no_churn_probability": 0.18,
    "timestamp": "2024-01-15T10:30:01Z"
  }
]
```

---

## 4. CSV Upload Prediction

### Request
```bash
curl -X POST http://localhost:8000/predict/csv \
  -H "Content-Type: multipart/form-data" \
  -F "file=@customers.csv"
```

### Sample CSV Format
```csv
customer_id,gender,tenure,monthly_charges,total_charges,contract,payment_method,internet_service
C001,Male,24,75.5,1810.0,One year,Bank transfer,Fiber optic
C002,Female,12,50.0,600.0,Month-to-month,Electronic check,DSL
C003,Male,48,95.0,4560.0,Two year,Credit card,Fiber optic
```

---

## 5. Get Prediction History

### Request
```bash
curl http://localhost:8000/predictions/history?skip=0&limit=10
```

### Response
```json
[
  {
    "id": 1,
    "customer_id": "C12345",
    "prediction": 0,
    "probability": 0.25,
    "created_at": "2024-01-15T10:30:00Z"
  },
  {
    "id": 2,
    "customer_id": "C67890",
    "prediction": 1,
    "probability": 0.82,
    "created_at": "2024-01-15T10:29:00Z"
  }
]
```

---

## 6. Get Customer Predictions

### Request
```bash
curl http://localhost:8000/predictions/customer/C12345
```

---

## 7. Get Analytics Summary

### Request
```bash
curl http://localhost:8000/analytics/summary
```

### Response
```json
{
  "total_predictions": 1250,
  "churn_predictions": 312,
  "no_churn_predictions": 938,
  "churn_rate": 24.96,
  "avg_churn_probability": 0.68,
  "recent_predictions_24h": 45
}
```

---

## 8. Model Information

### Request
```bash
curl http://localhost:8000/model/info
```

### Response
```json
{
  "model_type": "RandomForestClassifier",
  "model_version": "1.0.0",
  "features": ["gender", "tenure", "monthly_charges", ...],
  "trained_at": "2024-01-15T08:00:00Z",
  "accuracy": 0.85
}
```

---

## Interactive API Documentation

FastAPI automatically generates interactive API documentation:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

You can test all endpoints directly from the browser!
```

---
