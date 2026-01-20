# 🔐 Authentication & Rate Limiting Guide

## Quick Start

### 1. Generate Secret Key
```bash
# Generate secure secret key
make generate-secret

# Add to .env file
echo "SECRET_KEY=your-generated-key" >> .env
```

### 2. Start Services
```bash
# Start API with Redis
docker-compose up api redis

# Or locally
export SECRET_KEY="your-secret-key"
export REDIS_URL="redis://localhost:6379/0"
uvicorn src.api.main:app --reload
```

### 3. Create Admin User
```bash
# Using script
python scripts/create_admin.py

# Or using Make
make create-admin
```

---

## Authentication Flow

### Registration
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

### Login
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

### Using Access Token
```bash
# Set token
TOKEN="your-access-token"

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

### Get Current User
```bash
curl -X GET "http://localhost:8000/auth/me" \
  -H "Authorization: Bearer $TOKEN"
```

---

## Rate Limiting

### Default Limits

| Endpoint | Limit | Description |
|----------|-------|-------------|
| `/` | 60/minute | Root endpoint |
| `/health` | 100/minute | Health check |
| `/auth/register` | 5/hour | New registrations |
| `/auth/token` | 10/minute | Login attempts |
| `/predict` | 30/minute | Single predictions |
| `/predict/batch` | 10/hour | Batch predictions |

### Rate Limit Headers

Responses include rate limit information:
```
X-RateLimit-Limit: 30
X-RateLimit-Remaining: 25
X-RateLimit-Reset: 1642248000
```

### Rate Limit Exceeded

**Response (429):**
```json
{
  "error": "Rate limit exceeded",
  "detail": "30 per 1 minute"
}
```

---

## User Roles

### Available Roles
- **user**: Regular user (default)
- **admin**: Administrator with full access

### Role-Based Endpoints

#### Admin Only
```bash
# List all users (admin only)
curl -X GET "http://localhost:8000/auth/users" \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

---

## Security Best Practices

### 1. Secret Key
```bash
# Generate cryptographically secure key
python -c "import secrets; print(secrets.token_urlsafe(32))"

# Store in environment variable
export SECRET_KEY="your-generated-key"

# Or in .env file
SECRET_KEY=your-generated-key
```

### 2. Password Requirements
- Minimum 8 characters
- At least one uppercase letter
- At least one digit
- No common passwords

### 3. HTTPS in Production
```python
# Use HTTPS in production
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

### 4. CORS Configuration
```python
# In production, specify allowed origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],  # Not "*"
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)
```

---

## Redis Setup

### Local Redis
```bash
# Install Redis
# macOS
brew install redis
brew services start redis

# Ubuntu/Debian
sudo apt-get install redis-server
sudo systemctl start redis

# Test connection
redis-cli ping
```

### Docker Redis
```bash
# Start Redis container
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Check Redis
docker exec -it redis redis-cli ping
```

### Redis Configuration
```bash
# Set in environment
export REDIS_URL="redis://localhost:6379/0"

# Or in .env
REDIS_URL=redis://localhost:6379/0
```

---

## Testing

### Test Authentication
```bash
# Run auth tests
pytest tests/test_auth.py -v

# Test specific function
pytest tests/test_auth.py::test_register_user -v
```

### Test Rate Limiting
```bash
# Run rate limit tests
pytest tests/test_rate_limit.py -v
```

### Manual Testing
```bash
# Test rate limit
for i in {1..70}; do
  curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8000/
done | sort | uniq -c

# Should show mix of 200 and 429 responses
```

---

## Troubleshooting

### Token Expired
```json
{
  "detail": "Could not validate credentials"
}
```
**Solution**: Login again to get new token

### Rate Limit Issues
```bash
# Check Redis connection
redis-cli ping

# View rate limit keys
redis-cli keys "rate_limit:*"

# Clear rate limits
redis-cli flushdb
```

### Database Errors
```bash
# Reset database
make db-reset

# Recreate admin
make create-admin
```

---

## Production Deployment

### Environment Variables
```bash
# Required
SECRET_KEY=your-production-secret-key-min-32-chars
DATABASE_URL=postgresql://user:pass@host/db
REDIS_URL=redis://prod-redis:6379/0

# Optional
ACCESS_TOKEN_EXPIRE_MINUTES=15
RATE_LIMIT_ENABLED=true
CORS_ORIGINS=https://yourdomain.com
```

### Docker Deployment
```bash
# Build image
docker build -t churn-api:prod -f docker/Dockerfile.api .

# Run with environment
docker run -d \
  --name churn-api \
  -p 8000:8000 \
  -e SECRET_KEY=$SECRET_KEY \
  -e DATABASE_URL=$DATABASE_URL \
  -e REDIS_URL=$REDIS_URL \
  churn-api:prod
```

### Health Monitoring
```bash
# Check health
curl http://localhost:8000/health

# Check metrics
curl http://localhost:8000/analytics/summary \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```
```

---

## 🎉 Summary

Sekarang API Anda memiliki:

✅ **JWT Authentication** - Secure token-based auth
✅ **OAuth2 Password Flow** - Standard authentication
✅ **User Management** - Registration, login, profile
✅ **Role-Based Access** - User/Admin roles
✅ **Rate Limiting** - Prevent API abuse
✅ **Redis Integration** - Distributed rate limiting
✅ **Password Hashing** - Bcrypt encryption
✅ **Token Refresh** - Long-lived sessions
✅ **Request Tracking** - User activity monitoring
✅ **Comprehensive Tests** - Auth & rate limit tests

### Quick Commands:
```bash
# Setup
make generate-secret
make create-admin
make redis-start

# Run API
make api-run

# Test
make test-auth
make test-rate-limit