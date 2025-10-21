# PayAware Backend - Wage Fairness API

Backend API for PayAware (Fair Wage Australia) - A comprehensive wage fairness analysis platform.

## Overview

This backend provides RESTful APIs for:
- Wage fairness calculations
- Gender pay gap analysis
- Suburb livability scoring
- AI-powered salary negotiation chatbot
- Wage forecasting and predictions

Technology Stack:
- Python 3.9
- AWS Lambda (Serverless)
- API Gateway
- RDS MySQL
- ZHIPU AI (ChatGLM)

## Project Structure

```
wage-fairness-australia/
├── data/                       # Data processing scripts and utilities
├── pymysql/                    # MySQL connector library
├── AI_chat.py                  # AI chatbot Lambda handler
├── forecast.py                 # Wage forecasting handlers (predict & compare)
├── gender_gap_handler.py       # Gender pay gap analysis handler
├── handler.py                  # Main wage fairness calculation handler
├── suburb_scoring_handler.py   # Suburb livability scoring handler
├── serverless.yml              # Infrastructure as Code (Lambda, API Gateway)
├── requirements.txt            # Python dependencies
├── package.json                # Node.js dependencies (Serverless Framework)
├── test_wage.py                # Unit tests for wage calculations
├── test_gender.py              # Unit tests for gender gap analysis
├── chattest.py                 # Manual testing for AI chat
├── forcasttest.py              # Manual testing for forecasting
├── subtest.py                  # Manual testing for suburb scoring
└── README.md                   # This file
```

## Prerequisites

- Python 3.9 (required)
- Node.js 20 LTS (for Serverless Framework)
- AWS CLI (configured with credentials)
- Serverless Framework >= 3.38.0

## Installation

### 1. Clone Repository

```bash
git clone https://github.com/Yiming1107/wage-fairness-australia.git
cd wage-fairness-australia
```

### 2. Install Dependencies

**Python packages:**
```bash
pip install -r requirements.txt --break-system-packages
```

**Node.js packages:**
```bash
npm install
```

**Serverless Framework (if not installed globally):**
```bash
npm install -g serverless
```

### 3. Configure AWS Credentials

```bash
aws configure
# Enter your AWS Access Key ID, Secret Key, and Region (ap-southeast-2)
```

## Deployment

### Deploy All Functions

```bash
serverless deploy --stage dev
```

**Expected Output:**
```
✔ Service deployed to stack wage-fairness-api-dev

endpoints:
  POST - https://1qbm73da9b.execute-api.ap-southeast-2.amazonaws.com/dev/fairness/calculate
  POST - https://1qbm73da9b.execute-api.ap-southeast-2.amazonaws.com/dev/gender-gap/calculate
  POST - https://1qbm73da9b.execute-api.ap-southeast-2.amazonaws.com/dev/suburb/score
  POST - https://1qbm73da9b.execute-api.ap-southeast-2.amazonaws.com/dev/ai/chat
  POST - https://1qbm73da9b.execute-api.ap-southeast-2.amazonaws.com/dev/forecast/predict
  POST - https://1qbm73da9b.execute-api.ap-southeast-2.amazonaws.com/dev/forecast/compare

functions:
  calculate: wage-fairness-api-dev-calculate
  gender-gap: wage-fairness-api-dev-gender-gap
  suburb-score: wage-fairness-api-dev-suburb-score
  ai-chat: wage-fairness-api-dev-ai-chat
  forecast-predict: wage-fairness-api-dev-forecast-predict
  forecast-compare: wage-fairness-api-dev-forecast-compare
```

### Deploy Single Function (Quick Updates)

```bash
serverless deploy function --function calculate --stage dev
```

### View Deployment Info

```bash
serverless info --stage dev
```

## API Endpoints

Base URL: `https://1qbm73da9b.execute-api.ap-southeast-2.amazonaws.com/dev`

### 1. Calculate Wage Fairness

**Endpoint:** `POST /fairness/calculate`

**Request:**
```json
{
  "occupation": "Software Engineer",
  "experience": 5,
  "location": "Sydney",
  "hourlyRate": 50
}
```

**Response:**
```json
{
  "fairnessScore": 85,
  "marketRate": 52,
  "recommendation": "Your wage is fair"
}
```

### 2. Gender Pay Gap Analysis

**Endpoint:** `POST /gender-gap/calculate`

**Request:**
```json
{
  "industry": "IT",
  "occupation": "Software Engineer"
}
```

### 3. Suburb Livability Score

**Endpoint:** `POST /suburb/score`

**Request:**
```json
{
  "suburb": "Melbourne",
  "state": "VIC"
}
```

### 4. AI Chat

**Endpoint:** `POST /ai/chat`

**Request:**
```json
{
  "message": "How can I negotiate my salary?",
  "conversationHistory": []
}
```

### 5. Wage Forecast

**Endpoint:** `POST /forecast/predict`

**Request:**
```json
{
  "occupation": "Software Engineer",
  "years": 5
}
```

### 6. Compare Scenarios

**Endpoint:** `POST /forecast/compare`

**Request:**
```json
{
  "scenario1": {...},
  "scenario2": {...}
}
```

## Testing

### Run Unit Tests

```bash
python test_wage.py          # Test wage fairness calculations
python test_gender.py        # Test gender gap analysis
```

### Manual Testing

```bash
python chattest.py           # Test AI chatbot
python forcasttest.py        # Test forecasting
python subtest.py            # Test suburb scoring
```

### API Testing with cURL

```bash
curl -X POST https://1qbm73da9b.execute-api.ap-southeast-2.amazonaws.com/dev/fairness/calculate \
  -H "Content-Type: application/json" \
  -d '{
    "occupation": "Software Engineer",
    "experience": 5,
    "location": "Sydney",
    "hourlyRate": 50
  }'
```

## Monitoring and Logs

### View Live Logs

```bash
serverless logs --function calculate --tail
```

### CloudWatch Log Groups

- `/aws/lambda/wage-fairness-api-dev-calculate`
- `/aws/lambda/wage-fairness-api-dev-gender-gap`
- `/aws/lambda/wage-fairness-api-dev-suburb-score`
- `/aws/lambda/wage-fairness-api-dev-ai-chat`
- `/aws/lambda/wage-fairness-api-dev-forecast-predict`
- `/aws/lambda/wage-fairness-api-dev-forecast-compare`

## Database Configuration

RDS MySQL:
- Host: `fairwageaustralia.ct08osmucf2b.ap-southeast-2.rds.amazonaws.com`
- Port: `3306`
- Database: `fairwageaustralia`
- User: `admin`
- Password: Configured in `serverless.yml`

Connection is managed automatically by Lambda functions.

## Rollback

If deployment causes issues:

```bash
# View previous deployments
serverless deploy list --stage dev

# Rollback to specific timestamp
serverless rollback --timestamp 2025-01-14T10:30:00 --stage dev
```

## Environment Variables

Configured in `serverless.yml`:

| Variable | Description |
|----------|-------------|
| `DB_HOST` | RDS MySQL endpoint |
| `DB_PORT` | MySQL port (3306) |
| `DB_USER` | Database username |
| `DB_NAME` | Database name |
| `DB_PASSWORD` | Database password |
| `ZHIPU_API_KEY` | AI chatbot API key |
| `MAX_REQUEST_SIZE` | Maximum request size |
| `MAX_CONVERSATION_LENGTH` | Max chat history |

## Security Notes

Important: 
- Database credentials are currently stored in `serverless.yml` (not recommended for production)
- Migrate to AWS Secrets Manager for production deployment
- API currently allows all CORS origins - restrict to production domain

## Dependencies

Python Packages:
- `pymysql==1.1.0` - MySQL connector
- `zhipuai` - ZHIPU AI SDK
- `pandas` - Data manipulation
- `numpy` - Numerical computing
- `scikit-learn` - Machine learning

Node.js Packages:
- `serverless` - Serverless Framework
- `serverless-python-requirements` - Python packaging plugin

## Troubleshooting

### Common Issues

1. Deployment fails with "credentials not found"
```bash
aws configure
# Re-enter your AWS credentials
```

2. Function timeout
- Increase timeout in `serverless.yml`
- Check database connection

3. Module import error
- Ensure all dependencies in `requirements.txt`
- Redeploy: `serverless deploy --stage dev`

4. Database connection error
- Verify RDS is running
- Check security group allows Lambda access
- Verify credentials in `serverless.yml`

## Support

- GitHub Issues: https://github.com/Yiming1107/wage-fairness-australia/issues
- AWS Support: https://console.aws.amazon.com/support/
- Documentation: See project wiki

## License

This project is developed as part of FIT5120 - Industry Experience Studio Project at Monash University.

## Team

- Backend Development Team
- Monash University FIT5120 - 2024/2025

## Related Repositories

- Frontend: https://github.com/cpet0023-ai/Final-Project
- Documentation: See Support & Maintenance documents

---

Last Updated: January 2025  
Version: 1.0.0