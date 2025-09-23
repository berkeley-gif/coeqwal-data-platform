# COEQWAL Backend

A comprehensive backend system for COEQWAL scenario data and analytics.

## 🏗️ Repository Structure

```
coeqwal-backend/
├── 📊 database/
│   ├── schema/              # ERD and table definitions
│   ├── seed_tables/         # Initial data for lookup tables  
│   └── utils/               # Currently db audit lambdas
├── 🔄 etl/
│   ├── coeqwal-etl/         # DSS extraction and validation
│   └── lambda-trigger/      # AWS lambda trigger on model-run upload
├── 🌐 api/
│   └── coeqwal-api/         # FastAPI and web services
├── ⚙️ config/
│   └── environments/        # Centralized config management (underused currently)
```

## 🚀 Quick start

### Prerequisites
- Python 3.10+
- PostgreSQL 13+
- AWS CLI configured
- Docker (optional)

### Setup
```bash
# Clone and setup
git clone <repository-url>
cd coeqwal-backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```
