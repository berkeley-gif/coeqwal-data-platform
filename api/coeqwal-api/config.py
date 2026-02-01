"""
Configuration for COEQWAL API
"""

import os
from typing import List


class Settings:
    # Database
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql://username:password@hostname:port/database"
    )

    # API Settings
    API_TITLE: str = "COEQWAL API"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "API for COEQWAL California CalSim run data and related data"

    # CORS Settings
    ALLOWED_ORIGINS: List[str] = [
        "http://localhost:3000",  # Next.js development
        "http://localhost:3001",  # Alternative port
        "http://localhost:3002",  # Alternative port
        "http://localhost:3003",  # Alternative port
        "http://localhost:3004",  # Alternative port
        "https://dev.coeqwal.org"  # Development frontend
        "https://staging.coeqwal.org"  # Staging frontend
        "https://coeqwal.org",  # Development frontend
    ]

    # Pagination
    DEFAULT_PAGE_SIZE: int = 100
    MAX_PAGE_SIZE: int = 10000  # Increased for large datasets

    # Network Analysis
    DEFAULT_MAX_DEPTH: int = 3
    MAX_TRAVERSAL_DEPTH: int = 10


settings = Settings()
