"""
Configuration settings for the application
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Cloudflare R2 Configuration
    R2_ACCOUNT_ID: str
    R2_ACCESS_KEY_ID: str
    R2_SECRET_ACCESS_KEY: str
    R2_BUCKET_NAME: str
    
    # API Security
    API_KEY: str
    
    # Dashboard Authentication
    DASHBOARD_USERNAME: str
    DASHBOARD_PASSWORD: str 
    
    # Conversion Settings
    CONVERSION_NAME: str = "purchase"
    CURRENCY: str = "BRL"
    TIMEZONE: str = "America/Sao_Paulo"
    
    # Application Settings
    APP_NAME: str = "Google Ads Offline Conversions"
    DEBUG: bool = False
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Global settings instance
settings = Settings()
