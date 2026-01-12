"""
Pydantic models for request validation
"""
from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime


class PostbackRequest(BaseModel):
    """Model for validating postback requests"""
    
    # Required fields
    gclid: str = Field(..., description="Google Click ID", min_length=1)
    src: str = Field(..., description="Source/Account ID", min_length=1)
    
    # Transaction details
    orderId: Optional[str] = None
    commission: Optional[float] = Field(None, ge=0, description="Commission value")
    productName: Optional[str] = None
    productId: Optional[str] = None
    dateTime: Optional[str] = None
    
    # UTM parameters
    utmSource: Optional[str] = None
    utmCampaign: Optional[str] = None
    utmMedium: Optional[str] = None
    utmContent: Optional[str] = None
    utmTerm: Optional[str] = None
    
    # Upsell info
    upsellNo: Optional[int] = Field(None, ge=0)
    
    @field_validator('src')
    @classmethod
    def validate_src(cls, v: str) -> str:
        """Validate that src contains only digits and hyphens"""
        cleaned = v.replace('-', '')
        if not cleaned.isdigit():
            raise ValueError('src must contain only digits (hyphens are allowed)')
        return cleaned  # Return without hyphens for consistency
    
    @field_validator('dateTime')
    @classmethod
    def validate_datetime(cls, v: Optional[str]) -> Optional[str]:
        """Validate datetime format if provided"""
        if v is None:
            return None
        try:
            # Try to parse the datetime to validate format
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('dateTime must be in ISO 8601 format')


class ConversionResponse(BaseModel):
    """Response model for successful conversion"""
    success: bool
    message: str
    src: str
    gclid: str
    csv_url: str


class ConversionStats(BaseModel):
    """Statistics for conversions"""
    recent: int = Field(..., description="Recent conversions (last 25 hours)")
    history: int = Field(..., description="Archived conversions")
    total: int = Field(..., description="Total conversions")


class CleanupResponse(BaseModel):
    """Response for cleanup operations"""
    success: bool
    src: str
    archived: int = Field(..., description="Number of conversions archived")
    remaining: int = Field(..., description="Number of conversions remaining")
    message: str
