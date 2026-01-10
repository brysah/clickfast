"""
CSV Handler for managing conversion data and Cloudflare R2 storage
"""
import boto3
from botocore.client import Config
from io import StringIO, BytesIO
import csv
from datetime import datetime
from typing import List, Dict, Optional
import pytz
from config import settings


class R2Storage:
    """Handles Cloudflare R2 storage operations"""
    
    def __init__(self):
        """Initialize R2 client"""
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f'https://{settings.R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
            aws_access_key_id=settings.R2_ACCESS_KEY_ID,
            aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
            config=Config(signature_version='s3v4'),
            region_name='auto'
        )
        self.bucket_name = settings.R2_BUCKET_NAME
        
    def get_csv(self, ctid: str) -> Optional[str]:
        """
        Retrieve CSV content from R2
        
        Args:
            ctid: Customer ID
            
        Returns:
            CSV content as string, or None if file doesn't exist
        """
        key = f"{ctid}.csv"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read().decode('utf-8')
        except self.s3_client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error retrieving CSV for {ctid}: {e}")
            return None
    
    def save_csv(self, ctid: str, csv_content: str) -> bool:
        """
        Save CSV content to R2
        
        Args:
            ctid: Customer ID
            csv_content: CSV content as string
            
        Returns:
            True if successful, False otherwise
        """
        key = f"{ctid}.csv"
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            return True
        except Exception as e:
            print(f"Error saving CSV for {ctid}: {e}")
            return False
    
    def get_public_url(self, ctid: str) -> str:
        """
        Generate public URL for CSV file with API key in path
        Compatible with Google Ads (ends in .csv)
        
        Args:
            ctid: Customer ID
            
        Returns:
            Public URL to access the CSV (format: /csv/{api_key}/{ctid}.csv)
        """
        # For Railway or production, you'll need to set APP_URL environment variable
        # Example: https://seu-app.railway.app
        app_url = getattr(settings, 'APP_URL', 'http://localhost:8000')
        
        # Build URL with API key in path and .csv extension
        url = f"{app_url}/csv/{settings.API_KEY}/{ctid}.csv"
        
        return url


class CSVHandler:
    """Handles CSV operations for Google Ads conversions"""
    
    def __init__(self):
        """Initialize CSV handler with R2 storage"""
        self.storage = R2Storage()
        self.timezone = pytz.timezone(settings.TIMEZONE)
    
    def add_conversion(self, ctid: str, gclid: str, conversion_time: str, 
                      conversion_value: Optional[float] = None) -> bool:
        """
        Add a conversion to the CSV file
        
        Args:
            ctid: Customer ID
            gclid: Google Click ID
            conversion_time: ISO 8601 datetime string
            conversion_value: Optional conversion value
            
        Returns:
            True if successful, False otherwise
        """
        # Parse and format conversion time
        try:
            dt = datetime.fromisoformat(conversion_time.replace('Z', '+00:00'))
            # Convert to configured timezone
            dt_local = dt.astimezone(self.timezone)
            # Google Ads requires format: yyyy-MM-dd HH:mm:ss (with timezone in Parameters)
            formatted_time = dt_local.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"Error parsing datetime: {e}")
            # Use current time as fallback
            dt_local = datetime.now(self.timezone)
            formatted_time = dt_local.strftime('%Y-%m-%d %H:%M:%S')
        
        # Get existing CSV or create new one
        existing_csv = self.storage.get_csv(ctid)
        
        if existing_csv:
            # Append to existing CSV
            lines = existing_csv.strip().split('\n')
            # Remove Parameters:TimeZone if present, keep only header and data
            header_lines = [line for line in lines if line.startswith('Google Click ID')]
            data_lines = [line for line in lines if not line.startswith('Parameters:') and not line.startswith('Google Click ID') and line.strip()]
            value = str(conversion_value) if conversion_value is not None else ""
            new_row = f"{gclid},{settings.CONVERSION_NAME},{formatted_time},{value},{settings.CURRENCY}"
            csv_content = '\n'.join(header_lines) + '\n' + '\n'.join(data_lines + [new_row])
        else:
            # Create new CSV with header
            csv_content = self._create_new_csv(gclid, formatted_time, conversion_value)
        
        # Save to R2
        return self.storage.save_csv(ctid, csv_content)
    
    def _create_new_csv(self, gclid: str, formatted_time: str, 
                       conversion_value: Optional[float] = None) -> str:
        """
        Create a new CSV with header and first row
        Following Google Ads official format specification
        
        Args:
            gclid: Google Click ID
            formatted_time: Formatted datetime string (yyyy-MM-dd HH:mm:ss)
            conversion_value: Optional conversion value
            
        Returns:
            CSV content as string
        """
        # Header without Parameters:TimeZone (Google Ads format)
        header = "Google Click ID,Conversion Name,Conversion Time,Conversion Value,Conversion Currency\n"
        value = str(conversion_value) if conversion_value is not None else ""
        row = f"{gclid},{settings.CONVERSION_NAME},{formatted_time},{value},{settings.CURRENCY}"
        return header + row
    
    def get_csv_content(self, ctid: str) -> Optional[str]:
        """
        Get CSV content for a customer ID
        
        Args:
            ctid: Customer ID
            
        Returns:
            CSV content as string, or None if not found
        """
        return self.storage.get_csv(ctid)
    
    def get_csv_url(self, ctid: str) -> str:
        """
        Get public URL for CSV file
        
        Args:
            ctid: Customer ID
            
        Returns:
            Public URL
        """
        return self.storage.get_public_url(ctid)
    
    def get_all_customer_ids(self) -> List[str]:
        """
        Get list of all customer IDs that have CSV files
        
        Returns:
            List of customer IDs
        """
        try:
            response = self.storage.s3_client.list_objects_v2(Bucket=self.storage.bucket_name)
            if 'Contents' not in response:
                return []
            
            ctids = []
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.csv'):
                    ctid = key.replace('.csv', '')
                    ctids.append(ctid)
            
            return sorted(ctids)
        except Exception as e:
            print(f"Error listing customer IDs: {e}")
            return []
    
    def get_conversion_count(self, ctid: str) -> int:
        """
        Get number of conversions for a customer ID
        
        Args:
            ctid: Customer ID
            
        Returns:
            Number of conversions
        """
        csv_content = self.storage.get_csv(ctid)
        if not csv_content:
            return 0
        
        lines = csv_content.strip().split('\n')
        # Count data lines (exclude header and parameter lines)
        data_lines = [line for line in lines if not line.startswith('Parameters:') and not line.startswith('Google Click ID') and line.strip()]
        return len(data_lines)
