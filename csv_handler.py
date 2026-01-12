"""
CSV Handler for managing conversion data and Cloudflare R2 storage
"""
import boto3
from botocore.client import Config
from io import StringIO, BytesIO
import csv
from datetime import datetime, timedelta
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
        
    def get_csv(self, src: str) -> Optional[str]:
        """
        Retrieve CSV content from R2
        
        Args:
            src: Source/Account ID
            
        Returns:
            CSV content as string, or None if file doesn't exist
        """
        key = f"{src}.csv"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read().decode('utf-8')
        except self.s3_client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error retrieving CSV for {src}: {e}")
            return None
    
    def save_csv(self, src: str, csv_content: str) -> bool:
        """
        Save CSV content to R2
        
        Args:
            src: Source/Account ID
            csv_content: CSV content as string
            
        Returns:
            True if successful, False otherwise
        """
        key = f"{src}.csv"
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            return True
        except Exception as e:
            print(f"Error saving CSV for {src}: {e}")
            return False
    
    def get_public_url(self, src: str) -> str:
        """
        Generate public URL for CSV file with API key in path
        Compatible with Google Ads (ends in .csv)
        
        Args:
            src: Source/Account ID
            
        Returns:
            Public URL to access the CSV (format: /csv/{api_key}/{src}.csv)
        """
        # For Railway or production, you'll need to set APP_URL environment variable
        # Example: https://seu-app.railway.app
        app_url = getattr(settings, 'APP_URL', 'http://localhost:8000')
        
        # Build URL with API key in path and .csv extension
        url = f"{app_url}/csv/{settings.API_KEY}/{src}.csv"
        
        return url


class CSVHandler:
    """Handles CSV operations for Google Ads conversions"""
    
    def __init__(self):
        """Initialize CSV handler with R2 storage"""
        self.storage = R2Storage()
        self.timezone = pytz.timezone(settings.TIMEZONE)
    
    def create_empty_source(self, src: str) -> bool:
        """
        Cria um CSV vazio (apenas com header) para um src (conta), caso ainda não exista.
        Retorna True se criado com sucesso ou já existir, False em caso de erro.
        """
        existing_csv = self.storage.get_csv(src)
        if existing_csv:
            # Já existe, não sobrescreve
            return True
        header = "Google Click ID,Conversion Name,Conversion Time,Conversion Value,Conversion Currency,Order ID\n"
        try:
            return self.storage.save_csv(src, header)
        except Exception as e:
            print(f"Erro ao criar CSV vazio para {src}: {e}")
            return False
    
    def add_conversion(self, src: str, gclid: str, conversion_time: str, 
                      conversion_value: Optional[float] = None,
                      order_id: Optional[str] = None) -> bool:
        """
        Add a conversion to the CSV file
        
        Args:
            src: Source/Account ID
            gclid: Google Click ID
            conversion_time: ISO 8601 datetime string
            conversion_value: Optional conversion value
            order_id: Optional order ID
            
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
        existing_csv = self.storage.get_csv(src)
        
        if existing_csv:
            # Append to existing CSV
            lines = existing_csv.strip().split('\n')
            # Remove Parameters:TimeZone if present, keep only header and data
            header_lines = [line for line in lines if line.startswith('Google Click ID')]
            data_lines = [line for line in lines if not line.startswith('Parameters:') and not line.startswith('Google Click ID') and line.strip()]
            value = str(conversion_value) if conversion_value is not None else ""
            order_id_value = order_id if order_id else ""
            new_row = f"{gclid},{settings.CONVERSION_NAME},{formatted_time},{value},{settings.CURRENCY},{order_id_value}"
            csv_content = '\n'.join(header_lines) + '\n' + '\n'.join(data_lines + [new_row])
        else:
            # Create new CSV with header
            csv_content = self._create_new_csv(gclid, formatted_time, conversion_value, order_id)
        
        # Save to R2
        return self.storage.save_csv(src, csv_content)
    
    def _create_new_csv(self, gclid: str, formatted_time: str, 
                       conversion_value: Optional[float] = None,
                       order_id: Optional[str] = None) -> str:
        """
        Create a new CSV with header and first row
        Following Google Ads official format specification
        
        Args:
            gclid: Google Click ID
            formatted_time: Formatted datetime string (yyyy-MM-dd HH:mm:ss)
            conversion_value: Optional conversion value
            order_id: Optional order ID
            
        Returns:
            CSV content as string
        """
        # Header with Order ID (Google Ads format)
        header = "Google Click ID,Conversion Name,Conversion Time,Conversion Value,Conversion Currency,Order ID\n"
        value = str(conversion_value) if conversion_value is not None else ""
        order_id_value = order_id if order_id else ""
        row = f"{gclid},{settings.CONVERSION_NAME},{formatted_time},{value},{settings.CURRENCY},{order_id_value}"
        return header + row
    
    def get_csv_content(self, src: str) -> Optional[str]:
        """
        Get CSV content for a source/account ID
        
        Args:
            src: Source/Account ID
            
        Returns:
            CSV content as string, or None if not found
        """
        return self.storage.get_csv(src)
    
    def get_csv_url(self, src: str) -> str:
        """
        Get public URL for CSV file
        
        Args:
            src: Source/Account ID
            
        Returns:
            Public URL
        """
        return self.storage.get_public_url(src)
    
    def get_all_sources(self) -> List[str]:
        """
        Get list of all customer IDs that have CSV files
        
        Returns:
            List of customer IDs
        """
        try:
            response = self.storage.s3_client.list_objects_v2(Bucket=self.storage.bucket_name)
            if 'Contents' not in response:
                return []
            
            srcs = []
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.csv'):
                    src = key.replace('.csv', '')
                    srcs.append(src)
            return sorted(srcs)
        except Exception as e:
            print(f"Error listing customer IDs: {e}")
            return []
    
    def get_conversion_count(self, src: str) -> Dict[str, int]:
        """
        Get number of conversions for a customer ID
        Returns separate counts for recent and archived conversions
        
        Args:
            src: Customer ID
            
        Returns:
            Dict with 'recent', 'history', and 'total' counts
        """
        # Count recent conversions
        recent_csv = self.storage.get_csv(src)
        recent_count = 0
        if recent_csv:
            lines = recent_csv.strip().split('\n')
            data_lines = [line for line in lines if not line.startswith('Parameters:') and not line.startswith('Google Click ID') and line.strip()]
            recent_count = len(data_lines)
        
        # Count archived conversions
        history_csv = self.storage.get_csv(f"{src}_history")
        history_count = 0
        if history_csv:
            lines = history_csv.strip().split('\n')
            data_lines = [line for line in lines if not line.startswith('Parameters:') and not line.startswith('Google Click ID') and line.strip()]
            history_count = len(data_lines)
        
        return {
            'recent': recent_count,
            'history': history_count,
            'total': recent_count + history_count
        }
    
    def cleanup_old_conversions(self, src: str, hours: int = 25) -> Dict[str, int]:
        """
        Archive conversions older than specified hours
        
        Args:
            src: Customer ID
            hours: Number of hours (default: 25)
            
        Returns:
            Dict with 'archived' and 'remaining' counts
        """
        csv_content = self.storage.get_csv(src)
        if not csv_content:
            return {'archived': 0, 'remaining': 0}
        
        cutoff_time = datetime.now(self.timezone) - timedelta(hours=hours)
        
        lines = csv_content.strip().split('\n')
        header_line = None
        recent_rows = []
        old_rows = []
        
        for line in lines:
            # Keep header
            if line.startswith('Google Click ID'):
                header_line = line
                continue
            
            # Skip parameter lines and empty lines
            if line.startswith('Parameters:') or not line.strip():
                continue
            
            # Parse conversion time from CSV row
            try:
                parts = line.split(',')
                if len(parts) >= 3:
                    conversion_time_str = parts[2]  # Conversion Time is 3rd column
                    # Parse datetime (format: yyyy-MM-dd HH:mm:ss)
                    conversion_time = datetime.strptime(conversion_time_str, '%Y-%m-%d %H:%M:%S')
                    # Make timezone aware
                    conversion_time = self.timezone.localize(conversion_time)
                    
                    if conversion_time < cutoff_time:
                        old_rows.append(line)
                    else:
                        recent_rows.append(line)
                else:
                    # Malformed row, keep it in recent to be safe
                    recent_rows.append(line)
            except Exception as e:
                print(f"Error parsing conversion time: {e}, keeping row in recent")
                recent_rows.append(line)
        
        # Save recent conversions back to main CSV
        if header_line:
            recent_csv = header_line + '\n' + '\n'.join(recent_rows) if recent_rows else header_line
        else:
            recent_csv = '\n'.join(recent_rows)
        
        self.storage.save_csv(src, recent_csv)
        
        # Append old conversions to history
        if old_rows:
            self._append_to_history(src, old_rows)
        
        print(f"🧹 Cleanup for {src}: {len(old_rows)} archived, {len(recent_rows)} remaining")
        
        return {
            'archived': len(old_rows),
            'remaining': len(recent_rows)
        }
    
    def _append_to_history(self, src: str, rows: List[str]) -> bool:
        """
        Append conversion rows to history CSV
        
        Args:
            src: Customer ID
            rows: List of CSV rows to append
            
        Returns:
            True if successful
        """
        history_key = f"{src}_history"
        existing_history = self.storage.get_csv(history_key)
        
        if existing_history:
            # Append to existing history
            updated_history = existing_history.strip() + '\n' + '\n'.join(rows)
        else:
            # Create new history file with header
            header = "Google Click ID,Conversion Name,Conversion Time,Conversion Value,Conversion Currency,Order ID\n"
            updated_history = header + '\n' + '\n'.join(rows)
        
        return self.storage.save_csv(history_key, updated_history)
    
    def cleanup_all_sources(self, hours: int = 25) -> Dict[str, Dict[str, int]]:
        """
        Execute cleanup for all customer IDs
        Used by automatic scheduler
        
        Args:
            hours: Number of hours threshold
            
        Returns:
            Dict mapping src to cleanup results
        """
        sources = self.get_all_sources()
        results = {}
        
        for src in sources:
            # Skip history files
            if src.endswith('_history'):
                continue
            results[src] = self.cleanup_old_conversions(src, hours)
        
        return results
