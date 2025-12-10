import os
import pandas as pd
import json
from datetime import datetime, timedelta
import shutil
import csv

class FileHandler:
    def __init__(self, upload_folder):
        self.upload_folder = upload_folder
        if not os.path.exists(upload_folder):
            os.makedirs(upload_folder)
    
    def save_uploaded_file(self, file, filename):
        """Save uploaded file with timestamp"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_filename = f"{timestamp}_{filename}"
        filepath = os.path.join(self.upload_folder, unique_filename)
        file.save(filepath)
        return filepath
    
    def load_file(self, filepath):
        """Load file with robust error handling"""
        ext = os.path.splitext(filepath)[1].lower()
        
        if ext == '.csv':
            return self._safe_read_csv(filepath)
        elif ext in ['.xlsx', '.xls']:
            return pd.read_excel(filepath)
        elif ext == '.json':
            return pd.read_json(filepath)
        else:
            # Try as CSV
            return self._safe_read_csv(filepath)
    
    def _safe_read_csv(self, filepath):
        """Safely read CSV file with multiple fallbacks"""
        strategies = [
            lambda: pd.read_csv(filepath, encoding='utf-8'),
            lambda: pd.read_csv(filepath, encoding='latin1'),
            lambda: pd.read_csv(filepath, encoding='utf-8', on_bad_lines='skip', engine='python'),
            lambda: pd.read_csv(filepath, encoding='latin1', on_bad_lines='skip', engine='python'),
            lambda: self._manual_csv_read(filepath),
        ]
        
        for strategy in strategies:
            try:
                return strategy()
            except:
                continue
        
        raise ValueError(f"Cannot read CSV file: {filepath}")
    
    def _manual_csv_read(self, filepath):
        """Manual CSV reading as last resort"""
        rows = []
        
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            for row in reader:
                rows.append(row)
        
        if len(rows) > 1:
            # Pad rows to same length
            max_len = max(len(row) for row in rows)
            for row in rows:
                if len(row) < max_len:
                    row.extend([''] * (max_len - len(row)))
            
            df = pd.DataFrame(rows[1:], columns=rows[0])
        else:
            df = pd.DataFrame()
        
        return df
    
    def save_cleaned_data(self, df, filename):
        """Save cleaned data to file"""
        filepath = os.path.join(self.upload_folder, filename)
        ext = os.path.splitext(filename)[1].lower()
        
        if ext == '.csv':
            df.to_csv(filepath, index=False)
        elif ext in ['.xlsx', '.xls']:
            df.to_excel(filepath, index=False)
        elif ext == '.json':
            df.to_json(filepath, orient='records')
        else:
            # Default to CSV
            filepath = filepath.rsplit('.', 1)[0] + '.csv'
            df.to_csv(filepath, index=False)
        
        return filepath
    
    def save_cleaning_report(self, report, filename):
        """Save cleaning report as JSON"""
        filepath = os.path.join(self.upload_folder, filename)
        
        # Ensure report is JSON serializable
        def default_serializer(obj):
            if isinstance(obj, (int, float)):
                return obj
            elif isinstance(obj, str):
                return obj
            elif obj is None:
                return None
            else:
                return str(obj)
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2, default=default_serializer)
        return filepath
    
    def cleanup_old_files(self, max_age_hours=1):
        """Clean up files older than max_age_hours"""
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        
        for filename in os.listdir(self.upload_folder):
            filepath = os.path.join(self.upload_folder, filename)
            
            if os.path.isfile(filepath):
                file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                
                if file_time < cutoff_time:
                    try:
                        os.remove(filepath)
                    except:
                        pass  # Skip files that can't be deleted