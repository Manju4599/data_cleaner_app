import pandas as pd
import numpy as np
from datetime import datetime
import chardet
import csv
import os

class SimpleDataCleaner:
    def __init__(self, filepath, options=None):
        self.filepath = filepath
        self.options = options or {}
        self.df = None
        self.report = {}
        self._load_data()
    
    def _load_data(self):
        """Load data from file with robust error handling"""
        print(f"\nLoading file: {self.filepath}")
        
        ext = os.path.splitext(self.filepath)[1].lower()
        
        if ext == '.csv':
            self.df = self._read_csv_robustly()
        elif ext in ['.xlsx', '.xls']:
            self.df = pd.read_excel(self.filepath)
        elif ext == '.json':
            self.df = pd.read_json(self.filepath)
        else:
            # Try as CSV
            self.df = self._read_csv_robustly()
        
        if self.df is not None and not self.df.empty:
            self.original_shape = self.df.shape
            self.report['original_rows'] = int(self.original_shape[0])
            self.report['original_columns'] = int(self.original_shape[1])
            
            print(f"✓ File loaded successfully!")
            print(f"  Shape: {self.df.shape}")
            print(f"  Columns: {list(self.df.columns)}")
        else:
            print("✗ Failed to load file or file is empty")
            self.df = pd.DataFrame()
            self.original_shape = (0, 0)
    
    def _read_csv_robustly(self):
        """Read CSV file with multiple fallback strategies"""
        strategies = [
            self._try_pandas_read,
            self._try_csv_module,
            self._try_manual_parse,
        ]
        
        for strategy in strategies:
            df = strategy()
            if df is not None and not df.empty:
                return df
        
        return pd.DataFrame()
    
    def _try_pandas_read(self):
        """Try reading with pandas"""
        encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1', 'utf-16']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(self.filepath, encoding=encoding, on_bad_lines='skip', engine='python')
                if not df.empty:
                    print(f"  Pandas read with {encoding}: ✓")
                    return df
            except Exception as e:
                print(f"  Pandas read with {encoding}: ✗")
                continue
        
        return None
    
    def _try_csv_module(self):
        """Try reading with Python's csv module"""
        encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                with open(self.filepath, 'r', encoding=encoding, errors='replace') as f:
                    # Read first line to detect delimiter
                    first_line = f.readline()
                    f.seek(0)
                    
                    # Detect delimiter
                    delimiters = [',', ';', '\t', '|']
                    delimiter_counts = {d: first_line.count(d) for d in delimiters}
                    delimiter = max(delimiter_counts, key=delimiter_counts.get)
                    
                    # Read all rows
                    reader = csv.reader(f, delimiter=delimiter)
                    rows = list(reader)
                    
                    if rows and len(rows) > 1:
                        # Create DataFrame
                        df = pd.DataFrame(rows[1:], columns=rows[0])
                        print(f"  CSV module with {encoding}: ✓")
                        return df
            except Exception as e:
                print(f"  CSV module with {encoding}: ✗")
                continue
        
        return None
    
    def _try_manual_parse(self):
        """Manual parsing as last resort"""
        try:
            with open(self.filepath, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            if not lines:
                return None
            
            # Clean lines
            cleaned_lines = []
            for line in lines:
                line = line.strip()
                if line:
                    cleaned_lines.append(line)
            
            if not cleaned_lines:
                return None
            
            # Split by comma (simple approach)
            rows = []
            for line in cleaned_lines:
                rows.append([cell.strip() for cell in line.split(',')])
            
            # Find max columns
            max_cols = max(len(row) for row in rows) if rows else 0
            
            # Pad rows
            for row in rows:
                if len(row) < max_cols:
                    row.extend([''] * (max_cols - len(row)))
            
            # Create DataFrame
            if len(rows) > 1:
                df = pd.DataFrame(rows[1:], columns=rows[0])
                print("  Manual parse: ✓")
                return df
            else:
                df = pd.DataFrame(columns=rows[0] if rows else [])
                print("  Manual parse: ✓ (header only)")
                return df
        except Exception as e:
            print(f"  Manual parse: ✗")
            return None
    
    def clean_data(self):
        """Simple cleaning pipeline"""
        if self.df.empty:
            self.report['error'] = 'Empty DataFrame'
            return self.report
        
        original_rows = self.original_shape[0]
        original_cols = self.original_shape[1]
        
        print(f"\nStarting cleaning process...")
        print(f"Initial shape: {self.df.shape}")
        
        # 1. Clean column names first
        self.df.columns = [self._clean_column_name(col) for col in self.df.columns]
        print(f"Cleaned column names: {list(self.df.columns)}")
        
        # 2. Drop columns with too many missing values
        threshold = self.options.get('missing_threshold', 0.5)
        if threshold < 1:  # Only if threshold is reasonable
            missing_percent = self.df.isnull().sum() / len(self.df)
            cols_to_drop = missing_percent[missing_percent > threshold].index.tolist()
            if cols_to_drop:
                print(f"Dropping columns with >{threshold*100}% missing: {cols_to_drop}")
                self.df = self.df.drop(columns=cols_to_drop)
        
        # 3. Fill missing values (simplified)
        for col in self.df.columns:
            if self.df[col].isnull().any():
                null_count = self.df[col].isnull().sum()
                print(f"Column '{col}' has {null_count} missing values")
                
                if pd.api.types.is_numeric_dtype(self.df[col]):
                    fill_value = self.df[col].median()
                    self.df[col] = self.df[col].fillna(fill_value)
                    print(f"  Filled with median: {fill_value}")
                else:
                    self.df[col] = self.df[col].fillna('Unknown')
                    print(f"  Filled with 'Unknown'")
        
        # 4. Remove duplicates if requested
        if self.options.get('handle_duplicates', 'drop') == 'drop':
            duplicates_count = int(self.df.duplicated().sum())
            if duplicates_count > 0:
                print(f"Removing {duplicates_count} duplicate rows")
                self.df = self.df.drop_duplicates()
                self.report['duplicates_removed'] = duplicates_count
        
        # 5. Standardize text if requested (carefully)
        if self.options.get('standardize_text'):
            text_cols = self.df.select_dtypes(include=['object']).columns
            for col in text_cols:
                # Only clean if it doesn't look like currency or special data
                sample = self.df[col].dropna().iloc[0] if not self.df[col].dropna().empty else ''
                if not (isinstance(sample, str) and ('$' in sample or sample.startswith('['))):
                    self.df[col] = self.df[col].astype(str).str.strip()
        
        # Generate report
        self.report.update({
            'final_rows': int(len(self.df)),
            'final_columns': int(len(self.df.columns)),
            'rows_removed': int(original_rows - len(self.df)),
            'columns_removed': int(original_cols - len(self.df.columns)),
            'cleaning_timestamp': datetime.now().isoformat(),
            'note': 'Simple cleaning applied'
        })
        
        print(f"Final shape: {self.df.shape}")
        print(f"Rows removed: {self.report['rows_removed']}")
        print(f"Columns removed: {self.report['columns_removed']}")
        
        return self.report
    
    def _clean_column_name(self, col):
        """Clean a single column name"""
        if pd.isna(col):
            return 'unknown_column'
        
        col = str(col).strip()
        
        # Remove special characters but keep underscores
        col = re.sub(r'[^\w\s]', '', col)
        
        # Replace spaces with underscores
        col = re.sub(r'\s+', '_', col)
        
        # Convert to lowercase
        col = col.lower()
        
        # If empty after cleaning
        if not col:
            return 'column'
        
        return col

# Add regex import if not already there
import re