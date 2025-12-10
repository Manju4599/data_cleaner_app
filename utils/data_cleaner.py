import pandas as pd
import numpy as np
from datetime import datetime
import chardet

class SimpleDataCleaner:
    def __init__(self, filepath, options=None):
        self.filepath = filepath
        self.options = options or {}
        self.df = None
        self.report = {}
        self._load_data()
    
    def _load_data(self):
        """Load data from file with encoding detection"""
        import os
        ext = os.path.splitext(self.filepath)[1].lower()
        
        if ext == '.csv':
            # Try to detect encoding first
            encoding = self._detect_encoding()
            
            # Try multiple reading strategies
            strategies = [
                lambda: pd.read_csv(self.filepath, encoding=encoding, on_bad_lines='skip', engine='python'),
                lambda: pd.read_csv(self.filepath, encoding='utf-8', on_bad_lines='skip', engine='python'),
                lambda: pd.read_csv(self.filepath, encoding='latin1', on_bad_lines='skip', engine='python'),
                lambda: pd.read_csv(self.filepath, encoding='cp1252', on_bad_lines='skip', engine='python'),
                lambda: self._manual_csv_read(self.filepath),
            ]
            
            for i, strategy in enumerate(strategies, 1):
                try:
                    print(f"CSV reading strategy {i}...")
                    self.df = strategy()
                    if not self.df.empty:
                        print(f"✓ Strategy {i} succeeded, shape: {self.df.shape}")
                        break
                except Exception as e:
                    print(f"✗ Strategy {i} failed: {str(e)[:100]}")
                    continue
            
            if self.df is None or self.df.empty:
                raise ValueError("Could not read CSV file with any strategy")
        
        elif ext in ['.xlsx', '.xls']:
            self.df = pd.read_excel(self.filepath)
        
        elif ext == '.json':
            self.df = pd.read_json(self.filepath)
        
        else:
            # Try as CSV
            self.df = pd.read_csv(self.filepath, sep=None, engine='python', on_bad_lines='skip')
        
        self.original_shape = self.df.shape
        self.report['original_rows'] = int(self.original_shape[0])
        self.report['original_columns'] = int(self.original_shape[1])
        
        print(f"Loaded DataFrame shape: {self.df.shape}")
        print(f"Columns: {list(self.df.columns)}")
    
    def _detect_encoding(self):
        """Detect file encoding"""
        try:
            with open(self.filepath, 'rb') as f:
                raw_data = f.read(10000)
                result = chardet.detect(raw_data)
                encoding = result['encoding']
                print(f"Detected encoding: {encoding} (confidence: {result['confidence']})")
                return encoding
        except:
            return 'utf-8'
    
    def _manual_csv_read(self, filepath):
        """Manual CSV reading fallback"""
        import csv
        
        rows = []
        encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                with open(filepath, 'r', encoding=encoding, errors='replace') as f:
                    reader = csv.reader(f)
                    for row in reader:
                        rows.append(row)
                break
            except:
                continue
        
        if rows:
            # Find max columns
            max_cols = max(len(row) for row in rows)
            
            # Pad rows
            for i in range(len(rows)):
                if len(rows[i]) < max_cols:
                    rows[i].extend([''] * (max_cols - len(rows[i])))
            
            df = pd.DataFrame(rows[1:], columns=rows[0])
        else:
            df = pd.DataFrame()
        
        return df
    
    def clean_data(self):
        """Simple cleaning pipeline"""
        original_rows = self.original_shape[0]
        original_cols = self.original_shape[1]
        
        # 1. Drop columns with too many missing values
        threshold = self.options.get('missing_threshold', 0.5)
        missing_percent = self.df.isnull().sum() / len(self.df)
        cols_to_drop = missing_percent[missing_percent > threshold].index.tolist()
        if cols_to_drop:
            self.df = self.df.drop(columns=cols_to_drop)
        
        # 2. Fill missing values
        method = self.options.get('handle_missing', 'auto')
        for col in self.df.columns:
            if self.df[col].isnull().any():
                if method == 'mean' and pd.api.types.is_numeric_dtype(self.df[col]):
                    self.df[col] = self.df[col].fillna(self.df[col].mean())
                elif method == 'median' and pd.api.types.is_numeric_dtype(self.df[col]):
                    self.df[col] = self.df[col].fillna(self.df[col].median())
                elif method == 'mode':
                    mode_val = self.df[col].mode()
                    if not mode_val.empty:
                        self.df[col] = self.df[col].fillna(mode_val.iloc[0])
                    else:
                        self.df[col] = self.df[col].fillna('Unknown')
                else:  # auto or default
                    if pd.api.types.is_numeric_dtype(self.df[col]):
                        self.df[col] = self.df[col].fillna(self.df[col].median())
                    else:
                        self.df[col] = self.df[col].fillna('Unknown')
        
        # 3. Remove duplicates (only if explicitly requested)
        if self.options.get('handle_duplicates', 'drop') == 'drop':
            duplicates_count = int(self.df.duplicated().sum())
            if duplicates_count > 0:
                self.df = self.df.drop_duplicates()
                self.report['duplicates_removed'] = duplicates_count
        
        # 4. Standardize text if requested (but skip for certain data types)
        if self.options.get('standardize_text'):
            text_cols = self.df.select_dtypes(include=['object']).columns
            for col in text_cols:
                # Don't lowercase currency or special data
                if 'gross' not in col.lower() and '$' not in str(self.df[col].iloc[0] if not self.df[col].empty else ''):
                    self.df[col] = self.df[col].astype(str).str.strip()
        
        # 5. Standardize column names (but keep them readable)
        new_columns = []
        for col in self.df.columns:
            new_col = str(col).strip()
            new_col = new_col.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
            new_col = new_col.lower()
            new_columns.append(new_col)
        self.df.columns = new_columns
        
        # Generate report
        self.report.update({
            'final_rows': int(len(self.df)),
            'final_columns': int(len(self.df.columns)),
            'rows_removed': int(original_rows - len(self.df)),
            'columns_removed': int(original_cols - len(self.df.columns)),
            'cleaning_timestamp': datetime.now().isoformat()
        })
        
        return self.report