# ...existing code...
import pandas as pd
import numpy as np
import csv

try:
    import chardet
except ImportError:
    chardet = None
    import warnings
    warnings.warn("Optional dependency 'chardet' is not installed; falling back to 'utf-8' for encoding detection.", ImportWarning)

def detect_encoding(filepath):
    """Detect file encoding"""
    if chardet is None:
        # Best-effort fallback
        return 'utf-8'
    with open(filepath, 'rb') as f:
        raw_data = f.read(10000)  # Read first 10KB
        result = chardet.detect(raw_data)
        return result['encoding']

# ...existing code...
def smart_read_csv(filepath, encoding=None):
    """
    Smart CSV reader that handles various issues:
    - Inconsistent number of columns
    - Commas in text fields
    - Different encodings
    - Line breaks in cells
    """
    
    if encoding is None:
        encoding = detect_encoding(filepath)

    
    # Multiple read attempts with different strategies
    strategies = [
        # Strategy 1: Standard read
        lambda: pd.read_csv(filepath, encoding=encoding),
        
        # Strategy 2: Skip bad lines
        lambda: pd.read_csv(filepath, encoding=encoding, on_bad_lines='skip', engine='python'),
        
        # Strategy 3: Warn on bad lines
        lambda: pd.read_csv(filepath, encoding=encoding, on_bad_lines='warn', engine='python'),
        
        # Strategy 4: Manual parsing
        lambda: manual_csv_parse(filepath, encoding),
    ]
    
    for i, strategy in enumerate(strategies):
        try:
            print(f"Attempting strategy {i+1}...")
            df = strategy()
            print(f"✓ Strategy {i+1} succeeded")
            return df
        except Exception as e:
            print(f"✗ Strategy {i+1} failed: {e}")
            continue
    
    raise ValueError("All CSV reading strategies failed")

def manual_csv_parse(filepath, encoding='utf-8'):
    """Manually parse CSV file"""
    rows = []
    
    with open(filepath, 'r', encoding=encoding, errors='ignore') as f:
        # Read entire content
        content = f.read()
        
        # Replace problematic patterns
        # Handle quoted fields with commas
        import re
        
        # Pattern to match quoted text
        quoted_pattern = r'"[^"]*"'
        
        # Temporarily replace commas inside quotes
        def replace_commas(match):
            return match.group(0).replace(',', '||COMMA||')
        
        content = re.sub(quoted_pattern, replace_commas, content)
        
        # Now split by lines and commas
        lines = content.strip().split('\n')
        
        # Find max columns
        max_cols = 0
        for line in lines:
            cols = line.split(',')
            if len(cols) > max_cols:
                max_cols = len(cols)
        
        # Process each line
        for line in lines:
            cols = line.split(',')
            
            # Pad or truncate columns
            if len(cols) < max_cols:
                cols.extend([''] * (max_cols - len(cols)))
            elif len(cols) > max_cols:
                # Merge extra columns into last column
                cols = cols[:max_cols-1] + [','.join(cols[max_cols-1:])]
            
            # Restore commas inside quotes
            cols = [col.replace('||COMMA||', ',') for col in cols]
            
            # Remove quotes if present
            cols = [col.strip('"') for col in cols]
            
            rows.append(cols)
    
    # Create DataFrame
    if rows:
        df = pd.DataFrame(rows[1:], columns=rows[0])
    else:
        df = pd.DataFrame()
    
    return df

def clean_csv_file(input_path, output_path=None):
    """Clean a CSV file and save fixed version"""
    if output_path is None:
        import tempfile
        output_path = tempfile.NamedTemporaryFile(delete=False, suffix='.csv').name
    
    df = smart_read_csv(input_path)
    df.to_csv(output_path, index=False, encoding='utf-8')
    
    return output_path