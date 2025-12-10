import pandas as pd
import csv
import re
import tempfile
import os
import io

def fix_csv_issues(filepath):
    """
    Fix all CSV issues and return path to fixed file
    """
    # Read the raw file content
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # Fix 1: Handle line endings
    content = content.replace('\r\n', '\n').replace('\r', '\n')
    
    # Fix 2: Remove BOM if present
    if content.startswith('\ufeff'):
        content = content[1:]
    
    # Fix 3: Fix the specific issue with spaces around quotes
    # Pattern: comma, spaces, quote
    content = re.sub(r',\s+"', ',"', content)
    # Pattern: quote, spaces, comma or end of line
    content = re.sub(r'"\s+,', '",', content)
    content = re.sub(r'"\s*$', '"', content)
    
    # Fix 4: Ensure consistent number of columns
    lines = content.strip().split('\n')
    
    # Count commas in header
    header = lines[0] if lines else ''
    expected_commas = header.count(',')
    
    fixed_lines = []
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
            
        comma_count = line.count(',')
        
        if comma_count > expected_commas:
            # Too many commas - likely unquoted commas in text
            # Find and quote fields that might have commas
            parts = line.split(',')
            if len(parts) > expected_commas + 1:
                # Join extra parts into the last field
                line = ','.join(parts[:expected_commas]) + ',"' + ','.join(parts[expected_commas:]) + '"'
        elif comma_count < expected_commas:
            # Add missing empty fields
            line += ',' * (expected_commas - comma_count)
        
        fixed_lines.append(line)
    
    # Create temp file with fixed content
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8')
    temp_path = temp_file.name
    
    with open(temp_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(fixed_lines))
    
    return temp_path

def smart_read_csv(filepath):
    """
    Smart CSV reader that handles all issues
    """
    # First try standard read
    try:
        return pd.read_csv(filepath)
    except Exception as e:
        print(f"Standard read failed: {e}")
    
    # If failed, fix the file and read
    fixed_path = None
    try:
        fixed_path = fix_csv_issues(filepath)
        df = pd.read_csv(fixed_path)
        
        # Clean up temp file
        if fixed_path and os.path.exists(fixed_path):
            os.unlink(fixed_path)
        
        return df
    except Exception as e:
        print(f"Fixed read also failed: {e}")
        
        # Last resort: manual parsing
        return manual_csv_parse(filepath)

def manual_csv_parse(filepath):
    """
    Manual CSV parsing as last resort
    """
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    
    # Clean each line
    cleaned = []
    for line in lines:
        line = line.strip()
        if line:
            # Remove extra spaces around commas
            line = re.sub(r'\s*,\s*', ',', line)
            cleaned.append(line)
    
    # Parse with csv module
    rows = []
    for line in cleaned:
        try:
            # Use csv module to handle quoted fields
            reader = csv.reader([line])
            row = next(reader)
            rows.append(row)
        except:
            # Fallback: split by comma, ignoring commas in quotes
            in_quotes = False
            fields = []
            current_field = []
            
            for char in line:
                if char == '"':
                    in_quotes = not in_quotes
                    current_field.append(char)
                elif char == ',' and not in_quotes:
                    fields.append(''.join(current_field).strip('"').strip())
                    current_field = []
                else:
                    current_field.append(char)
            
            # Add last field
            if current_field:
                fields.append(''.join(current_field).strip('"').strip())
            
            rows.append(fields)
    
    # Create DataFrame
    if len(rows) > 1:
        df = pd.DataFrame(rows[1:], columns=rows[0])
    else:
        df = pd.DataFrame()
    
    return df