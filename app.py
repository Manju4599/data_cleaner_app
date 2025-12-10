import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify
from werkzeug.utils import secure_filename
from datetime import datetime
import json
import traceback
import numpy as np
import csv
import re
import tempfile
import chardet
import io

from utils.simple_cleaner import SimpleDataCleaner as DataCleaner
from config import Config
from utils.file_handler import FileHandler

app = Flask(__name__)
app.config.from_object(Config)

# Initialize file handler
file_handler = FileHandler(app.config['UPLOAD_FOLDER'])

# Context processor for templates
@app.context_processor
def inject_now():
    return {'now': datetime.now()}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def convert_to_serializable(obj):
    """Convert object to JSON serializable format"""
    if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif pd.isna(obj):
        return None
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, pd.Series):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_to_serializable(item) for item in obj)
    else:
        return obj

def detect_file_encoding(filepath):
    """Detect file encoding with chardet"""
    try:
        with open(filepath, 'rb') as f:
            raw_data = f.read(100000)  # Read more data for better detection
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']
            print(f"Detected encoding: {encoding} (confidence: {confidence})")
            return encoding if confidence > 0.7 else 'utf-8'
    except Exception as e:
        print(f"Encoding detection failed: {e}")
        return 'utf-8'

def read_csv_with_fallbacks(filepath):
    """
    Read CSV file with multiple fallback strategies
    Returns DataFrame
    """
    print(f"\nReading CSV: {filepath}")
    print("-" * 50)
    
    # Strategy 1: Try standard pandas read with various encodings
    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1', 'utf-16', 'ascii']
    
    for encoding in encodings:
        try:
            print(f"Trying pandas with {encoding} encoding...")
            df = pd.read_csv(filepath, encoding=encoding)
            if not df.empty and len(df.columns) > 1:
                print(f"✓ Success with {encoding}! Shape: {df.shape}")
                return df
        except Exception as e:
            print(f"✗ Failed with {encoding}: {str(e)[:100]}")
    
    # Strategy 2: Try with error handling
    for encoding in encodings:
        try:
            print(f"Trying pandas with error handling ({encoding})...")
            df = pd.read_csv(filepath, encoding=encoding, on_bad_lines='skip', engine='python')
            if not df.empty:
                print(f"✓ Success with error handling! Shape: {df.shape}")
                return df
        except Exception as e:
            print(f"✗ Failed: {str(e)[:100]}")
    
    # Strategy 3: Manual CSV parsing
    print("Trying manual CSV parsing...")
    df = manual_csv_parse(filepath)
    if not df.empty:
        print(f"✓ Manual parsing succeeded! Shape: {df.shape}")
        return df
    
    # Strategy 4: Try reading as text and parsing
    print("Trying text-based parsing...")
    df = text_based_csv_parse(filepath)
    if not df.empty:
        print(f"✓ Text parsing succeeded! Shape: {df.shape}")
        return df
    
    print("✗ All CSV reading strategies failed")
    return pd.DataFrame()

def manual_csv_parse(filepath):
    """Manual CSV parsing"""
    rows = []
    
    # Try different encodings
    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1', 'utf-16']
    
    for encoding in encodings:
        try:
            with open(filepath, 'r', encoding=encoding, errors='replace') as f:
                # Read first few lines to check format
                lines = []
                for i, line in enumerate(f):
                    if i < 50:  # Read first 50 lines
                        lines.append(line.strip())
                    else:
                        break
                
                if lines:
                    # Try to detect delimiter
                    delimiters = [',', ';', '\t', '|']
                    delimiter_counts = {d: lines[0].count(d) for d in delimiters}
                    delimiter = max(delimiter_counts, key=delimiter_counts.get)
                    
                    # Parse with csv module
                    f.seek(0)
                    reader = csv.reader(f, delimiter=delimiter)
                    rows = list(reader)
                    break
        except Exception as e:
            continue
    
    if not rows:
        # Last resort: simple split
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                rows.append(line.strip().split(','))
    
    if len(rows) > 1:
        # Find max columns
        max_cols = max(len(row) for row in rows)
        
        # Pad rows with fewer columns
        for i in range(len(rows)):
            if len(rows[i]) < max_cols:
                rows[i].extend([''] * (max_cols - len(rows[i])))
        
        # Create DataFrame
        df = pd.DataFrame(rows[1:], columns=rows[0])
    else:
        df = pd.DataFrame()
    
    return df

def text_based_csv_parse(filepath):
    """Parse CSV by reading as text and cleaning"""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Clean the content
        content = clean_csv_content(content)
        
        # Try to parse
        try:
            df = pd.read_csv(io.StringIO(content))
            return df
        except:
            # Manual parsing
            lines = content.strip().split('\n')
            if lines:
                # Guess delimiter
                first_line = lines[0]
                delimiters = [',', ';', '\t', '|']
                delimiter_counts = {d: first_line.count(d) for d in delimiters}
                delimiter = max(delimiter_counts, key=delimiter_counts.get)
                
                # Parse
                rows = []
                for line in lines:
                    rows.append([cell.strip() for cell in line.split(delimiter)])
                
                if len(rows) > 1:
                    df = pd.DataFrame(rows[1:], columns=rows[0])
                    return df
    except Exception as e:
        print(f"Text parsing error: {e}")
    
    return pd.DataFrame()

def clean_csv_content(content):
    """Clean CSV content"""
    # Fix line endings
    content = content.replace('\r\n', '\n').replace('\r', '\n')
    
    # Remove BOM
    if content.startswith('\ufeff'):
        content = content[1:]
    
    # Fix common encoding issues
    replacements = {
        'Â': '',
        'â€': '-',
        'Ã©': 'é',
        'Ã': 'í',
        'Ã³': 'ó',
        'Ã¡': 'á',
        'Ã±': 'ñ',
        'Ãº': 'ú',
        'Ã¼': 'ü',
        'Ã§': 'ç',
        '\xa0': ' ',
        '\x96': '-',
        '\x97': '-',
    }
    
    for old, new in replacements.items():
        content = content.replace(old, new)
    
    # Remove empty lines
    lines = [line for line in content.split('\n') if line.strip()]
    
    # Ensure consistent columns
    if lines:
        # Count commas in first line (header)
        header = lines[0]
        comma_count = header.count(',')
        
        cleaned_lines = [header]
        for line in lines[1:]:
            line_comma_count = line.count(',')
            if line_comma_count < comma_count:
                # Add missing commas
                line += ',' * (comma_count - line_comma_count)
            elif line_comma_count > comma_count:
                # Remove extra commas (keep first N+1 fields)
                parts = line.split(',')
                if len(parts) > comma_count + 1:
                    # Merge extra fields into last column
                    line = ','.join(parts[:comma_count]) + ',"' + ','.join(parts[comma_count:]) + '"'
            cleaned_lines.append(line)
        
        content = '\n'.join(cleaned_lines)
    
    return content

def create_debug_file(filepath, df):
    """Create debug file to see what was read"""
    debug_path = filepath + '.debug.txt'
    with open(debug_path, 'w', encoding='utf-8') as f:
        f.write(f"File: {filepath}\n")
        f.write(f"DataFrame shape: {df.shape}\n")
        f.write(f"Columns: {list(df.columns)}\n")
        f.write("\nFirst 5 rows:\n")
        f.write(df.head().to_string())
        f.write("\n\nDataFrame info:\n")
        f.write(str(df.info()))
    
    print(f"Debug file created: {debug_path}")
    return debug_path

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Check if file was uploaded
        if 'file' not in request.files:
            flash('No file selected')
            return redirect(request.url)
        
        file = request.files['file']
        
        if file.filename == '':
            flash('No file selected')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            try:
                print(f"\n{'='*60}")
                print(f"Processing file: {file.filename}")
                print('='*60)
                
                # Save uploaded file
                filename = secure_filename(file.filename)
                original_path = file_handler.save_uploaded_file(file, filename)
                print(f"Original file saved to: {original_path}")
                
                # Get cleaning options from form
                cleaning_options = {
                    'handle_missing': request.form.get('handle_missing', 'auto'),
                    'missing_threshold': float(request.form.get('missing_threshold', 0.5)),
                    'handle_duplicates': request.form.get('handle_duplicates', 'drop'),
                    'standardize_dates': request.form.get('standardize_dates') == 'on',
                    'remove_outliers': request.form.get('remove_outliers') == 'on',
                    'outlier_method': request.form.get('outlier_method', 'iqr'),
                    'standardize_text': request.form.get('standardize_text') == 'on',
                    'infer_types': request.form.get('infer_types') == 'on',
                    'encoding': request.form.get('encoding', 'utf-8')
                }
                
                print(f"Cleaning options: {cleaning_options}")
                
                # Read the file first to check if it's valid
                print("\nAttempting to read file...")
                if filename.lower().endswith('.csv'):
                    df_raw = read_csv_with_fallbacks(original_path)
                    
                    if df_raw.empty:
                        # Try one more approach: read as text and save as fixed CSV
                        print("Raw reading failed, trying text-based approach...")
                        with open(original_path, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                        
                        # Create a fixed version
                        temp_file = tempfile.NamedTemporaryFile(
                            mode='w', delete=False, suffix='.csv', encoding='utf-8'
                        )
                        temp_path = temp_file.name
                        temp_file.write(clean_csv_content(content))
                        temp_file.close()
                        
                        print(f"Created fixed CSV: {temp_path}")
                        filepath = temp_path
                    else:
                        filepath = original_path
                        print(f"File read successfully, shape: {df_raw.shape}")
                else:
                    filepath = original_path
                
                # Initialize data cleaner
                print(f"\nInitializing DataCleaner with: {filepath}")
                cleaner = DataCleaner(filepath, cleaning_options)
                
                # Check if DataFrame was loaded
                if cleaner.df is None or cleaner.df.empty:
                    flash('The uploaded file appears to be empty or could not be read properly. Please check the file format.')
                    return redirect(request.url)
                
                print(f"DataFrame loaded successfully!")
                print(f"  Shape: {cleaner.df.shape}")
                print(f"  Columns: {list(cleaner.df.columns)}")
                print(f"  First few rows:")
                print(cleaner.df.head(3).to_string())
                
                # Create debug file
                debug_path = create_debug_file(filepath, cleaner.df)
                
                # Perform cleaning
                cleaning_report = cleaner.clean_data()
                
                # Convert report to serializable format
                serializable_report = convert_to_serializable(cleaning_report)
                
                # Save cleaned data
                cleaned_filename = f"cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}"
                cleaned_filepath = file_handler.save_cleaned_data(cleaner.df, cleaned_filename)
                print(f"Cleaned data saved to: {cleaned_filepath}")
                
                # Save cleaning report
                report_filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                report_path = file_handler.save_cleaning_report(serializable_report, report_filename)
                
                # Clean up temporary files
                if 'temp_path' in locals() and os.path.exists(temp_path):
                    try:
                        os.unlink(temp_path)
                        print(f"Cleaned up temporary file: {temp_path}")
                    except:
                        pass
                
                if os.path.exists(debug_path):
                    try:
                        os.unlink(debug_path)
                    except:
                        pass
                
                # Clean up old files
                file_handler.cleanup_old_files()
                
                # Convert DataFrame to HTML for display
                try:
                    df_html = cleaner.df.head(10).to_html(
                        classes='table table-striped', 
                        index=False,
                        na_rep=''
                    )
                except Exception as e:
                    print(f"Error creating HTML: {e}")
                    # Simple HTML fallback
                    df_html = "<div class='alert alert-warning'>"
                    df_html += "Data preview is not available. Download the cleaned file to view results."
                    df_html += "</div>"
                
                print(f"\nCleaning completed successfully!")
                print(f"Original: {cleaning_report.get('original_rows', '?')} rows")
                print(f"Cleaned: {cleaning_report.get('final_rows', '?')} rows")
                print('='*60)
                
                return render_template('results.html', 
                                     original_filename=filename,
                                     cleaned_filename=cleaned_filename,
                                     report=serializable_report,
                                     data_preview=df_html,
                                     columns=list(cleaner.df.columns))
                
            except Exception as e:
                error_msg = f'Error processing file: {str(e)}'
                print(f"\nERROR: {error_msg}")
                print(traceback.format_exc())
                
                # More user-friendly error message
                if 'empty' in str(e).lower() or 'no columns' in str(e).lower():
                    flash('The file appears to be empty or in an unexpected format. Please check if it contains valid data.')
                else:
                    flash(error_msg)
                
                return redirect(request.url)
        else:
            flash('File type not allowed. Please upload CSV, Excel, JSON, or TXT files.')
            return redirect(request.url)
    
    return render_template('index.html')

@app.route('/download/<filename>')
def download_file(filename):
    try:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        return send_file(filepath, as_attachment=True)
    except Exception as e:
        flash(f'Error downloading file: {str(e)}')
        return redirect(url_for('index'))

@app.route('/api/clean', methods=['POST'])
def api_clean():
    """API endpoint for programmatic data cleaning"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            original_path = file_handler.save_uploaded_file(file, filename)
            
            # Read file
            if filename.lower().endswith('.csv'):
                df_raw = read_csv_with_fallbacks(original_path)
                if df_raw.empty:
                    return jsonify({'error': 'Could not read CSV file'}), 400
                filepath = original_path
            else:
                filepath = original_path
            
            # Get options from JSON or form data
            if request.is_json:
                options = request.get_json()
            else:
                options = request.form.to_dict()
            
            cleaner = DataCleaner(filepath, options)
            
            if cleaner.df.empty:
                return jsonify({'error': 'File is empty'}), 400
            
            cleaning_report = cleaner.clean_data()
            
            # Convert to serializable format
            serializable_report = convert_to_serializable(cleaning_report)
            
            # Convert DataFrame to serializable format
            df_data = cleaner.df.head(100).to_dict(orient='records')
            serializable_data = convert_to_serializable(df_data)
            
            # Return cleaned data as JSON
            result = {
                'report': serializable_report,
                'data': serializable_data,
                'columns': list(cleaner.df.columns),
                'shape': [int(cleaner.df.shape[0]), int(cleaner.df.shape[1])]
            }
            
            return jsonify(result)
        else:
            return jsonify({'error': 'Invalid file type'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/preview', methods=['POST'])
def preview():
    """Preview data without cleaning"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file'}), 400
    
    file = request.files['file']
    if file and allowed_file(file.filename):
        try:
            filename = secure_filename(file.filename)
            filepath = file_handler.save_uploaded_file(file, filename)
            
            # Load data
            if filename.lower().endswith('.csv'):
                df = read_csv_with_fallbacks(filepath)
            else:
                df = file_handler.load_file(filepath)
            
            if df.empty:
                return jsonify({
                    'error': 'File is empty or could not be read',
                    'head': [],
                    'columns': [],
                    'shape': [0, 0],
                    'dtypes': {},
                    'missing_values': {}
                }), 400
            
            # Convert DataFrame to serializable format
            head_data = df.head(10).to_dict(orient='records')
            serializable_head = convert_to_serializable(head_data)
            
            preview = {
                'head': serializable_head,
                'columns': list(df.columns),
                'shape': [int(df.shape[0]), int(df.shape[1])],
                'dtypes': df.dtypes.astype(str).to_dict(),
                'missing_values': convert_to_serializable(df.isnull().sum().to_dict())
            }
            
            return jsonify(preview)
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    return jsonify({'error': 'Invalid file'}), 400

if __name__ == '__main__':
    # Ensure upload directory exists
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    print("\n" + "="*60)
    print("DATA CLEANER APP - Starting Server")
    print("="*60)
    print(f"Server URL: http://127.0.0.1:5000")
    print(f"Upload folder: {app.config['UPLOAD_FOLDER']}")
    print("="*60 + "\n")
    
    # For production, use environment variable for port
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

