import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

def generate_uncleaned_dataset(num_records=100, output_file='uncleaned_dataset.csv'):
    """
    Generate a dataset with various data quality issues
    """
    
    # Lists for generating random data
    first_names = ['John', 'Jane', 'Bob', 'Alice', 'Carol', 'David', 'Emma', 'Frank', 'Grace', 'Henry',
                   'Ivy', 'Jack', 'Kate', 'Leo', 'Mia', 'Noah', 'Olivia', 'Peter', 'Quinn', 'Ryan',
                   'Sara', 'Tom', 'Ursula', 'Victor', 'Wendy', 'Xavier', 'Yvonne', 'Zachary']
    
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                  'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
                  'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin']
    
    departments = ['IT', 'HR', 'Sales', 'Marketing', 'Finance', 'Management', 'Operations', 'R&D']
    
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio',
              'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville', 'Fort Worth', 'Columbus',
              'Charlotte', 'San Francisco', 'Indianapolis', 'Seattle', 'Denver', 'Washington']
    
    # Define data issues
    data_issues = {
        'missing_values': 0.1,  # 10% missing
        'duplicates': 0.05,     # 5% duplicates
        'inconsistent_format': 0.2,  # 20% inconsistent
        'outliers': 0.03,       # 3% outliers
        'extra_spaces': 0.15,   # 15% extra spaces
        'mixed_case': 0.25,     # 25% mixed case
        'date_formats': 0.3,    # 30% different date formats
        'special_characters': 0.1  # 10% special chars
    }
    
    data = []
    
    for i in range(1, num_records + 1):
        # Generate base data
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        age = random.randint(22, 65)
        salary = random.randint(40000, 120000)
        department = random.choice(departments)
        city = random.choice(cities)
        join_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))
        rating = round(random.uniform(2.0, 5.0), 1)
        phone = f"{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        # Apply data issues
        if random.random() < data_issues['missing_values']:
            if random.random() < 0.5:
                age = np.nan
            else:
                phone = ''
        
        if random.random() < data_issues['extra_spaces']:
            first_name = f"  {first_name}  "
            last_name = f"{last_name}  "
        
        if random.random() < data_issues['mixed_case']:
            if random.random() < 0.5:
                first_name = first_name.upper()
                last_name = last_name.lower()
            else:
                first_name = first_name.title()
                last_name = last_name.upper()
        
        # Email with occasional issues
        if random.random() < 0.05:
            email = f"{first_name.lower().strip()}@{last_name.lower().strip()}.com"
        elif random.random() < 0.02:
            email = ''
        else:
            email = f"{first_name.lower().strip()}.{last_name.lower().strip()}@example.com"
        
        # Date format inconsistencies
        date_formats = [
            join_date.strftime('%Y-%m-%d'),      # ISO format
            join_date.strftime('%d/%m/%Y'),      # European
            join_date.strftime('%m/%d/%Y'),      # US
            join_date.strftime('%d-%m-%Y'),      # European with dashes
            join_date.strftime('%m-%d-%Y'),      # US with dashes
            join_date.strftime('%Y%m%d'),        # Compact
            join_date.strftime('%B %d, %Y'),     # Full month
        ]
        
        if random.random() < data_issues['date_formats']:
            join_date_str = random.choice(date_formats[1:])  # Skip ISO format
        else:
            join_date_str = date_formats[0]
        
        # Salary outliers
        if random.random() < data_issues['outliers']:
            salary = random.choice([1000, 5000, 10000, 500000, 1000000])
        
        # Comments with special characters
        comments = []
        if random.random() < 0.7:
            comments.append(random.choice([
                'Good employee',
                'Needs improvement',
                'Hard working',
                'Team player',
                'Excellent performance',
                'Average performance',
                'New hire',
                'Experienced'
            ]))
        
        if random.random() < data_issues['special_characters']:
            comments.append(random.choice([
                'Special@characters#here',
                'Multiple  spaces',
                'Line\nbreak',
                'Tab\tcharacter',
                'Emoji 😊 included',
                'UTF-8: café, naïve, résumé'
            ]))
        
        comment = ', '.join(comments) if comments else ''
        
        # Create record
        record = {
            'Employee_ID': i,
            'First Name': first_name,
            'Last Name': last_name,
            'Full Name': f"{first_name} {last_name}",
            'Age': age,
            'Email Address': email,
            'Salary': salary,
            'Annual Salary (USD)': salary,
            'Join Date': join_date_str,
            'Department': department,
            'City/Location': city,
            'Phone Number': phone,
            'Performance_Rating': rating,
            'Comments': comment,
            'Active': random.choice(['Yes', 'No', 'TRUE', 'FALSE', '1', '0', 'Y', 'N']),
            'Manager': random.choice(['John Smith', 'Jane Doe', 'Robert Johnson', '', 'Not Assigned', 'TBD'])
        }
        
        data.append(record)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Add duplicate records
    num_duplicates = int(num_records * data_issues['duplicates'])
    if num_duplicates > 0:
        duplicates = df.sample(n=num_duplicates, replace=True)
        df = pd.concat([df, duplicates], ignore_index=True)
        df['Employee_ID'] = range(1, len(df) + 1)
    
    # Add some inconsistent column names
    df.rename(columns={
        'Salary': 'salary',
        'Performance_Rating': 'Rating (1-5)'
    }, inplace=True)
    
    # Add some empty columns
    df['Empty_Column'] = ''
    df['Another Empty Column'] = np.nan
    
    # Add column with mostly null values (>50%)
    df['Rarely_Used_Column'] = np.nan
    mask = np.random.rand(len(df)) < 0.3  # Only 30% filled
    df.loc[mask, 'Rarely_Used_Column'] = random.choices(['A', 'B', 'C'], k=mask.sum())
    
    # Save to file
    output_path = os.path.join('static', 'uploads', output_file)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save in multiple formats for testing
    df.to_csv(output_path, index=False)
    df.to_excel(output_path.replace('.csv', '.xlsx'), index=False)
    df.to_json(output_path.replace('.csv', '.json'), orient='records', indent=2)
    
    print(f"Generated {len(df)} records with data quality issues")
    print(f"Files saved:")
    print(f"  - CSV: {output_path}")
    print(f"  - Excel: {output_path.replace('.csv', '.xlsx')}")
    print(f"  - JSON: {output_path.replace('.csv', '.json')}")
    
    # Print data quality issues summary
    print("\nData Quality Issues Included:")
    print(f"  • Missing values: {df.isnull().sum().sum()} total")
    print(f"  • Duplicate rows: {df.duplicated().sum()}")
    print(f"  • Inconsistent date formats: {len(set(df['Join Date'].str.len())) > 1}")
    print(f"  • Salary outliers: {(df['salary'] < 20000).sum() + (df['salary'] > 200000).sum()}")
    print(f"  • Inconsistent boolean values: {df['Active'].nunique()} different values for Active")
    
    return df

if __name__ == '__main__':
    # Generate dataset
    df = generate_uncleaned_dataset(num_records=50)
    
    # Display sample
    print("\nSample of generated data (first 5 rows):")
    print(df.head().to_string())
    
    print("\nColumns in dataset:")
    print(list(df.columns))