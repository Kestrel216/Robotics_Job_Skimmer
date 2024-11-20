import pandas as pd
import io

def convert_df_to_csv(df):
    """Convert DataFrame to CSV string"""
    return df.to_csv(index=False).encode('utf-8')

def clean_job_description(description):
    """Clean and format job description text"""
    if not description:
        return ""
    
    # Remove extra whitespace
    description = " ".join(description.split())
    
    # Remove special characters
    description = description.replace('\n', ' ').replace('\r', ' ')
    
    return description

def format_date(date_str):
    """Format date string to consistent format"""
    try:
        date_obj = pd.to_datetime(date_str)
        return date_obj.strftime('%Y-%m-%d')
    except:
        return None
