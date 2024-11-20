import pandas as pd
import io
import re
from bs4 import BeautifulSoup
import unicodedata

def convert_df_to_csv(df):
    """Convert DataFrame to CSV string"""
    return df.to_csv(index=False).encode('utf-8')

def clean_job_description(description):
    """Clean and format job description text"""
    if not description:
        return ""
    
    # Remove HTML tags if present
    soup = BeautifulSoup(description, 'html.parser')
    description = soup.get_text()
    
    # Normalize unicode characters
    description = unicodedata.normalize('NFKD', description)
    
    # Remove extra whitespace and newlines
    description = " ".join(description.split())
    
    # Remove special characters but keep basic punctuation
    description = re.sub(r'[^\w\s.,!?-]', ' ', description)
    
    # Clean up bullet points and lists
    description = re.sub(r'•|\*|➢|►|▪|■', '- ', description)
    
    # Remove multiple dashes
    description = re.sub(r'-+', '-', description)
    
    # Remove multiple spaces
    description = re.sub(r'\s+', ' ', description)
    
    # Clean up common formatting issues
    description = re.sub(r'\s+([.,!?])', r'\1', description)
    
    # Ensure proper spacing after punctuation
    description = re.sub(r'([.,!?])([^\s])', r'\1 \2', description)
    
    # Trim extra whitespace
    description = description.strip()
    
    return description

def format_date(date_str):
    """Format date string to consistent format"""
    try:
        date_obj = pd.to_datetime(date_str)
        return date_obj.strftime('%Y-%m-%d')
    except:
        return None

def extract_salary_range(description):
    """Extract salary range from job description if available"""
    salary_pattern = r'\$?\d{1,3}(?:,\d{3})*(?:\s*-\s*\$?\d{1,3}(?:,\d{3})*)?(?:\s*k|\s*K|\s*per\syear|\s*\/\s*year|\s*annually)?'
    matches = re.findall(salary_pattern, description)
    return matches[0] if matches else None

def normalize_location(location):
    """Normalize location string"""
    if not location:
        return ""
    
    # Remove extra whitespace
    location = " ".join(location.split())
    
    # Remove common prefixes/suffixes
    location = re.sub(r'^Location:\s*', '', location, flags=re.IGNORECASE)
    location = re.sub(r'\s*\(.*?\)', '', location)
    
    return location.strip()
