import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime
import json

def get_db_connection():
    """Create database connection using environment variables"""
    return psycopg2.connect(
        host=os.environ['PGHOST'],
        database=os.environ['PGDATABASE'],
        user=os.environ['PGUSER'],
        password=os.environ['PGPASSWORD'],
        port=os.environ['PGPORT']
    )

def init_db():
    """Initialize the database tables"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            company TEXT NOT NULL,
            location TEXT,
            description TEXT,
            url TEXT,
            posted_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_company ON jobs(company);
        CREATE INDEX IF NOT EXISTS idx_posted_date ON jobs(posted_date);
    """)
    
    conn.commit()
    cur.close()
    conn.close()

def insert_jobs(jobs_data):
    """Insert new job listings into the database"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Convert jobs_data to list of tuples
    values = [(
        job['title'],
        job['company'],
        job['location'],
        job['description'],
        job['url'],
        job['posted_date']
    ) for job in jobs_data]
    
    execute_values(cur, """
        INSERT INTO jobs (title, company, location, description, url, posted_date)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, values)
    
    conn.commit()
    cur.close()
    conn.close()

def get_jobs():
    """Retrieve all jobs from the database"""
    conn = get_db_connection()
    query = """
        SELECT title, company, location, description, url, posted_date
        FROM jobs
        ORDER BY posted_date DESC, company
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_unique_companies():
    """Get list of unique companies"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT DISTINCT company FROM jobs ORDER BY company")
    companies = [row[0] for row in cur.fetchall()]
    
    cur.close()
    conn.close()
    return companies

# Initialize database when module is imported
init_db()
