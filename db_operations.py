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

        CREATE TABLE IF NOT EXISTS job_applications (
            id SERIAL PRIMARY KEY,
            job_id INTEGER REFERENCES jobs(id),
            status VARCHAR(50) NOT NULL DEFAULT 'Applied',
            application_date DATE NOT NULL DEFAULT CURRENT_DATE,
            notes TEXT,
            follow_up_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT valid_status CHECK (status IN ('Applied', 'Interview', 'Offer', 'Rejected', 'Withdrawn'))
        );

        CREATE INDEX IF NOT EXISTS idx_job_applications_job_id ON job_applications(job_id);
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

def get_jobs(include_applications=False):
    """Retrieve all jobs from the database"""
    conn = get_db_connection()
    
    if include_applications:
        query = """
            SELECT j.id, j.title, j.company, j.location, j.description, j.url, j.posted_date,
                   ja.id as application_id, ja.status, ja.application_date, ja.notes, ja.follow_up_date
            FROM jobs j
            LEFT JOIN job_applications ja ON j.id = ja.job_id
            ORDER BY j.posted_date DESC, j.company
        """
    else:
        query = """
            SELECT id, title, company, location, description, url, posted_date
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

def add_job_application(job_id, status='Applied', notes=None, follow_up_date=None):
    """Add a new job application"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO job_applications (job_id, status, notes, follow_up_date)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, (job_id, status, notes, follow_up_date))
    
    application_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return application_id

def update_job_application(application_id, status=None, notes=None, follow_up_date=None):
    """Update an existing job application"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    update_parts = []
    values = []
    
    if status is not None:
        update_parts.append("status = %s")
        values.append(status)
    if notes is not None:
        update_parts.append("notes = %s")
        values.append(notes)
    if follow_up_date is not None:
        update_parts.append("follow_up_date = %s")
        values.append(follow_up_date)
    
    if update_parts:
        update_parts.append("updated_at = CURRENT_TIMESTAMP")
        query = f"""
            UPDATE job_applications 
            SET {', '.join(update_parts)}
            WHERE id = %s
        """
        values.append(application_id)
        
        cur.execute(query, values)
        conn.commit()
    
    cur.close()
    conn.close()

def get_application_statistics():
    """Get statistics about job applications"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            status,
            COUNT(*) as count
        FROM job_applications
        GROUP BY status
    """)
    
    stats = dict(cur.fetchall())
    
    cur.close()
    conn.close()
    return stats

# Initialize database when module is imported
init_db()
