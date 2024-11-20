import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import db_operations as db
import random

def scrape_boston_dynamics():
    """Scrape jobs from Boston Dynamics careers page"""
    jobs = []
    try:
        response = requests.get('https://www.bostondynamics.com/careers')
        soup = BeautifulSoup(response.text, 'html.parser')
        
        job_listings = soup.find_all('div', class_='job-listing')
        for job in job_listings:
            jobs.append({
                'title': job.find('h3').text.strip(),
                'company': 'Boston Dynamics',
                'location': job.find('div', class_='location').text.strip(),
                'description': job.find('div', class_='description').text.strip(),
                'url': 'https://www.bostondynamics.com' + job.find('a')['href'],
                'posted_date': datetime.now().date()
            })
    except Exception as e:
        print(f"Error scraping Boston Dynamics: {str(e)}")
    return jobs

def scrape_universal_robots():
    """Scrape jobs from Universal Robots careers page"""
    jobs = []
    try:
        response = requests.get('https://www.universal-robots.com/careers')
        soup = BeautifulSoup(response.text, 'html.parser')
        
        job_listings = soup.find_all('div', class_='job-posting')
        for job in job_listings:
            jobs.append({
                'title': job.find('h4').text.strip(),
                'company': 'Universal Robots',
                'location': job.find('span', class_='location').text.strip(),
                'description': job.find('div', class_='description').text.strip(),
                'url': job.find('a')['href'],
                'posted_date': datetime.now().date()
            })
    except Exception as e:
        print(f"Error scraping Universal Robots: {str(e)}")
    return jobs

def scrape_all_jobs():
    """Scrape jobs from all sources"""
    all_jobs = []
    
    # Add jobs from each source
    all_jobs.extend(scrape_boston_dynamics())
    time.sleep(random.uniform(1, 3))  # Random delay between requests
    all_jobs.extend(scrape_universal_robots())
    
    # Store in database if we have jobs
    if all_jobs:
        db.insert_jobs(all_jobs)

