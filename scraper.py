import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import db_operations as db
import random
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import utils
import trafilatura
from urllib.parse import urljoin, urlparse
import threading
import queue
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

# User agent rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
]

# Company configurations
COMPANIES = {
    'Agility Robotics': {
        'url': 'https://www.agilityrobotics.com/careers',
        'platform': 'greenhouse',
        'board_id': 'agilityrobotics'
    },
    'ANYbotics': {
        'url': 'https://www.anybotics.com/careers',
        'platform': 'lever',
        'board_id': 'anybotics'
    },
    'Aurora': {
        'url': 'https://www.aurora.tech/careers',
        'platform': 'greenhouse',
        'board_id': 'aurora'
    },
    'Berkshire Grey': {
        'url': 'https://www.berkshiregrey.com/careers',
        'platform': 'workday',
        'board_id': 'berkshiregrey'
    }
}

# Global cancel flag for scraping
cancel_scraping = threading.Event()

class RateLimiter:
    def __init__(self, requests_per_minute=10):
        self.requests_per_minute = requests_per_minute
        self.last_request = 0
        self.minimum_interval = 60.0 / requests_per_minute

    def wait(self):
        now = time.time()
        elapsed = now - self.last_request
        if elapsed < self.minimum_interval:
            time.sleep(self.minimum_interval - elapsed)
        self.last_request = time.time()

rate_limiter = RateLimiter()

def get_session(maintain_session=False):
    """Create a session with retry mechanism"""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.headers['User-Agent'] = random.choice(USER_AGENTS)
    
    if maintain_session:
        session.cookies.set('session_id', 'dummy_session')
    
    return session

def make_request(url, session=None, timeout=30):
    """Make a request with rate limiting and timeout"""
    if cancel_scraping.is_set():
        return None
        
    rate_limiter.wait()
    try:
        if session is None:
            session = get_session()
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {url}: {str(e)}")
        return None

def extract_job_description(url, session=None):
    """Extract job description using trafilatura"""
    if cancel_scraping.is_set():
        return None
        
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(downloaded)
            return utils.clean_job_description(text) if text else None
    except Exception as e:
        logging.error(f"Error extracting content from {url}: {str(e)}")
    return None

def scrape_greenhouse_jobs(company_name, board_id):
    """Scrape jobs from Greenhouse job boards"""
    jobs = []
    session = get_session()
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{board_id}/jobs"
    
    try:
        response = make_request(api_url, session=session)
        if not response:
            return jobs

        data = response.json()
        for job in data.get('jobs', []):
            if cancel_scraping.is_set():
                break
                
            try:
                description = extract_job_description(job['absolute_url'], session)
                jobs.append({
                    'title': job['title'],
                    'company': company_name,
                    'location': job.get('location', {}).get('name', 'Remote'),
                    'description': description,
                    'url': job['absolute_url'],
                    'posted_date': datetime.strptime(job['updated_at'][:10], '%Y-%m-%d').date()
                })
                logging.info(f"Successfully scraped {company_name} job: {job['title']}")
            except Exception as e:
                logging.error(f"Error parsing {company_name} job listing: {str(e)}")
                
    except Exception as e:
        logging.error(f"Error scraping {company_name}: {str(e)}")
    return jobs

def scrape_lever_jobs(company_name, board_id):
    """Scrape jobs from Lever job boards"""
    jobs = []
    session = get_session()
    api_url = f"https://api.lever.co/v0/postings/{board_id}"
    
    try:
        response = make_request(api_url, session=session)
        if not response:
            return jobs

        data = response.json()
        for job in data:
            if cancel_scraping.is_set():
                break
                
            try:
                description = extract_job_description(job['hostedUrl'], session)
                jobs.append({
                    'title': job['text'],
                    'company': company_name,
                    'location': job.get('categories', {}).get('location', 'Remote'),
                    'description': description,
                    'url': job['hostedUrl'],
                    'posted_date': datetime.now().date()  # Lever API doesn't provide posting date
                })
                logging.info(f"Successfully scraped {company_name} job: {job['text']}")
            except Exception as e:
                logging.error(f"Error parsing {company_name} job listing: {str(e)}")
                
    except Exception as e:
        logging.error(f"Error scraping {company_name}: {str(e)}")
    return jobs

def scrape_workday_jobs(company_name, board_id):
    """Scrape jobs from Workday career sites"""
    jobs = []
    session = get_session(maintain_session=True)
    
    try:
        url = f"https://{board_id}.wd1.myworkdayjobs.com/en-US/External"
        response = make_request(url, session=session)
        if not response:
            return jobs

        soup = BeautifulSoup(response.text, 'html.parser')
        job_listings = soup.find_all('li', {'class': 'job-listing'})
        
        for job in job_listings:
            if cancel_scraping.is_set():
                break
                
            try:
                job_url = urljoin(url, job.find('a')['href'])
                description = extract_job_description(job_url, session)
                
                jobs.append({
                    'title': job.find('h3').text.strip(),
                    'company': company_name,
                    'location': job.find('span', {'class': 'location'}).text.strip(),
                    'description': description,
                    'url': job_url,
                    'posted_date': datetime.now().date()
                })
                logging.info(f"Successfully scraped {company_name} job: {jobs[-1]['title']}")
            except Exception as e:
                logging.error(f"Error parsing {company_name} job listing: {str(e)}")
                
    except Exception as e:
        logging.error(f"Error scraping {company_name}: {str(e)}")
    return jobs

def scrape_boston_dynamics():
    """Scrape jobs from Boston Dynamics careers page"""
    jobs = []
    session = get_session(maintain_session=True)
    
    try:
        response = make_request('https://bostondynamics.wd1.myworkdayjobs.com/en-US/Boston_Dynamics/jobs', session=session)
        if not response:
            return jobs

        soup = BeautifulSoup(response.text, 'html.parser')
        job_listings = soup.find_all('li', {'class': 'job-listing'})
        
        for job in job_listings:
            if cancel_scraping.is_set():
                break
                
            try:
                job_url = urljoin('https://bostondynamics.wd1.myworkdayjobs.com', job.find('a')['href'])
                description = extract_job_description(job_url, session)
                
                jobs.append({
                    'title': job.find('h3').text.strip(),
                    'company': 'Boston Dynamics',
                    'location': job.find('span', {'class': 'location'}).text.strip(),
                    'description': description,
                    'url': job_url,
                    'posted_date': datetime.now().date()
                })
                logging.info(f"Successfully scraped job: {jobs[-1]['title']}")
            except Exception as e:
                logging.error(f"Error parsing Boston Dynamics job listing: {str(e)}")
                
    except Exception as e:
        logging.error(f"Error scraping Boston Dynamics: {str(e)}")
    return jobs

def scrape_linkedin_robotics():
    """Scrape robotics jobs from LinkedIn with improved rate limiting"""
    jobs = []
    session = get_session()
    rate_limiter.requests_per_minute = 5
    
    try:
        url = 'https://www.linkedin.com/jobs/search?keywords=robotics&location=United%20States'
        response = make_request(url, session=session)
        if not response:
            return jobs

        soup = BeautifulSoup(response.text, 'html.parser')
        job_listings = soup.find_all('div', {'class': 'base-card'})
        
        for job in job_listings[:10]:
            if cancel_scraping.is_set():
                break
                
            try:
                job_url = job.find('a', {'class': 'base-card__full-link'})['href']
                description = extract_job_description(job_url, session)
                
                jobs.append({
                    'title': job.find('h3', {'class': 'base-search-card__title'}).text.strip(),
                    'company': job.find('h4', {'class': 'base-search-card__subtitle'}).text.strip(),
                    'location': job.find('span', {'class': 'job-search-card__location'}).text.strip(),
                    'description': description,
                    'url': job_url,
                    'posted_date': datetime.now().date()
                })
                logging.info(f"Successfully scraped job: {jobs[-1]['title']}")
            except Exception as e:
                logging.error(f"Error parsing LinkedIn job listing: {str(e)}")
                
    except Exception as e:
        logging.error(f"Error scraping LinkedIn: {str(e)}")
    return jobs

def scrape_all_jobs():
    """Scrape jobs from all sources with timeout"""
    all_jobs = []
    cancel_scraping.clear()
    
    # Standard sources
    sources = [
        scrape_boston_dynamics,
        scrape_linkedin_robotics
    ]
    
    # Add company-specific scrapers based on platform
    for company, config in COMPANIES.items():
        if config['platform'] == 'greenhouse':
            sources.append(lambda c=company, b=config['board_id']: scrape_greenhouse_jobs(c, b))
        elif config['platform'] == 'lever':
            sources.append(lambda c=company, b=config['board_id']: scrape_lever_jobs(c, b))
        elif config['platform'] == 'workday':
            sources.append(lambda c=company, b=config['board_id']: scrape_workday_jobs(c, b))
    
    for scraper_func in sources:
        if cancel_scraping.is_set():
            break
            
        try:
            logging.info(f"Starting scraping from {scraper_func.__name__ if hasattr(scraper_func, '__name__') else 'company source'}")
            jobs = scraper_func()
            all_jobs.extend(jobs)
            logging.info(f"Successfully scraped {len(jobs)} jobs from source")
            time.sleep(random.uniform(2, 5))
        except Exception as e:
            logging.error(f"Error in scraping source: {str(e)}")
    
    if all_jobs:
        try:
            db.insert_jobs(all_jobs)
            logging.info(f"Successfully inserted {len(all_jobs)} jobs into database")
        except Exception as e:
            logging.error(f"Error inserting jobs into database: {str(e)}")
    else:
        logging.warning("No jobs were scraped from any source")
    
    return len(all_jobs)
