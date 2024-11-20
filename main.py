import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import db_operations as db
import scraper
import utils
import threading
import re

# Page config
st.set_page_config(
    page_title="Robotics Job Aggregator",
    page_icon="🤖",
    layout="wide"
)

# Initialize session state
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'is_scraping' not in st.session_state:
    st.session_state.is_scraping = False
if 'scraping_error' not in st.session_state:
    st.session_state.scraping_error = None
if 'show_applications' not in st.session_state:
    st.session_state.show_applications = False

# Cache the job results
@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_cached_jobs(include_applications=False):
    return db.get_jobs(include_applications)

def extract_experience_level(description):
    """Extract experience level from job description"""
    if not description:
        return 'Not Specified'
    description = str(description).lower()
    if any(senior in description for senior in ['senior', 'sr.', 'lead', 'staff', 'principal']):
        return 'Senior'
    elif any(mid in description for mid in ['mid-level', 'mid level', '3+ years', '3-5 years']):
        return 'Mid-Level'
    elif any(junior in description for junior in ['junior', 'entry', 'entry-level', '0-2 years', 'fresh']):
        return 'Junior'
    return 'Not Specified'

def extract_job_type(title, description):
    """Extract job type from title and description"""
    text = (title + ' ' + (description or '')).lower()
    if any(sw in text for sw in ['software', 'developer', 'python', 'java', 'programming']):
        return 'Software'
    elif any(hw in text for hw in ['hardware', 'electrical', 'electronics']):
        return 'Hardware'
    elif any(mech in text for mech in ['mechanical', 'mech', 'manufacturing']):
        return 'Mechanical'
    elif any(ai in text for ai in ['ai', 'machine learning', 'deep learning', 'ml']):
        return 'AI/ML'
    return 'Other'

def is_remote(location, description):
    """Check if job is remote"""
    text = ((location or '') + ' ' + (description or '')).lower()
    return any(remote in text for remote in ['remote', 'work from home', 'wfh', 'virtual'])

def cancel_scraping():
    """Cancel the ongoing scraping process"""
    scraper.cancel_scraping.set()
    st.session_state.is_scraping = False
    st.session_state.scraping_error = "Scraping cancelled by user"

def refresh_data():
    """Refresh job data with proper error handling"""
    st.session_state.is_scraping = True
    st.session_state.scraping_error = None
    scraper.cancel_scraping.clear()
    
    try:
        jobs_count = scraper.scrape_all_jobs()
        if jobs_count > 0:
            st.session_state.last_update = datetime.now()
            st.success(f"Successfully scraped {jobs_count} jobs!")
        else:
            st.warning("No new jobs found. Please try again later.")
    except Exception as e:
        st.session_state.scraping_error = f"Error during scraping: {str(e)}"
        st.error(st.session_state.scraping_error)
    finally:
        st.session_state.is_scraping = False
        get_cached_jobs.clear()  # Clear the cache to get fresh data

def show_application_statistics():
    """Display application statistics"""
    stats = db.get_application_statistics()
    
    st.sidebar.title("Application Statistics")
    total = sum(stats.values())
    
    if total > 0:
        st.sidebar.write(f"Total Applications: {total}")
        for status, count in stats.items():
            percentage = (count / total) * 100
            st.sidebar.write(f"{status}: {count} ({percentage:.1f}%)")
    else:
        st.sidebar.write("No applications yet")

def main():
    st.title("🤖 Robotics Job Aggregator")
    
    # Sidebar
    st.sidebar.title("Controls")
    
    # Application tracking toggle
    st.session_state.show_applications = st.sidebar.checkbox("Show My Applications", value=st.session_state.show_applications)
    
    if st.session_state.show_applications:
        show_application_statistics()
    
    # Refresh button and cancel button
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("Refresh Data", disabled=st.session_state.is_scraping):
            refresh_data()
    with col2:
        if st.button("Cancel", disabled=not st.session_state.is_scraping):
            cancel_scraping()
    
    # Show loading state
    if st.session_state.is_scraping:
        st.sidebar.markdown("⏳ Fetching fresh job data...")
    
    # Show last update time
    if st.session_state.last_update:
        st.sidebar.write(f"Last successful update: {st.session_state.last_update.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Show error message if any
    if st.session_state.scraping_error:
        st.sidebar.error(st.session_state.scraping_error)
    
    # Advanced Filters
    st.sidebar.title("Advanced Filters")
    
    # Date Range Filter
    date_filter = st.sidebar.selectbox(
        "Posted Date",
        ["All Time", "Last 24 Hours", "Last 7 Days", "Last 30 Days"]
    )
    
    # Experience Level Filter
    experience_level = st.sidebar.multiselect(
        "Experience Level",
        ["Junior", "Mid-Level", "Senior", "Not Specified"]
    )
    
    # Job Type Filter
    job_type = st.sidebar.multiselect(
        "Job Category",
        ["Software", "Hardware", "Mechanical", "AI/ML", "Other"]
    )
    
    # Remote Filter
    remote_filter = st.sidebar.radio(
        "Work Type",
        ["All", "Remote Only", "On-site Only"]
    )
    
    # Main content area
    st.subheader("Search and Filter Jobs")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        companies = db.get_unique_companies()
        selected_companies = st.multiselect("Filter by Company", companies)
    
    with col2:
        locations = ["All Locations", "United States", "Europe", "Asia", "Remote"]
        selected_location = st.selectbox("Filter by Location", locations)
    
    with col3:
        search_query = st.text_input("Search Jobs", "")

    # Get and display jobs
    try:
        jobs_df = get_cached_jobs(include_applications=st.session_state.show_applications)
        
        # Apply filters
        if selected_companies:
            jobs_df = jobs_df[jobs_df['company'].isin(selected_companies)]
        
        if search_query:
            jobs_df = jobs_df[
                jobs_df['title'].str.contains(search_query, case=False, na=False) |
                jobs_df['description'].str.contains(search_query, case=False, na=False)
            ]
        
        # Date filter
        if date_filter != "All Time":
            today = datetime.now().date()
            if date_filter == "Last 24 Hours":
                jobs_df = jobs_df[jobs_df['posted_date'] >= (today - timedelta(days=1))]
            elif date_filter == "Last 7 Days":
                jobs_df = jobs_df[jobs_df['posted_date'] >= (today - timedelta(days=7))]
            elif date_filter == "Last 30 Days":
                jobs_df = jobs_df[jobs_df['posted_date'] >= (today - timedelta(days=30))]
        
        # Add derived columns for filtering
        jobs_df['experience_level'] = jobs_df['description'].apply(extract_experience_level)
        jobs_df['job_type'] = jobs_df.apply(lambda x: extract_job_type(x['title'], x['description']), axis=1)
        jobs_df['is_remote'] = jobs_df.apply(lambda x: is_remote(x['location'], x['description']), axis=1)
        
        # Apply advanced filters
        if experience_level:
            jobs_df = jobs_df[jobs_df['experience_level'].isin(experience_level)]
        
        if job_type:
            jobs_df = jobs_df[jobs_df['job_type'].isin(job_type)]
        
        if remote_filter == "Remote Only":
            jobs_df = jobs_df[jobs_df['is_remote']]
        elif remote_filter == "On-site Only":
            jobs_df = jobs_df[~jobs_df['is_remote']]
        
        if selected_location != "All Locations":
            if selected_location == "Remote":
                jobs_df = jobs_df[jobs_df['is_remote']]
            else:
                jobs_df = jobs_df[jobs_df['location'].str.contains(selected_location, case=False, na=False)]

        # Display jobs count
        st.write(f"Found {len(jobs_df)} jobs")
        
        # Export functionality
        if not jobs_df.empty:
            csv = utils.convert_df_to_csv(jobs_df)
            st.download_button(
                label="Download Jobs as CSV",
                data=csv,
                file_name=f"robotics_jobs_{datetime.now().strftime('%Y%m%d')}.csv",
                mime='text/csv'
            )

        # Display job listings
        for _, job in jobs_df.iterrows():
            with st.expander(f"{job['title']} at {job['company']}"):
                cols = st.columns(4)
                with cols[0]:
                    st.write(f"**Location:** {job['location']}")
                with cols[1]:
                    st.write(f"**Type:** {job['job_type']}")
                with cols[2]:
                    st.write(f"**Level:** {job['experience_level']}")
                with cols[3]:
                    st.write(f"**Remote:** {'Yes' if job['is_remote'] else 'No'}")
                
                st.write(f"**Posted:** {job['posted_date']}")
                st.write(f"**Description:**\n{job['description']}")
                if job['url']:
                    st.markdown(f"[Apply Here]({job['url']})")
                
                # Application tracking section
                if st.session_state.show_applications:
                    st.write("---")
                    st.write("**Application Tracking**")
                    
                    # Show existing application or add new one
                    if pd.notna(job.get('application_id')):
                        status = st.selectbox("Status", 
                            ['Applied', 'Interview', 'Offer', 'Rejected', 'Withdrawn'],
                            key=f"status_{job['id']}",
                            index=['Applied', 'Interview', 'Offer', 'Rejected', 'Withdrawn'].index(job['status'])
                        )
                        notes = st.text_area("Notes", 
                            value=job.get('notes', ''),
                            key=f"notes_{job['id']}"
                        )
                        follow_up = st.date_input("Follow-up Date",
                            value=job.get('follow_up_date'),
                            key=f"follow_up_{job['id']}"
                        )
                        
                        if st.button("Update Application", key=f"update_{job['id']}"):
                            db.update_job_application(
                                job['application_id'],
                                status=status,
                                notes=notes,
                                follow_up_date=follow_up
                            )
                            st.success("Application updated!")
                            st.experimental_rerun()
                    else:
                        if st.button("Track Application", key=f"track_{job['id']}"):
                            db.add_job_application(job['id'])
                            st.success("Application added to tracking!")
                            st.experimental_rerun()

    except Exception as e:
        st.error(f"Error loading jobs: {str(e)}")

if __name__ == "__main__":
    main()
