import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import db_operations as db
import scraper
import utils
import threading

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

# Cache the job results
@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_cached_jobs():
    return db.get_jobs()

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

def main():
    st.title("🤖 Robotics Job Aggregator")
    
    # Sidebar
    st.sidebar.title("Controls")
    
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
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        companies = db.get_unique_companies()
        selected_companies = st.multiselect("Filter by Company", companies)
    
    with col2:
        search_query = st.text_input("Search Jobs", "")

    # Get and display jobs
    try:
        jobs_df = get_cached_jobs()
        
        # Apply filters
        if selected_companies:
            jobs_df = jobs_df[jobs_df['company'].isin(selected_companies)]
        if search_query:
            jobs_df = jobs_df[
                jobs_df['title'].str.contains(search_query, case=False) |
                jobs_df['description'].str.contains(search_query, case=False)
            ]

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
                st.write(f"**Location:** {job['location']}")
                st.write(f"**Posted:** {job['posted_date']}")
                st.write(f"**Description:**\n{job['description']}")
                if job['url']:
                    st.markdown(f"[Apply Here]({job['url']})")
    except Exception as e:
        st.error(f"Error loading jobs: {str(e)}")

if __name__ == "__main__":
    main()
