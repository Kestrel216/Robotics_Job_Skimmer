import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import db_operations as db
import scraper
import utils

# Page config
st.set_page_config(
    page_title="Robotics Job Aggregator",
    page_icon="🤖",
    layout="wide"
)

# Initialize session state
if 'last_update' not in st.session_state:
    st.session_state.last_update = None

def main():
    st.title("🤖 Robotics Job Aggregator")
    
    # Sidebar
    st.sidebar.title("Controls")
    if st.sidebar.button("Refresh Data"):
        with st.spinner("Fetching fresh job data..."):
            scraper.scrape_all_jobs()
            st.session_state.last_update = datetime.now()
        st.success("Data updated successfully!")
    
    if st.session_state.last_update:
        st.sidebar.write(f"Last updated: {st.session_state.last_update.strftime('%Y-%m-%d %H:%M:%S')}")

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        companies = db.get_unique_companies()
        selected_companies = st.multiselect("Filter by Company", companies)
    
    with col2:
        search_query = st.text_input("Search Jobs", "")

    # Get and display jobs
    jobs_df = db.get_jobs()
    
    # Apply filters
    if selected_companies:
        jobs_df = jobs_df[jobs_df['company'].isin(selected_companies)]
    if search_query:
        jobs_df = jobs_df[
            jobs_df['title'].str.contains(search_query, case=False) |
            jobs_df['description'].str.contains(search_query, case=False)
        ]

    # Display jobs
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

if __name__ == "__main__":
    main()
