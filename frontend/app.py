import streamlit as st
import requests
import pandas as pd
from typing import Dict, Any
from datetime import datetime
import time

def init_page_config():
    """Initialize page configuration and styling"""
    st.set_page_config(
        page_title="SEC Data Analysis Tool",
        layout="wide",
        initial_sidebar_state="auto"
    )
    
    # Keeping all the original CSS styling
    st.markdown("""
        <style>
            .stApp {
                background-color: #1a1a1a;
                color: #ffffff;
            }
            
            .stTabs [data-baseweb="tab-list"] {
                gap: 8px;
                background-color: #2d2d2d;
                padding: 10px;
                border-radius: 8px;
            }
            
            .stTabs [data-baseweb="tab"] {
                height: 50px;
                padding: 10px 20px;
                color: #ffffff;
                background-color: #2d2d2d;
            }
            
            h1 {
                font-size: 32px;
                font-weight: 600;
                margin: 20px 0;
                color: #ffffff;
            }
            
            .stSelectbox > div > div,
            .stTextArea > div > div > textarea {
                background-color: #2d3140;
                color: #ffffff;
                border: 1px solid #404040;
                border-radius: 6px;
                padding: 12px;
            }
            
            .stButton > button {
                background-color: #2196f3;
                color: white;
                border: none;
                border-radius: 6px;
                padding: 12px 24px;
                font-weight: 500;
                transition: background-color 0.3s;
            }
            
            .stButton > button:hover {
                background-color: #1976d2;
            }
            
            .status-card {
                background-color: #2d3140;
                padding: 16px;
                border-radius: 8px;
                margin: 10px 0;
                border: 1px solid #404040;
            }
            
            .footer {
                position: fixed;
                bottom: 0;
                left: 0;
                width: 100%;
                background-color: #2d2d2d;
                color: #ffffff;
                text-align: center;
                padding: 12px;
                font-size: 14px;
                border-top: 1px solid #404040;
            }
        </style>
    """, unsafe_allow_html=True)

def handle_api_request(endpoint: str, method: str = "POST", payload: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Handle API requests with proper error handling
    
    Args:
        endpoint: API endpoint
        method: HTTP method
        payload: Request payload
        
    Returns:
        API response data
    """
    try:
        # Updated base URL to match FastAPI endpoints
        url = f"http://127.0.0.1:8000/{endpoint}"
        
        if method == "POST":
            response = requests.post(url, json=payload, timeout=30)
        else:
            response = requests.get(url, timeout=30)
            
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API request failed: {str(e)}")
        return None
    except ValueError as e:
        st.error(f"Invalid response format: {str(e)}")
        return None

def check_extraction_status(run_id: str) -> Dict[str, Any]:
    """Check the status of an extraction run"""
    return handle_api_request(f"status/{run_id}", method="GET")

def get_active_runs() -> Dict[str, Any]:
    """Get all active extraction runs"""
    return handle_api_request("runs", method="GET")

def display_status_card(run_data: Dict[str, Any]):
    """Display a status card for an extraction run"""
    status_color = {
        "success": "🟢",
        "running": "🟡",
        "failed": "🔴",
        "queued": "⚪"
    }.get(run_data.get("status", "").lower(), "⚪")
    
    st.markdown(f"""
        <div class="status-card">
            <h4>{status_color} Run ID: {run_data.get('run_id')}</h4>
            <p>Status: {run_data.get('status')}</p>
            <p>Start Time: {run_data.get('start_date', 'N/A')}</p>
            <p>End Time: {run_data.get('end_date', 'N/A')}</p>
        </div>
    """, unsafe_allow_html=True)

def render_data_extraction_tab():
    """Render the data extraction tab content"""
    st.title("SEC Data Extractor")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        current_year = datetime.now().year
        year = st.selectbox(
            "Select Year",
            list(range(2006, current_year + 1)),
            index=len(range(2006, current_year + 1)) - 1
        )
        quarter = st.selectbox("Select Quarter", ["Q1", "Q2", "Q3", "Q4"])
        
        if st.button("Extract Data", type="primary"):
            with st.spinner("Triggering extraction pipeline..."):
                # Updated to match your FastAPI endpoint
                result = handle_api_request("extract", payload={
                    "year": year,
                    "quarter": quarter
                })
                
                if result:
                    st.success(f"Pipeline triggered successfully! Run ID: {result.get('run_id')}")
                    st.session_state['current_run_id'] = result.get('run_id')
        
        # Status tracking section
        # if 'current_run_id' in st.session_state:
        #     st.subheader("Current Extraction Status")
        #     status = check_extraction_status(st.session_state['current_run_id'])
        #     if status:
        #         display_status_card(status)
    
    # with col2:
    #     st.subheader("Active Extractions")
    #     active_runs = get_active_runs()
    #     if active_runs and active_runs.get('active_runs'):
    #         for run in active_runs['active_runs']:
    #             display_status_card(run)
    #     else:
    #         st.info("No active extraction pipelines")

def render_query_execution_tab():
    """Render the query execution tab content"""
    st.title("SQL Query Executor")
    
    # Keeping the same sample queries
    SAMPLE_QUERIES = {
        "Basic Company Information": {
            "description": "Get basic information about company submissions",
            "query": """
                SELECT 
                    adsh, 
                    name, 
                    cik, 
                    period 
                FROM SUB 
                ORDER BY period DESC 
                LIMIT 10;
            """
        },
        "Financial Metrics": {
            "description": "View key financial metrics",
            "query": """
                SELECT 
                    n.adsh,
                    s.name,
                    n.tag,
                    n.value,
                    n.ddate 
                FROM NUM n
                JOIN SUB s ON n.adsh = s.adsh
                WHERE n.tag IN ('Assets', 'Revenues', 'NetIncomeLoss')
                ORDER BY n.ddate DESC
                LIMIT 10;
            """
        }
    }
    
    query_choice = st.radio(
        "Choose Query Type",
        ["Sample Queries", "Custom Query"],
        horizontal=True
    )
    
    if query_choice == "Sample Queries":
        selected_query = st.selectbox(
            "Select a Sample Query",
            list(SAMPLE_QUERIES.keys())
        )
        if selected_query:
            st.info(SAMPLE_QUERIES[selected_query]["description"])
            query = st.text_area(
                "SQL Query",
                value=SAMPLE_QUERIES[selected_query]["query"].strip(),
                height=200
            )
    else:
        query = st.text_area(
            "Enter Custom SQL Query",
            height=200,
            placeholder="SELECT * FROM table_name WHERE condition;"
        )
    
    if st.button("Execute Query", type="primary"):
        if query.strip():
            st.info("Query execution is not implemented in the current API version")
            st.code(query, language="sql")
        else:
            st.warning("Please enter a query to execute.")

def main():
    """Main application entry point"""
    init_page_config()
    
    # Initialize session state
    if 'current_run_id' not in st.session_state:
        st.session_state['current_run_id'] = None
    
    tab1, tab2 = st.tabs(["Data Extraction", "Query Execution"])
    
    with tab1:
        render_data_extraction_tab()
    
    with tab2:
        render_query_execution_tab()
    
    st.markdown(
        """
        <div class='footer'>
            SEC XBRL Data Analysis Tool | Version 2.0
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()