import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime

# Set page configuration
st.set_page_config(page_title="SEC Data Explorer", layout="wide", initial_sidebar_state="expanded")

# Apply custom CSS for styling
st.markdown("""
    <style>
        body, .stApp {
            background-color: white !important;
            color: black !important;
        }
        .stButton>button {
            background-color: #4CAF50;
            color: white;
            font-weight: bold;
            min-width: 150px;
        }
        .stButton>button:disabled {
            background-color: #cccccc !important;
            cursor: not-allowed;
        }
        .loading {
            display: inline-block;
            width: 20px;
            height: 20px;
            margin-left: 10px;
            border: 3px solid rgba(255,255,255,.3);
            border-radius: 50%;
            border-top-color: white;
            animation: spin 1s ease-in-out infinite;
        }
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        .stSelectbox>div {
            background-color: #f0f0f0 !important;
            color: black !important;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 24px;
        }
        .stTabs [data-baseweb="tab"] {
            height: 50px;
            padding: 0px 16px;
            background-color: #FFFFFF;
            border-radius: 4px;
        }
        .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] {
            background-color: #4CAF50;
            color: white;
        }
        .sql-editor {
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 10px;
            font-family: monospace;
        }
        h1 { color: black; }
    </style>
""", unsafe_allow_html=True)

# Initialize session states
if 'processing' not in st.session_state:
    st.session_state.processing = False
    st.session_state.button_text = "Extract"

if 'query_history' not in st.session_state:
    st.session_state.query_history = []

# Title
st.title("SEC Data Explorer")

# Create tabs
tab1, tab2, tab3 = st.tabs(["Data Extraction", "Predefined Queries", "Custom Query"])

# Predefined queries
PREDEFINED_QUERIES = {
    "Recent Filings": """
        SELECT s.name, s.form, s.filed, s.period
        FROM SUB s
        ORDER BY s.filed DESC
        LIMIT 10
    """,
    "Top Companies by Filing Count": """
        SELECT s.name, COUNT(*) as filing_count
        FROM SUB s
        GROUP BY s.name
        ORDER BY filing_count DESC
        LIMIT 10
    """,
    "Financial Values by Tag": """
        SELECT n.tag, n.value, s.name, n.ddate
        FROM NUM n
        JOIN SUB s ON n.adsh = s.adsh
        WHERE n.tag IN ('Assets', 'Liabilities', 'StockholdersEquity')
        ORDER BY n.ddate DESC
        LIMIT 10
    """,
    "Company Tags Analysis": """
        SELECT t.tag, t.tlabel, COUNT(*) as usage_count
        FROM TAG t
        JOIN NUM n ON t.tag = n.tag
        GROUP BY t.tag, t.tlabel
        ORDER BY usage_count DESC
        LIMIT 10
    """
}

def execute_query(query, api_endpoint="http://127.0.0.1:8000/query"):
    """Execute SQL query through API endpoint"""
    try:
        response = requests.post(
            api_endpoint,
            json={"query": query},
            timeout=30
        )
        if response.status_code == 200:
            return pd.DataFrame(response.json()["data"])
        else:
            st.error(f"Query failed: {response.text}")
            return None
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None

# Data Extraction Tab
with tab1:
    st.header("Data Extraction")
    
    # Year and Quarter Dropdowns
    year = st.selectbox("Select Year:", list(range(2006, 2026)))
    quarter = st.selectbox("Select Quarter:", ["Q1", "Q2", "Q3", "Q4"])
    
    # Extract functionality
    if st.button("Extract", key="extract_button", disabled=st.session_state.processing):
        st.session_state.processing = True
        
        try:
            # Show processing message
            with st.spinner('Extracting data...'):
                # Set a timeout for the request
                response = requests.post(
                    "http://127.0.0.1:8000/extract",
                    json={"year": year, "quarter": quarter},
                    timeout=10  # 10 second timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    st.success(data["message"])
                else:
                    st.error(f"Failed to extract data. Status code: {response.status_code}")
                    
        except requests.exceptions.Timeout:
            st.error("Request timed out. The server is taking too long to respond.")
        except requests.exceptions.ConnectionError:
            st.error("Connection failed. Please check if the server is running.")
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
        finally:
            # Reset processing state
            st.session_state.processing = False
            st.rerun()  # Refresh the page to update button state

    # Status indicator
    status = "Processing..." if st.session_state.processing else "Ready"
    st.markdown(f"**Status:** {status}")

# Predefined Queries Tab
with tab2:
    st.header("Predefined Queries")
    selected_query = st.selectbox(
        "Select a query:",
        list(PREDEFINED_QUERIES.keys())
    )
    
    # Show the SQL for the selected query
    st.code(PREDEFINED_QUERIES[selected_query], language="sql")
    
    if st.button("Run Predefined Query"):
        with st.spinner("Executing query..."):
            df = execute_query(PREDEFINED_QUERIES[selected_query])
            if df is not None:
                st.success("Query executed successfully!")
                st.dataframe(df)

# Custom Query Tab
with tab3:
    st.header("Custom Query")
    
    # Query input
    custom_query = st.text_area(
        "Enter your SQL query:",
        height=200,
        help="Write your custom SQL query here. Use the table names: NUM, PRE, SUB, and TAG"
    )
    
    # Query execution
    if st.button("Run Custom Query"):
        if not custom_query.strip():
            st.warning("Please enter a query first.")
        else:
            with st.spinner("Executing query..."):
                df = execute_query(custom_query)
                if df is not None:
                    st.success("Query executed successfully!")
                    st.dataframe(df)
                    
                    # Add to query history
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    st.session_state.query_history.append({
                        "timestamp": timestamp,
                        "query": custom_query
                    })

    # Query History
    with st.expander("Query History"):
        for item in reversed(st.session_state.query_history):
            st.text(f"Time: {item['timestamp']}")
            st.code(item['query'], language="sql")
            st.markdown("---")

# Schema Reference
with st.expander("Table Schema Reference"):
    st.markdown("""
    ### NUM Table
    - `adsh`: VARCHAR(20) NOT NULL
    - `tag`: VARCHAR(256) NOT NULL
    - `version`: VARCHAR(20) NOT NULL
    - `ddate`: DATE NOT NULL
    - `qtrs`: INT NOT NULL
    - `uom`: VARCHAR(20) NOT NULL
    - `segments`: TEXT
    - `coreg`: VARCHAR(256)
    - `value`: DECIMAL(28,4)
    - `footnote`: TEXT

    ### PRE Table
    - `adsh`: VARCHAR(50)
    - `report`: INT
    - `line`: INT
    - `stmt`: VARCHAR(5)
    - `inpth`: INT
    - `rfile`: VARCHAR(5)
    - `tag`: VARCHAR(255)
    - `version`: VARCHAR(50)
    - `plabel`: TEXT
    - `negating`: INT

    ### SUB Table
    - Contains company submission details
    - Key fields: adsh, cik, name, form, period, filed

    ### TAG Table
    - `tag`: VARCHAR(256) NOT NULL
    - `version`: VARCHAR(20) NOT NULL
    - `custom`: BOOLEAN NOT NULL
    - `abstract`: BOOLEAN NOT NULL
    - `datatype`: VARCHAR(20)
    - `tlabel`: VARCHAR(512)
    - `doc`: TEXT
    """)