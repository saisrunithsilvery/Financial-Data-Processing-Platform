import streamlit as st
import requests
import pandas as pd
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

# Predefined queries dictionary separated by data format
PREDEFINED_QUERIES = {
    "RAW": {
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
    },
    "JSON": {
        "Top Companies by Market": """
            SELECT 
                m.name,
                m.country,
                COUNT(f.id) as data_points
            FROM financial_metadata m
            JOIN financial_data f ON m.symbol = f.symbol
            GROUP BY m.name, m.country
            ORDER BY data_points DESC
            LIMIT 10
        """,
        "Balance Sheet Analysis": """
            SELECT 
                m.symbol,
                m.name,
                f.concept,
                f.value
            FROM financial_metadata m
            JOIN financial_data f ON m.symbol = f.symbol
            WHERE f.data_type = 'bs'
            AND f.concept IN ('totalAssets', 'totalLiabilities', 'totalEquity')
            ORDER BY m.symbol, f.concept
        """,
        "Cash Flow Metrics": """
            SELECT 
                m.symbol,
                m.name,
                f.concept,
                f.value,
                m.year,
                m.quarter
            FROM financial_metadata m
            JOIN financial_data f ON m.symbol = f.symbol
            WHERE f.data_type = 'cf'
            AND f.concept IN ('operatingCashFlow', 'investingCashFlow', 'financingCashFlow')
            ORDER BY m.year DESC, m.quarter DESC
        """,
        "Company Financial Health": """
            SELECT 
                m.symbol,
                m.name,
                SUM(CASE WHEN f.data_type = 'bs' AND f.concept = 'totalAssets' THEN f.value ELSE 0 END) as total_assets,
                SUM(CASE WHEN f.data_type = 'bs' AND f.concept = 'totalLiabilities' THEN f.value ELSE 0 END) as total_liabilities,
                SUM(CASE WHEN f.data_type = 'cf' AND f.concept = 'operatingCashFlow' THEN f.value ELSE 0 END) as operating_cash_flow
            FROM financial_metadata m
            JOIN financial_data f ON m.symbol = f.symbol
            GROUP BY m.symbol, m.name
            HAVING total_assets > 0
            ORDER BY total_assets DESC
        """,
        "Quarterly Performance Trends": """
            SELECT 
                m.symbol,
                m.name,
                m.year,
                m.quarter,
                MAX(CASE WHEN f.concept = 'totalRevenue' THEN f.value END) as revenue,
                MAX(CASE WHEN f.concept = 'netIncome' THEN f.value END) as net_income
            FROM financial_metadata m
            JOIN financial_data f ON m.symbol = f.symbol
            WHERE f.data_type = 'bs'
            GROUP BY m.symbol, m.name, m.year, m.quarter
            ORDER BY m.year DESC, m.quarter DESC
        """
    }
}

# Initialize session states
if 'processing' not in st.session_state:
    st.session_state.processing = False
    st.session_state.button_text = "Extract"

if 'query_history' not in st.session_state:
    st.session_state.query_history = []

if 'current_format' not in st.session_state:
    st.session_state.current_format = "RAW"

# Title
st.title("SEC Data Explorer")

# Create tabs
tab1, tab2, tab3 = st.tabs(["Data Extraction", "Predefined Queries", "Custom Query"])

def execute_query(query, data_format="RAW"):
    """Execute SQL query through API endpoint based on data format"""
    api_endpoint = "http://backend:8000/queries/execute"
    if data_format == "JSON":
        api_endpoint = "http://backend:8000/queries/execute"
        
    try:
        response = requests.post(
            api_endpoint,
            json={"query": query},
            timeout=30
        )
        if response.status_code == 200:
            response_data = response.json()
            if "data" in response_data:
                df = pd.DataFrame(response_data["data"])
                if response_data.get("execution_time"):
                    st.info(f"Query executed in {response_data['execution_time']:.2f} seconds")
                return df
            else:
                st.error("Invalid response format from server")
                return None
        else:
            error_msg = response.json().get("detail", "Unknown error occurred")
            st.error(f"Query failed: {error_msg}")
            return None
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None

# Data Extraction Tab
with tab1:
    st.header("Data Extraction")
    
    # Year and Quarter Dropdowns
    year = st.selectbox("Select Year:", list(range(2009, 2024)))
    quarter = st.selectbox("Select Quarter:", ["Q1", "Q2", "Q3", "Q4"])
    ways = st.selectbox("Select Data Format:", ["RAW", "JSON", "NORMALIZED"])
    
    # Extract functionality
    if st.button("Extract", key="extract_button", disabled=st.session_state.processing):
        st.session_state.processing = True
        
        try:
            # Show processing message
            with st.spinner('Extracting data...'):
                response = requests.post(
                    "http://backend:8000/extract",
                    json={"year": year, "quarter": quarter, "way": ways},
                    timeout=10
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
            st.session_state.processing = False
            st.rerun()

    # Status indicator
    status = "Processing..." if st.session_state.processing else "Ready"
    st.markdown(f"**Status:** {status}")

# Predefined Queries Tab
with tab2:
    st.header("Predefined Queries")
    
    # Data format selection
    data_format = st.selectbox(
        "Select Data Format:",
        ["RAW", "JSON"],
        key="predefined_format"
    )
    st.session_state.current_format = data_format
    
    # Query selection based on format
    selected_query = st.selectbox(
        "Select a query:",
        list(PREDEFINED_QUERIES[data_format].keys())
    )
    
    # Show the SQL for the selected query
    st.code(PREDEFINED_QUERIES[data_format][selected_query], language="sql")
    
    if st.button("Run Predefined Query"):
        with st.spinner("Executing query..."):
            df = execute_query(PREDEFINED_QUERIES[data_format][selected_query], data_format)
            if df is not None:
                st.success("Query executed successfully!")
                st.dataframe(df)
                
                # Add to query history
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state.query_history.append({
                    "timestamp": timestamp,
                    "query": PREDEFINED_QUERIES[data_format][selected_query],
                    "format": data_format
                })

# Custom Query Tab
with tab3:
    st.header("Custom Query")
    
    # Data format selection for custom query
    data_format = st.selectbox(
        "Select Data Format:",
        ["RAW", "JSON"],
        key="custom_format"
    )
    
    # Schema reference based on format
    if data_format == "RAW":
        schema_ref = """
        ### NUM Table
        - `adsh`: VARCHAR(20) NOT NULL
        - `tag`: VARCHAR(256) NOT NULL
        - `version`: VARCHAR(20) NOT NULL
        - `ddate`: DATE NOT NULL
        - `qtrs`: INT NOT NULL
        - `uom`: VARCHAR(20) NOT NULL
        - `value`: DECIMAL(28,4)

        ### SUB Table
        - Contains company submission details
        - Key fields: adsh, cik, name, form, period, filed

        ### TAG Table
        - `tag`: VARCHAR(256) NOT NULL
        - `version`: VARCHAR(20) NOT NULL
        - `custom`: BOOLEAN NOT NULL
        - `abstract`: BOOLEAN NOT NULL
        """
    else:
        schema_ref = """
        ### financial_metadata Table
        - `id`: INTEGER PRIMARY KEY
        - `startDate`: DATE
        - `endDate`: DATE
        - `year`: INT
        - `quarter`: VARCHAR
        - `symbol`: VARCHAR NOT NULL
        - `name`: VARCHAR
        - `country`: VARCHAR
        - `city`: VARCHAR

        ### financial_data Table
        - `id`: INTEGER PRIMARY KEY
        - `symbol`: VARCHAR
        - `data_type`: VARCHAR
        - `concept`: VARCHAR
        - `info`: VARCHAR
        - `unit`: VARCHAR
        - `value`: FLOAT
        """
    
    with st.expander("Table Schema Reference"):
        st.markdown(schema_ref)
    
    # Query input
    custom_query = st.text_area(
        "Enter your SQL query:",
        height=200,
        help=f"Write your custom SQL query here for {data_format} format."
    )
    
    # Query execution
    if st.button("Run Custom Query"):
        if not custom_query.strip():
            st.warning("Please enter a query first.")
        else:
            with st.spinner("Executing query..."):
                df = execute_query(custom_query, data_format)
                if df is not None:
                    st.success("Query executed successfully!")
                    st.dataframe(df)
                    
                    # Add to query history
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    st.session_state.query_history.append({
                        "timestamp": timestamp,
                        "query": custom_query,
                        "format": data_format
                    })

    # Query History
    with st.expander("Query History"):
        for item in reversed(st.session_state.query_history):
            st.text(f"Time: {item['timestamp']}")
            # Check if format exists in the item (for backward compatibility)
            if 'format' in item:
                st.text(f"Format: {item['format']}")
            st.code(item['query'], language="sql")
            st.markdown("---")