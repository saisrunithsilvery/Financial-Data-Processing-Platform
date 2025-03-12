import streamlit as st
 
# Must be the first Streamlit command
st.set_page_config(
    page_title="Findata Inc. | Financial Database Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)
 
# Custom CSS for consistent styling
st.markdown("""
    <style>
    /* Global styles */
    .stApp {
        background-color: white;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: white;
    }
    
    /* Navigation items in sidebar */
    .css-17lntkn {
        color: #A9A9A9 !important;
        font-weight: normal;
    }
    
    /* Selected navigation item */
    .css-17lntkn.selected {
        background-color: rgba(240, 242, 246, 0.5) !important;
        border-radius: 4px;
    }
    
    /* Main content text */
    h1, h2, h3, p {
        color: black !important;
    }
    
    /* Feature cards */
    .feature-card {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 20px;
        margin: 10px 0;
        border: 1px solid #eee;
    }
    
    .feature-card h3 {
        color: black !important;
        margin-bottom: 10px;
    }
    
    .feature-card p {
        color: #666 !important;
    }
    
    /* Stats container */
    .stats-container {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 20px;
        margin: 20px 0;
        border: 1px solid #eee;
    }
    
    /* Metric styling */
    [data-testid="stMetricValue"] {
        color: black !important;
    }
    
    /* Button styling */
    .stButton > button {
        background-color: white;
        color: black;
        border: 1px solid #ddd;
        border-radius: 4px;
        padding: 0.5rem 1.5rem;
        width: 100%;
    }
    
    .stButton > button:hover {
        background-color: #f8f9fa;
        border-color: #ddd;
    }
 
    /* Make header white */
    header[data-testid="stHeader"] {
        background-color: white;
    }
    
    /* Remove any dark backgrounds */
    .stApp header {
        background-color: white !important;
    }
    
    /* Style header elements */
    .stApp header button {
        color: black !important;
    }
    
    /* Sidebar navigation styling */
    [data-testid="stSidebar"] {
        background-color: white;
    }
    
    /* Fix for the uploaded file name color in the file uploader */
    [data-testid="stFileUploader"] div {
        color: black !important; 
        font-weight: 500;        
    }
 
    /* Adjust the input and dropdown text color */
    .stTextInput, .stSelectbox {
        color: black !important;
        background-color: white !important;
    }
    
    /* Ensure that all text within the sidebar is visible */
    [data-testid="stSidebar"] * {
        color: black !important;
    }
    
    /* General fix for button and interactive element text */
    .stButton > button, .stRadio > div, .stSelectbox > div {
        color: black !important;
        background-color: white !important;
    }
    
    /* Specific styling for the file uploader */
    .stFileUploader {
        background-color: #f8f9fa;
        border: 1px solid #ddd;
        border-radius: 4px;
    }
    
    .stButton > button:hover {
        background-color: rgba(240, 242, 246, 0.5);
        border-radius: 4px;
    }
    </style>
""", unsafe_allow_html=True)
 
# Initialize session state if needed
if 'welcome_shown' not in st.session_state:
    st.session_state.welcome_shown = False
 
# Main content
st.markdown("""
    <div style='padding: 1rem 0;'>
        <h1 style='color: black; font-size: 2.5rem; font-weight: 800;'>📊 Findata Inc.</h1>
        <p style='color: #666; font-size: 1.2rem;'>Master Financial Statement Database Platform</p>
    </div>
""", unsafe_allow_html=True)
 
# Platform overview section
st.markdown("### Transform Your Financial Analysis")
st.markdown("""
    Findata Inc. provides a powerful platform for analysts conducting fundamental analysis of US public companies.
    Our database integrates SEC Financial Statement Data Sets into a comprehensive system built on Snowflake,
    offering multiple storage approaches to suit your analytical needs.
""")
 
# Feature highlights
col1, col2, col3 = st.columns(3)
 
with col1:
    st.markdown("""
        <div class='feature-card'>
            <h3>📄 SEC Data Processing</h3>
            <p>Extract and process financial statement data from SEC filings</p>
            <ul style='color: #666;'>
                <li>Balance Sheet data</li>
                <li>Income Statement data</li>
                <li>Cash Flow data</li>
            </ul>
        </div>
    """, unsafe_allow_html=True)
 
with col2:
    st.markdown("""
        <div class='feature-card'>
            <h3>🌐 Flexible Storage Options</h3>
            <p>Choose the best storage approach for your analysis</p>
            <ul style='color: #666;'>
                <li>Raw Staging</li>
                <li>JSON Transformation</li>
                <li>Denormalized Fact Tables</li>
            </ul>
        </div>
    """, unsafe_allow_html=True)
 
with col3:
    st.markdown("""
        <div class='feature-card'>
            <h3>📊 Data Transformation</h3>
            <p>Seamlessly transform financial data with DBT</p>
            <ul style='color: #666;'>
                <li>Validated transformations</li>
                <li>Automated testing</li>
                <li>Streamlined pipeline</li>
            </ul>
        </div>
    """, unsafe_allow_html=True)
 
# Platform statistics
st.markdown("### Platform Statistics")
st.markdown("""
    <div class='stats-container'>
        <div style='display: flex; justify-content: space-between;'>
""", unsafe_allow_html=True)
 
col1, col2, col3, col4 = st.columns(4)
 
with col1:
    st.metric("Companies Covered", "10K+")
with col2:
    st.metric("Financial Statements", "500K+")
with col3:
    st.metric("Data Processing Time", "90% faster")
with col4:
    st.metric("Accuracy Rate", "99.9%")
 
# Getting Started section
st.markdown("### System Architecture")
st.markdown("""
    <div class='feature-card'>
        <p>Our comprehensive platform includes:</p>
        <ol style='color: #666;'>
            <li><b>Data Extraction:</b> Scraping data from SEC Markets Data page and processing financial statements</li>
            <li><b>Storage Design:</b> Multiple approaches including Raw Staging, JSON Transformation, and Denormalized Fact Tables</li>
            <li><b>Data Transformation:</b> DBT pipelines for validation and transformation of financial data</li>
            <li><b>Operational Pipeline:</b> Airflow pipelines for data validation and staging with S3 integration</li>
            <li><b>Testing Framework:</b> Comprehensive data integrity verification across all storage options</li>
            <li><b>Analysis Interface:</b> This Streamlit application with FastAPI backend for Snowflake connectivity</li>
        </ol>
    </div>
""", unsafe_allow_html=True)
 
# Sidebar
with st.sidebar:
    
    st.markdown("### Navigation")
 
    st.markdown("""
        <style>
        /* Style the navigation buttons to look like links */
        .nav-button {
            background: none;
            border: none;
            color: #666;
            padding: 8px 0;
            width: 100%;
            text-align: left;
            cursor: pointer;
        }
        .nav-button:hover {
            background-color: rgba(240, 242, 246, 0.5);
            border-radius: 4px;
        }
        </style>
    """, unsafe_allow_html=True)
    
    if st.button("📈 Start Financial Analysis"):
        st.switch_page("app.py")
    
  