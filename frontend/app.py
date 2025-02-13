import streamlit as st
import requests

# Set page configuration
st.set_page_config(page_title="Year and Quarter Selector", layout="centered", initial_sidebar_state="auto")

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
        }
        .stSelectbox>div {
            background-color: #f0f0f0 !important;
            color: black !important;
        }
        h1 {
            color: black;
        }
        .stAlert {
            background-color: #dff0d8 !important;
            color: black !important;
        }
    </style>
""", unsafe_allow_html=True)

# Title
st.title("Data Extractor")

# Year and Quarter Dropdowns
year = st.selectbox("Select Year:", list(range(2006, 2026)))
quarter = st.selectbox("Select Quarter:", ["Q1", "Q2", "Q3", "Q4"])

# Extract Button
if st.button("Extract"):
    response = requests.post("http://127.0.0.1:8000/extract", json={"year": year, "quarter": quarter})
    if response.status_code == 200:
        data = response.json()
        st.success(data["message"])
    else:
        st.error("Failed to extract data!")
