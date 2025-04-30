

 Financial Data Processing Platform  

## Project Links and Resources  

- **Codelabs Documentation**: [https://codelabs-preview.appspot.com/?file_id=1TyTEfFZ0RGRb6SoKJ_QyV0ByJaUFGLQ3JKNmdKlni_g/edit?tab=t.5cpih9qtxm58#6]  
- **Project Submission Video (5 Minutes)**: [https://drive.google.com/drive/u/1/folders/1898HGutXjQIxwx3OVnr_Yvx9Uq_SKAE1]  
- **Hosted Application Links**:  
  - **Frontend (Streamlit)**: [Insert Frontend URL]  
  - **Backend (FastAPI)**: [Insert Backend URL]  

---

## Introduction  

This project focuses on **building a master financial statement database** to support analysts conducting **fundamental analysis** of **US public companies**. The database is built using **Snowflake** and sourced from **SEC Financial Statement Data Sets**.  

The project includes:  
- **Data Scraping** from the **SEC Markets Data page** to retrieve dataset links.  
- **Data Storage Design** exploring **three approaches** (Raw Staging, JSON Transformation, Denormalized Fact Tables).  
- **Data Validation** using **Data Validation Tool (DVT)** for schema integrity.  
- **Operational Data Pipelines** with **Apache Airflow** and **S3** for staging.  
- **Post-Upload Testing** ensuring correctness across **all three storage solutions**.  
- **A client-facing Streamlit application** with a **FastAPI backend** to facilitate access and interaction with stored data.  

---

## Problem Statement  

The challenge is to design a **scalable financial data storage solution** by:  
1. **Scraping and extracting** SEC Financial Statement Data.  
2. **Comparing multiple data storage strategies** (Raw, JSON, and RDBMS).  
3. **Ensuring data integrity** with **validation techniques** before ingestion.  
4. **Developing Airflow pipelines** to **automate the ETL process**.  
5. **Building a front-end interface** for analysts to **query and visualize** data.  

### **Desired Outcome:**  
- Efficient storage and retrieval of **financial statement data**.  
- A robust **Snowflake-based infrastructure** supporting **real-time queries**.  
- A **Streamlit and FastAPI-powered interface** enabling intuitive access.  
- A pipeline architecture that ensures **data consistency and reliability**.  

### **Constraints:**  
- Handling **large SEC datasets efficiently** in **Snowflake**.  
- Managing **data schema transformation** (from raw SEC format to JSON and denormalized tables).  
- Ensuring **data validation** before ingestion and transformation.  

---

## **Proof of Concept**  

The project implements three **data storage strategies**:  
1. **Raw Staging**: Storing data **as-is** from SEC Financial Statement Datasets.  
2. **JSON Transformation**: Converting data into JSON format for **faster access**.  
3. **Denormalized Fact Tables**: Structuring data into **Balance Sheet, Income Statement, and Cash Flow tables**.  

### **Implementation Plan:**  
- **Data Extraction**: Scraping SEC links and **retrieving datasets**.  
- **Schema Design**: Creating table structures for **all three storage solutions**.  
- **ETL Pipelines**: Using **Airflow** and **S3 staging** for efficient data handling.  
- **Data Validation**: Implementing **DVT** checks for schema and data integrity.  
- **Frontend & Backend**: Developing **Streamlit (UI) + FastAPI (API) + Snowflake (DB)**.  

---

## Prerequisites  

- **Python 3.1 or higher**  
- **Snowflake Account** (for database operations)  
- **Airflow** (for automated ETL workflows)  
- **Streamlit & FastAPI** (for client-server communication)  
- **AWS S3** (for intermediate data storage)  
- **DVT (Data Validation Tool)** for **schema checks**  

---

## Setup Instructions  

### **1. Clone the Repository**  
```bash  
git clone <repository-url>  
cd <repository-name>  
```

### **2. Set Up Python Virtual Environment**  

#### On Windows:  
```bash  
python -m venv venv  
venv\Scripts\activate  
```

#### On macOS/Linux:  
```bash  
python3 -m venv venv  
source venv/bin/activate  
```

### **3. Install Dependencies**  
```bash  
pip install -r requirements.txt  
```

### **4. Configure Credentials in `.env`**  
```plaintext  
SNOWFLAKE_ACCOUNT=your_snowflake_account  
SNOWFLAKE_USER=your_username  
SNOWFLAKE_PASSWORD=your_password  
AWS_ACCESS_KEY=your_aws_key  
AWS_SECRET_KEY=your_aws_secret  
```

---

## Usage  

### **1. Activate the Virtual Environment**  
```bash  
# Windows  
venv\Scripts\activate  

# macOS/Linux  
source venv/bin/activate  
```

### **2. Run the Airflow Pipeline**  
```bash  
airflow dags trigger financial_data_pipeline  
```

### **3. Run the Backend**  
```bash  
python backend/main.py  
```

### **4. Run the Frontend**  
```bash  
streamlit run frontend/streamlit_app.py  
```

---

## Project Directory Structure  
```
Assignment02/
│── airflow/
│   ├── dags/
│   ├── airflow.sh
│   ├── cleanup.sh
│   ├── requirements.txt
│
│── backend/
│   ├── controllers/
│   ├── models/
│   ├── routes/
│   ├── utils/
│   ├── config.py
│   ├── main.py
│   ├── requirements.txt
│
│── dbt/
│   ├── txt.txt
│
│── docs/
│   ├── txt.txt
│
│── frontend/
│   ├── app.py
│   ├── requirements.txt
│
│── json_converter/
│   ├── airflow_forconverter/
│   ├── dags/
│   ├── logs/scheduler/
│
│── snowflake/
│   ├── sql/
│   ├── raw/
│   ├── setup/
│   ├── __init__.py
│   ├── config.yml.template
│   ├── requirements.txt
│   ├── run_pipeline.py
│
│── src/
│   ├── scrape_to_json.py
│
│── tests/
│   ├── txt.txt
│
│── .gitignore
│── LICENSE
│── README.md
```

---


### **Description of Project Directory**

- **airflow/**: Contains DAGs and scripts for automating ETL workflows.
- **backend/**: FastAPI-based backend for API endpoints, data processing, and interactions.
- **dbt/**: Configuration files for Data Build Tool (DBT) transformations.
- **docs/**: Documentation, reports, and system diagrams.
- **frontend/**: Streamlit-based web application for user interaction.
- **json_converter/**: Utilities for extracting and converting JSON data.
- **snowflake/**: SQL scripts, raw extracted data, and setup configurations for Snowflake.
- **src/**: Scripts for web scraping and JSON conversion.
- **tests/**: Unit and integration tests to validate system functionality.
- **.gitignore**: Specifies files and directories to ignore in version control.
- **LICENSE**: Licensing details for the project.
- **README.md**: Main project documentation and setup guide.


## Team Members  

- **Sai Priya Veerabomma** - 33.3% (Backend, Airflow)  
- **Sai Srunith Silvery** - 33.3% (Frontend, API Development)  
- **Vishal Prasanna** - 33.3% (Deployment, Snowflake Configuration)  

---

## References  

- [SEC Financial Statement Data Sets](https://www.sec.gov/files/financial-statement-data-sets.pdf)  
- [Snowflake Documentation](https://docs.snowflake.com/)  
- [Apache Airflow](https://airflow.apache.org/)  
- [Streamlit](https://docs.streamlit.io/)  
- [FastAPI](https://fastapi.tiangolo.com/)  

---

## License  
This project is **open-source** under the **MIT License**. See `LICENSE` for details.  



