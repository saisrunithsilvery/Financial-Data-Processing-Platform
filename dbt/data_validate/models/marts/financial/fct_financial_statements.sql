{{ config(
    materialized='table',
    schema='PUBLIC',
    databse= 'SEC_DATA'
) }}

WITH valid_companies AS (
    SELECT DISTINCT cik as company_symbol
    FROM {{ source('sec_data', 'sub') }}
    WHERE cik IS NOT NULL
),

-- Safe date parsing function to handle various formats
combined_metrics AS (
    SELECT 
        company_symbol,
        fiscal_year,
        fiscal_period,
        CASE 
            WHEN TRY_CAST(filing_date AS DATE) IS NULL THEN NULL
            WHEN TRY_CAST(filing_date AS DATE) < '1900-01-01' THEN NULL
            WHEN TRY_CAST(filing_date AS DATE) > '2100-01-01' THEN NULL
            ELSE TRY_CAST(filing_date AS DATE)
        END AS filing_date,
        metric_name,
        metric_value,
        metric_code,
        measurement_unit,
        source_file
    FROM {{ ref('stg_balance_sheet') }}
    WHERE company_symbol IS NOT NULL
      AND metric_value IS NOT NULL
    
    UNION ALL
    
    SELECT 
        company_symbol,
        fiscal_year,
        fiscal_period,
        CASE 
            WHEN TRY_CAST(filing_date AS DATE) IS NULL THEN NULL
            WHEN TRY_CAST(filing_date AS DATE) < '1900-01-01' THEN NULL
            WHEN TRY_CAST(filing_date AS DATE) > '2100-01-01' THEN NULL
            ELSE TRY_CAST(filing_date AS DATE)
        END AS filing_date,
        metric_name,
        metric_value,
        metric_code,
        measurement_unit,
        source_file
    FROM {{ ref('stg_income_statement') }}
    WHERE company_symbol IS NOT NULL
      AND metric_value IS NOT NULL
    
    UNION ALL
    
    SELECT 
        company_symbol,
        fiscal_year,
        fiscal_period,
        CASE 
            WHEN TRY_CAST(filing_date AS DATE) IS NULL THEN NULL
            WHEN TRY_CAST(filing_date AS DATE) < '1900-01-01' THEN NULL
            WHEN TRY_CAST(filing_date AS DATE) > '2100-01-01' THEN NULL
            ELSE TRY_CAST(filing_date AS DATE)
        END AS filing_date,
        metric_name,
        metric_value,
        metric_code,
        measurement_unit,
        source_file
    FROM {{ ref('stg_cash_flow') }}
    WHERE company_symbol IS NOT NULL
      AND metric_value IS NOT NULL
)

SELECT 
    cm.company_symbol,
    cm.fiscal_year,
    cm.fiscal_period,
    cm.filing_date,
    cm.metric_name,
    cm.metric_value,
    cm.metric_code,
    cm.measurement_unit,
    cm.source_file,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM combined_metrics cm
INNER JOIN valid_companies vc
    ON cm.company_symbol = vc.company_symbol
WHERE cm.metric_value IS NOT NULL
  AND cm.filing_date IS NOT NULL
  AND cm.fiscal_year IS NOT NULL
  AND cm.fiscal_period IN ('Q1', 'Q2', 'Q3', 'Q4', 'FY')


