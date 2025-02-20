{{ config(
    materialized='table',
    schema='PUBLIC',
    databse= 'SEC_DATA'
) }}

WITH base_metrics AS (
    SELECT 
        company_symbol,
        fiscal_year,
        fiscal_period,
        filing_date,
        metric_code,
        metric_value
    FROM {{ ref('fct_financial_statements') }}
    WHERE metric_value IS NOT NULL
),

-- Pivot the metrics to get them as columns
pivoted_metrics AS (
    SELECT
        company_symbol,
        fiscal_year,
        fiscal_period,
        filing_date,
        MAX(CASE WHEN metric_code = 'Assets' THEN metric_value END) AS total_assets,
        MAX(CASE WHEN metric_code = 'Liabilities' THEN metric_value END) AS total_liabilities,
        MAX(CASE WHEN metric_code = 'StockholdersEquity' THEN metric_value END) AS stockholders_equity,
        MAX(CASE WHEN metric_code = 'CashAndCashEquivalents' THEN metric_value END) AS cash_and_equivalents,
        MAX(CASE WHEN metric_code = 'Revenues' THEN metric_value END) AS revenue,
        MAX(CASE WHEN metric_code = 'CostOfRevenue' THEN metric_value END) AS cost_of_revenue,
        MAX(CASE WHEN metric_code = 'OperatingIncomeLoss' THEN metric_value END) AS operating_income,
        MAX(CASE WHEN metric_code = 'NetIncomeLoss' THEN metric_value END) AS net_income,
        MAX(CASE WHEN metric_code = 'EarningsPerShareBasic' THEN metric_value END) AS eps_basic,
        MAX(CASE WHEN metric_code = 'NetCashProvidedByUsedInOperatingActivities' THEN metric_value END) AS operating_cash_flow,
        MAX(CASE WHEN metric_code = 'NetCashProvidedByUsedInInvestingActivities' THEN metric_value END) AS investing_cash_flow,
        MAX(CASE WHEN metric_code = 'NetCashProvidedByUsedInFinancingActivities' THEN metric_value END) AS financing_cash_flow
    FROM base_metrics
    GROUP BY company_symbol, fiscal_year, fiscal_period, filing_date
),

-- Calculate financial ratios
calculated_metrics AS (
    SELECT
        company_symbol,
        fiscal_year,
        fiscal_period,
        filing_date,
        total_assets,
        total_liabilities,
        stockholders_equity,
        cash_and_equivalents,
        revenue,
        cost_of_revenue,
        operating_income,
        net_income,
        eps_basic,
        operating_cash_flow,
        investing_cash_flow,
        financing_cash_flow,
        
        -- Liquidity Ratios
        CASE WHEN total_assets > 0 THEN cash_and_equivalents / total_assets ELSE NULL END AS cash_ratio,
        
        -- Profitability Ratios
        CASE WHEN revenue > 0 THEN (revenue - cost_of_revenue) / revenue ELSE NULL END AS gross_margin,
        CASE WHEN revenue > 0 THEN net_income / revenue ELSE NULL END AS net_profit_margin,
        CASE WHEN total_assets > 0 THEN net_income / total_assets ELSE NULL END AS return_on_assets,
        CASE WHEN stockholders_equity > 0 THEN net_income / stockholders_equity ELSE NULL END AS return_on_equity,
        
        -- Leverage Ratios
        CASE WHEN stockholders_equity > 0 THEN total_liabilities / stockholders_equity ELSE NULL END AS debt_to_equity,
        CASE WHEN total_assets > 0 THEN total_liabilities / total_assets ELSE NULL END AS debt_ratio,
        
        -- Cash Flow Ratios
        CASE WHEN net_income > 0 THEN operating_cash_flow / net_income ELSE NULL END AS operating_cash_flow_ratio
    FROM pivoted_metrics
)

SELECT * FROM calculated_metrics