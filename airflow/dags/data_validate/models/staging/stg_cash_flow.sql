{{ config(materialized='view') }}

WITH source_data AS (
    SELECT 
        n.adsh,
        n.tag,
        n.value,
        n.uom,
        s.cik,
        s.period,
        s.fy,
        s.fp,
        t.tlabel,
        t.datatype
    FROM {{ source('sec_data', 'num') }} n
    INNER JOIN {{ source('sec_data', 'sub') }} s 
        ON n.adsh = s.adsh
    INNER JOIN {{ source('sec_data', 'tag') }} t 
        ON n.tag = t.tag
    WHERE n.tag IN (
        'NetCashProvidedByUsedInFinancingActivities',
        'NetCashProvidedByUsedInOperatingActivities',
        'NetCashProvidedByUsedInInvestingActivities'
    )
    AND n.value IS NOT NULL  -- Only include rows with non-null values
),

cleaned_data AS (
    SELECT DISTINCT
        cik as company_symbol,
        period as filing_date,
        fy as fiscal_year,
        CASE 
            WHEN UPPER(TRIM(fp)) IN ('Q1', 'QTR1', 'QUARTER1', '1', '1ST', 'FIRST') THEN 'Q1'
            WHEN UPPER(TRIM(fp)) IN ('Q2', 'QTR2', 'QUARTER2', '2', '2ND', 'SECOND') THEN 'Q2'
            WHEN UPPER(TRIM(fp)) IN ('Q3', 'QTR3', 'QUARTER3', '3', '3RD', 'THIRD') THEN 'Q3'
            WHEN UPPER(TRIM(fp)) IN ('Q4', 'QTR4', 'QUARTER4', '4', '4TH', 'FOURTH') THEN 'Q4'
            WHEN UPPER(TRIM(fp)) IN ('FY', 'YEAR', 'ANNUAL', 'Y', 'YEARLY') THEN 'FY'
            ELSE NULL
        END as fiscal_period,
        tlabel as metric_name,
        tag as metric_code,
        datatype as metric_info,
        CASE 
            WHEN UPPER(TRIM(uom)) IN ('USD', 'DOLLARS', '$') THEN 'USD'
            WHEN UPPER(TRIM(uom)) IN ('SHARES', 'SHS', 'SHARE') THEN 'shares'
            WHEN UPPER(TRIM(uom)) IN ('RATIO', 'R') THEN 'ratio'
            WHEN UPPER(TRIM(uom)) IN ('PERCENT', '%', 'PCT') THEN 'percent'
            ELSE 'USD'
        END as measurement_unit,
        TRY_TO_DECIMAL(value, 38, 4) as metric_value,
        'SEC_CASH_FLOW' as source_file
    FROM source_data
)

SELECT * FROM cleaned_data
WHERE company_symbol IS NOT NULL
  AND metric_value IS NOT NULL  -- Additional check for nulls after conversion




-- -- models/staging/stg_cash_flow.sql
-- {{ config(materialized='view') }}

-- WITH source_data AS (
--     SELECT 
--         n.adsh,
--         n.tag,
--         n.value,
--         n.uom,
--         s.cik,
--         s.period,
--         s.fy,
--         s.fp,
--         t.tlabel,
--         t.datatype
--     FROM {{ source('sec_data', 'num') }} n
--     INNER JOIN {{ source('sec_data', 'sub') }} s 
--         ON n.adsh = s.adsh
--     INNER JOIN {{ source('sec_data', 'tag') }} t 
--         ON n.tag = t.tag
--     WHERE n.tag IN (
--         'NetCashProvidedByUsedInFinancingActivities',
--         'NetCashProvidedByUsedInOperatingActivities',
--         'NetCashProvidedByUsedInInvestingActivities'
--     )
-- ),

-- cleaned_data AS (
--     SELECT DISTINCT
--         cik as company_symbol,
--         period as filing_date,
--         fy as fiscal_year,
--         CASE 
--             WHEN UPPER(TRIM(fp)) IN ('Q1', 'QTR1', 'QUARTER1') THEN 'Q1'
--             WHEN UPPER(TRIM(fp)) IN ('Q2', 'QTR2', 'QUARTER2') THEN 'Q2'
--             WHEN UPPER(TRIM(fp)) IN ('Q3', 'QTR3', 'QUARTER3') THEN 'Q3'
--             WHEN UPPER(TRIM(fp)) IN ('Q4', 'QTR4', 'QUARTER4') THEN 'Q4'
--             WHEN UPPER(TRIM(fp)) IN ('FY', 'YEAR', 'ANNUAL') THEN 'FY'
--             ELSE fp
--         END as fiscal_period,
--         tlabel as metric_name,
--         tag as metric_code,
--         datatype as metric_info,
--         CASE 
--             WHEN UPPER(TRIM(uom)) IN ('USD', 'DOLLARS', '$') THEN 'USD'
--             WHEN UPPER(TRIM(uom)) IN ('SHARES', 'SHS', 'SHARE') THEN 'shares'
--             WHEN UPPER(TRIM(uom)) IN ('RATIO', 'R') THEN 'ratio'
--             WHEN UPPER(TRIM(uom)) IN ('PERCENT', '%', 'PCT') THEN 'percent'
--             ELSE 'USD'
--         END as measurement_unit,
--         TRY_TO_DECIMAL(value, 38, 4) as metric_value,
--         'SEC_CASH_FLOW' as source_file
--     FROM source_data
-- )

-- SELECT * FROM cleaned_data
-- WHERE company_symbol IS NOT NULL





-- -- models/staging/stg_cash_flow.sql
-- {{ config(materialized='view') }}

-- WITH cash_flow_data AS (
--     SELECT DISTINCT
--         TRIM(s.cik)::STRING as company_symbol,
--         TRY_TO_DATE(s.period) as filing_date,
--         CASE 
--             WHEN TRIM(s.fy) RLIKE '^[0-9]+$' THEN TRIM(s.fy)::NUMBER 
--             ELSE NULL 
--         END as fiscal_year,
--         TRIM(s.fp)::STRING as fiscal_period,
--         TRIM(t.tlabel) as metric_name,
--         TRIM(n.tag) as metric_code,
--         TRIM(t.datatype) as metric_info,
--         COALESCE(TRIM(n.uom), 'USD') as measurement_unit,
--         CASE 
--             WHEN n.value IS NOT NULL AND TRY_TO_DECIMAL(TRIM(n.value), 38, 4) IS NOT NULL 
--             THEN TRY_TO_DECIMAL(TRIM(n.value), 38, 4)
--             ELSE NULL 
--         END as metric_value,
--         'SEC_CASH_FLOW' as source_file,
--         CURRENT_TIMESTAMP() as loaded_at
--     FROM {{ source('sec_data', 'num') }} n
--     INNER JOIN {{ source('sec_data', 'sub') }} s 
--         ON n.adsh = s.adsh
--     INNER JOIN {{ source('sec_data', 'tag') }} t 
--         ON n.tag = t.tag
--     WHERE n.tag IN (
--         'NetCashProvidedByUsedInFinancingActivities',
--         'NetCashProvidedByUsedInOperatingActivities',
--         'NetCashProvidedByUsedInInvestingActivities'
--     )
-- )

-- SELECT * FROM cash_flow_data
-- WHERE company_symbol IS NOT NULL





-- {{ config(materialized='view') }}

-- WITH cash_flow_data AS (
--     SELECT DISTINCT
--         s.cik::STRING as company_symbol,
--         TO_DATE(s.period) as filing_date,
--         s.fy::NUMBER as fiscal_year,
--         s.fp as fiscal_period,
--         t.tlabel as metric_name,
--         n.tag as metric_code,
--         t.datatype as metric_info,
--         COALESCE(n.uom, 'USD') as measurement_unit,
--         TO_NUMBER(n.value, 38, 4) as metric_value,
--         'SEC_CASH_FLOW' as source_file,
--         CURRENT_TIMESTAMP() as loaded_at
--     FROM {{ source('sec_data', 'num') }} n
--     INNER JOIN {{ source('sec_data', 'sub') }} s 
--         ON n.adsh = s.adsh
--     INNER JOIN {{ source('sec_data', 'tag') }} t 
--         ON n.tag = t.tag
--     WHERE n.tag IN (
--         'NetCashProvidedByUsedInFinancingActivities',
--         'NetCashProvidedByUsedInOperatingActivities',
--         'NetCashProvidedByUsedInInvestingActivities'
--     )
-- )

-- SELECT * FROM cash_flow_data
-- WHERE company_symbol IS NOT NULL



