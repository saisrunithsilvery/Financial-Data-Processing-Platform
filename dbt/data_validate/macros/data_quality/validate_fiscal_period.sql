{% macro validate_fiscal_period(model, column_name='fiscal_period') %}

SELECT 
    {{ column_name }},
    COUNT(*) as record_count
FROM {{ model }}
WHERE {{ column_name }} NOT IN ('Q1', 'Q2', 'Q3', 'Q4', 'FY')
GROUP BY {{ column_name }}
HAVING COUNT(*) > 0

{% endmacro %}

{% macro test_valid_fiscal_period(model, column_name='fiscal_period') %}

WITH validation AS (
    {{ validate_fiscal_period(model, column_name) }}
)

SELECT COUNT(*)
FROM validation

{% endmacro %}




