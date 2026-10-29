{{ config(
    materialized='table'
) }}

WITH silver_customers AS (
    SELECT * FROM {{ source('silver', 'silver_customers') }}
)

SELECT
    customer_sk,
    CustomerKey,
    FullName,
    EmailAddress,
    AnnualIncome,
    TotalChildren,
    Occupation,
    effective_start_date,
    effective_end_date,
    is_current
FROM silver_customers
