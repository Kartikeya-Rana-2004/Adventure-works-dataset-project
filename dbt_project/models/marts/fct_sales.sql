{{ config(
    materialized='table',
    partition_by=['OrderYear', 'OrderMonth']
) }}

WITH source_sales AS (
    SELECT * FROM {{ source('silver', 'silver_sales') }}
),
source_products AS (
    SELECT ProductKey, ProductCost, ProductPrice FROM {{ ref('dim_products') }}
)

SELECT
    s.OrderNumber,
    s.OrderLineItem,
    s.OrderDate,
    YEAR(s.OrderDate) AS OrderYear,
    MONTH(s.OrderDate) AS OrderMonth,
    s.CustomerKey,
    s.ProductKey,
    s.TerritoryKey,
    s.OrderQuantity,
    p.ProductPrice,
    p.ProductCost,
    (s.OrderQuantity * p.ProductPrice) AS GrossRevenue,
    (s.OrderQuantity * p.ProductCost) AS TotalCost,
    ((s.OrderQuantity * p.ProductPrice) - (s.OrderQuantity * p.ProductCost)) AS NetMargin,
    CURRENT_TIMESTAMP() AS _dbt_updated_at
FROM source_sales s
INNER JOIN source_products p ON s.ProductKey = p.ProductKey
