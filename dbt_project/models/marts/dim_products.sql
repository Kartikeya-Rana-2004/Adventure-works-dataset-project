{{ config(
    materialized='table'
) }}

WITH silver_products AS (
    SELECT * FROM {{ source('silver', 'silver_products') }}
)

SELECT
    ProductKey,
    ProductSKU,
    ProductName,
    ModelName,
    ProductColor,
    ProductCost,
    ProductPrice,
    UnitMargin,
    SubcategoryName,
    CategoryName
FROM silver_products
