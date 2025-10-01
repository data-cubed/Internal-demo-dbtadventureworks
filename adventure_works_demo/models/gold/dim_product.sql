{{ config(
    materialized='incremental',
    unique_key='ProductId',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{{ apply_constraints() }}"
    ]
) }}

SELECT 
    product.ProductID,
    product.Name as ProductName,
    product.Color,
    product.Size,
    product.ListPrice,
    product_model.Name as ProductModelName,
    product_description.Description as ProductDescription,
    product_category.Name as ProductCategoryName,
    product.ModifiedDate
FROM {{ ref('product') }} product
LEFT JOIN {{ ref('product_model') }} product_model
    ON product_model.ProductModelID = product.ProductModelID
LEFT JOIN {{ ref('product_model_product_description') }} product_model_product_description
    ON product.ProductModelID = product_model_product_description.ProductModelID 
LEFT JOIN {{ ref('product_description') }} product_description
    ON product_model_product_description.ProductDescriptionID = product_description.ProductDescriptionID
LEFT JOIN {{ ref('product_category') }} product_category
    ON product.ProductCategoryID = product_category.ProductCategoryID

{% if is_incremental() %}
WHERE product.ModifiedDate > (SELECT MAX(ModifiedDate) FROM {{ this }})
{% endif %}