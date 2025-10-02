
SELECT 
    dim_product.ProductID,
    dim_product.ProductName,
    dim_product.Color,
    dim_product.Size,
    dim_product.ListPrice,
    dim_product.ProductModelName,
    dim_product.ProductDescription,
    dim_product.ProductCategoryName,
    dim_product.ModifiedDate
FROM {{ ref('dim_product') }} dim_product