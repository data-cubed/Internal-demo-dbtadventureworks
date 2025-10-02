{{ config(
    materialized='incremental',
    unique_key='SalesOrderID',
    on_schema_change='append_new_columns'
) }}

SELECT
    sales_order_header.SalesOrderID,
    sales_order_header.SalesOrderNumber,
    sales_order_header.PurchaseOrderNumber,
    sales_order_header.CustomerID,
    sales_order_header.ShipToAddressID,
    sales_order_header.BillToAddressID,
    sales_order_header.ShipMethod,
    sales_order_header.SubTotal,
    sales_order_header.TaxAmt,
    sales_order_header.TotalDue,
    sales_order_header.Comment,
    sales_order_detail.OrderQty,
    sales_order_detail.ProductID,
    sales_order_detail.UnitPrice,
    sales_order_detail.UnitPriceDiscount,
    sales_order_header.ModifiedDate
FROM {{ ref('sales_order_header') }} sales_order_header
LEFT JOIN {{ ref('sales_order_detail') }} sales_order_detail
    ON sales_order_header.SalesOrderID = sales_order_detail.SalesOrderID

{% if is_incremental() %}
WHERE sales_order_header.ModifiedDate > (SELECT MAX(ModifiedDate) FROM {{ this }})
{% endif %}