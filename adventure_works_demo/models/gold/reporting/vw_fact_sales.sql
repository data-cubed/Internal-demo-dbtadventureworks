

SELECT
    fact_sales.SalesOrderID,
    fact_sales.SalesOrderNumber,
    fact_sales.PurchaseOrderNumber,
    fact_sales.CustomerID,
    fact_sales.ShipToAddressID,
    fact_sales.BillToAddressID,
    fact_sales.ShipMethod,
    fact_sales.SubTotal,
    fact_sales.TaxAmt,
    fact_sales.TotalDue,
    fact_sales.Comment,
    fact_sales.OrderQty,
    fact_sales.ProductID,
    fact_sales.UnitPrice,
    fact_sales.UnitPriceDiscount,
    fact_sales.ModifiedDate
FROM {{ ref('fact_sales') }} fact_sales
