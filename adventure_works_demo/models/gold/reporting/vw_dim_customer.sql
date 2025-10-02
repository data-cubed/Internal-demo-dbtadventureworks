
select
    dim_customer.CustomerID,
    dim_customer.NameStyle,
    dim_customer.Title,
    dim_customer.FirstName,
    dim_customer.MiddleName,
    dim_customer.LastName,
    dim_customer.Suffix,
    dim_customer.CompanyName,
    dim_customer.SalesPerson,
    dim_customer.EmailAddress,
    dim_customer.Phone,
    dim_customer.AddressLine1,
    dim_customer.AddressLine2,
    dim_customer.City,
    dim_customer.StateProvince,
    dim_customer.PostalCode,
    dim_customer.ModifiedDate
FROM {{ ref('dim_customer') }} dim_customer
