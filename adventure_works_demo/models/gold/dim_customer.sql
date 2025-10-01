{{ config(
    materialized='incremental',
    unique_key='CustomerId',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{{ apply_constraints() }}"
    ]
) }}

select
    customer.CustomerID,
    customer.NameStyle,
    customer.Title,
    customer.FirstName,
    customer.MiddleName,
    customer.LastName,
    customer.Suffix,
    customer.CompanyName,
    customer.SalesPerson,
    customer.EmailAddress,
    customer.Phone,
    address.AddressLine1,
    address.AddressLine2,
    address.City,
    address.StateProvince,
    address.PostalCode,
    customer.ModifiedDate
FROM {{ ref('customer') }} customer
LEFT JOIN {{ ref('customer_address') }} customer_address
    ON customer.CustomerID = customer_address.CustomerID
LEFT JOIN {{ ref('address') }} address
    ON customer_address.AddressID = address.AddressID

{% if is_incremental() %}
WHERE customer.ModifiedDate > (SELECT MAX(ModifiedDate) FROM {{ this }})
{% endif %}