select *
from {{ source('adventure_works_bronze', 'productmodelproductdescription') }}