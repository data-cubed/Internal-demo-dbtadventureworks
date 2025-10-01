{{config(
    post_hook=[
        "{{ apply_constraints() }}"
    ]
    )
}}

select *
from {{ source('adventure_works_bronze', 'salesorderheader') }}