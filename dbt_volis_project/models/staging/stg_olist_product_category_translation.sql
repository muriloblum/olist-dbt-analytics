with source as (
    select * from {{ source('olist', 'product_category_name_translation') }}
)

select
    product_category_name
    , product_category_name_english
from source