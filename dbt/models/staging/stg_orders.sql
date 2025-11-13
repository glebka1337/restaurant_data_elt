-- This model reads the raw JSONB data and flattens it into columns.
-- It will be materialized as a VIEW in the 'staging' schema.

with source as (

    select
        data,
        run_id,
        loaded_at
    from {{ source('raw_data', 'staging_raw_orders') }}

)

select
    -- ->> 'key' extracts a JSON key as TEXT
    -- (data ->> 'order_id')::uuid as order_id,
    (data ->> 'user_id')::int as user_id,
    (data ->> 'restaurant_id')::int as restaurant_id,
    data ->> 'status' as status,
    (data ->> 'created_at')::timestamp as created_at,
    (data ->> 'total_price')::float as total_price,
    
    -- -> 'key' extracts a JSON key as a JSONB object
    data -> 'items' as items,
    
    run_id,
    loaded_at

from source