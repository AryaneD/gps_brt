with brt_data as (
    select 
        id,
        position,
        speed
    from {{ ref('brt_gps_data') }}
)
select * from brt_data
