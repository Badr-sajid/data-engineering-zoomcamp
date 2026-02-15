with taxi_zome_lookup as (
    select * from {{ ref('taxi_zone_lookup') }}
),

renamed as (
    select 
        locationid as location_id,
        borough,
        zone,
        service_zone
    from taxi_zome_lookup
)
select * from renamed