with base_descriptors as (
    select *
    from {{ ref('base_ef3__descriptors') }}
)
select * from base_descriptors