-- bridge differences between parents and contacts
{{ config(materialized='view') }}
with base_parents as (
    select *, parent_unique_id as contact_unique_id
    from {{ ref('base_ef3__parents') }}
)
select * from base_parents
