with stage_staffs as (
    select * from {{ ref('stg_ef3__staffs') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_staff,
        {{ extract_descriptor('value:raceDescriptor::string') }} as race
    from stage_staffs
        {{ json_flatten('v_races') }}
)
select * from flattened
