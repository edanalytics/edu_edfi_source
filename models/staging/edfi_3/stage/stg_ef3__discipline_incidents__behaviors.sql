with stg_discipline_incidents as (
    select * from {{ ref('stg_ef3__discipline_incidents') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_discipline_incident,
        k_school,
        {{ extract_descriptor('value:behaviorDescriptor::string') }} as behavior_type
    from stg_discipline_incidents,
        lateral flatten(input => v_behaviors)
)
select * from flattened