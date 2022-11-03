with stage_student_discipline_incident as (
    select * from {{ ref('stg_ef3__student_discipline_incident_associations') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student,
        k_discipline_incident,
        {{ extract_descriptor('value:behaviorDescriptor::string') }} as behavior_type
    from stage_student_discipline_incident
        , lateral flatten(input=>v_behaviors)
)
select * from flattened