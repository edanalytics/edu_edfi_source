with stg_stu_discipline_incident_behaviors as (
    select * from {{ ref('stg_ef3__student_discipline_incident_behavior_associations') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        k_discipline_incident,
        {{ extract_descriptor('value:disciplineIncidentParticipationCodeDescriptor::string') }} as participation_code
    from stg_stu_discipline_incident_behaviors
        {{ json_flatten('v_discipline_incident_participation_codes') }}
)
select * from flattened
