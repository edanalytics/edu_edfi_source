with stg_stu_discipline_incident_non_offenders as (
    select * from {{ ref('stg_ef3__student_discipline_incident_non_offender_associations') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        k_discipline_incident,
        {{ extract_descriptor('value:disciplineIncidentParticipationCodeDescriptor::string') }} as participation_code
    from stg_stu_discipline_incident_non_offenders,
        lateral flatten(input => v_discipline_incident_participation_codes)
)
select * from flattened