with stg_discipline_actions as (
    select * from {{ ref('stg_ef3__discipline_actions') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        discipline_action_id,
        discipline_date,
        k_student,
        {{ extract_descriptor('value:studentDisciplineIncidentBehaviorAssociationReference:behaviorDescriptor::string') }} as behavior_type,
        value:studentDisciplineIncidentBehaviorAssociationReference:incidentIdentifier::string as incident_id,
        value:studentDisciplineIncidentBehaviorAssociationReference:schoolId::string as school_id,
        value:studentDisciplineIncidentBehaviorAssociationReference:studentUniqueId::string as student_unique_id
    from stg_discipline_actions,
        lateral flatten(input => v_student_discipline_incident_behavior_associations)

    union all

    select 
        tenant_code,
        api_year,
        discipline_action_id,
        discipline_date,
        k_student,
        null as behavior_type,
        value:studentDisciplineIncidentAssociationReference:incidentIdentifier::string as incident_id,
        value:studentDisciplineIncidentAssociationReference:schoolId::string as school_id,
        value:studentDisciplineIncidentAssociationReference:studentUniqueId::string as student_unique_id
    from stg_discipline_actions,
        lateral flatten(input => v_student_discipline_incident_associations)
)
select * from flattened