with stg_discipline_actions as (
    select * from {{ ref('stg_ef3__discipline_actions') }}
),

-- Use the current Ed-Fi reference when it is available. 
-- Behavior is part of the key, so the same student and incident can have multiple valid rows when more than one behavior was reported.
incident_behavior_associations as (
    select
        tenant_code,
        api_year,
        discipline_action_id,
        discipline_date,
        k_student,
        k_student_xyear,
        {{ gen_skey('k_discipline_incident', alt_ref='value:studentDisciplineIncidentBehaviorAssociationReference') }},
        {{ extract_descriptor('value:studentDisciplineIncidentBehaviorAssociationReference:behaviorDescriptor::string') }} as behavior_type,
        value:studentDisciplineIncidentBehaviorAssociationReference:incidentIdentifier::string as incident_id,
        value:studentDisciplineIncidentBehaviorAssociationReference:schoolId::string as school_id,
        value:studentDisciplineIncidentBehaviorAssociationReference:studentUniqueId::string as student_unique_id,

        -- edfi extensions
        value:_ext as v_ext
    from stg_discipline_actions
        {{ json_flatten('v_student_discipline_incident_behavior_associations') }}
),

-- Keep the deprecated Ed-Fi reference as a fallback. 
-- This reference does not tell us which behavior the action belongs to, so behavior_type is null for these rows.
incident_associations as (
    select
        tenant_code,
        api_year,
        discipline_action_id,
        discipline_date,
        k_student,
        k_student_xyear,
        {{ gen_skey('k_discipline_incident', alt_ref='value:studentDisciplineIncidentAssociationReference') }},
        null as behavior_type,
        value:studentDisciplineIncidentAssociationReference:incidentIdentifier::string as incident_id,
        value:studentDisciplineIncidentAssociationReference:schoolId::string as school_id,
        value:studentDisciplineIncidentAssociationReference:studentUniqueId::string as student_unique_id,

        -- edfi extensions
        value:_ext as v_ext
    from stg_discipline_actions
        {{ json_flatten('v_student_discipline_incident_associations') }}
),

-- Some implementations send both the current and deprecated references for the same action and incident.
-- Prefer the current behavior reference and use the deprecated reference only when it fills a gap.
gap_filling_incident_associations as (
    select
        incident_associations.*
    from incident_associations
    -- Note:
        -- Matching rows may fan out in the left join, but they are dropped by the null filter. 
        -- If the deprecated reference ever grows significantly, consider joining to distinct incident_behavior_associations instead.
    left join incident_behavior_associations
        on incident_behavior_associations.k_student             = incident_associations.k_student
        and incident_behavior_associations.k_discipline_incident = incident_associations.k_discipline_incident
        and  incident_behavior_associations.discipline_action_id  = incident_associations.discipline_action_id
        and incident_behavior_associations.discipline_date       = incident_associations.discipline_date
    where incident_behavior_associations.k_discipline_incident is null
),

flattened as (
    select * from incident_behavior_associations
    union all
    select * from gap_filling_incident_associations
),

extended as (
    select
        flattened.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from flattened
),

-- Remove exact duplicate references. 
-- Keep behavior_type in the partition because multiple behaviors for the same incident are valid and should remain separate rows.
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='extended',
            partition_by='discipline_action_id, discipline_date, k_student, k_discipline_incident, behavior_type',
            order_by='k_discipline_incident'
        )
    }}
)

select * from deduped