with student_discipline_incident_behavior as (
    {{ source_edfi3('student_discipline_incident_behavior_associations') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        filename,
        file_row_number,
        is_deleted,
        v:id::string                                             as record_guid,
        v:disciplineIncidentReference:incidentIdentifier::string as incident_id,
        v:disciplineIncidentReference:schoolId::int              as school_id,
        v:studentReference:studentUniqueId::string               as student_unique_id,
        v:behaviorDetailedDescription::string                    as behavior_detailed_description,
        -- descriptors
        {{ extract_descriptor('v:behaviorDescriptor::string') }} as behavior_type,
        -- references
        v:disciplineIncidentReference as discipline_incident_reference,
        v:studentReference           as student_reference,
        -- lists
        v:disciplineIncidentParticipationCodes as v_discipline_incident_participation_codes,

        -- edfi extensions
        v:_ext as v_ext
    from student_discipline_incident_behavior
)
select * from renamed