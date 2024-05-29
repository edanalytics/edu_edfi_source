with student_discipline_incident_nonoffender as (
    {{ source_edfi3('student_discipline_incident_non_offender_associations') }}
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
        -- references
        v:discipineIncidentReference as discipline_incident_reference,
        v:studentReference           as student_reference,
        -- lists
        v:disciplineIncidentParticipationCodes as v_discipline_incident_participation_codes,

        -- edfi extensions
        v:_ext as v_ext
    from student_discipline_incident_nonoffender
)
select * from renamed