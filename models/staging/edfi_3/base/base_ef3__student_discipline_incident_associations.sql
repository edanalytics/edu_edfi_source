-- note: this model is deprecated, but still in use
with student_discipline_incident as (
    {{ source_edfi3('student_discipline_incident_associations') }}
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
        ods_version,
        data_model_version,
        v:disciplineIncidentReference:incidentIdentifier::string as incident_id,
        v:disciplineIncidentReference:schoolId::int              as school_id,
        v:studentReference:studentUniqueId::string               as student_unique_id,
        -- descriptors
        {{ extract_descriptor('v:studentParticipationCodeDescriptor::string') }} as student_participation_code,
        -- references
        v:disciplineIncidentReference as discipline_incident_reference,
        v:studentReference           as student_reference,
        -- lists
        v:behaviors                  as v_behaviors,

        -- edfi extensions
        v:_ext as v_ext
    from student_discipline_incident
)
select * from renamed