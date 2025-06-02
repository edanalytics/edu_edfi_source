with discipline_incident as (
    {{ source_edfi3('discipline_incidents') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string                                  as record_guid,
        ods_version,
        data_model_version,
        v:incidentIdentifier::string                  as incident_id,
        v:schoolReference:schoolId::int               as school_id,
        v:incidentDate::date                          as incident_date,
        v:incidentTime::string                        as incident_time,
        v:caseNumber::string                          as case_number,
        v:incidentCost::float                         as incident_cost,
        v:incidentDescription::string                 as incident_description,
        v:reportedToLawEnforcement::boolean           as was_reported_to_law_enforcement,
        v:reporterName::string                        as reporter_name,
        --descriptors
        {{ extract_descriptor('v:reporterDescriptionDescriptor::string') }} as reporter_description,
        {{ extract_descriptor('v:incidentLocationDescriptor::string') }}    as incident_location,
        --lists
        v:behaviors            as v_behaviors,
        v:externalParticipants as v_external_participants,
        v:weapons              as v_weapons,
        --references
        v:schoolReference      as school_reference,
        --deprecated
        v:staffReference       as staff_reference,
        v:staffReference:staffUniqueId::string as staff_unique_id,

        -- edfi extensions
        v:_ext as v_ext
    from discipline_incident
)
select * from renamed
