with idea_events as (
    {{ source_edfi3('idea_events') }}
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
        v:id::string as record_guid,
        ods_version,
        data_model_version,
        -- key columns
        v:ideaEventID::string                                         as idea_event_id,
        v:studentReference:studentUniqueId::string                    as student_unique_id,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        {{ extract_descriptor('v:ideaEventDescriptor::string') }}     as idea_event,
        -- value columns
        v:eventBeginDate::date   as event_begin_date,
        v:eventEndDate::date     as event_end_date,
        v:eventNarrative::string as event_narrative,
        -- descriptors
        {{ extract_descriptor('v:eventReasonDescriptor::string') }}     as event_reason,
        {{ extract_descriptor('v:eventComplianceDescriptor::string') }} as event_compliance,
        -- references
        v:educationOrganizationReference as education_organization_reference,
        v:studentReference as student_reference,
        
        -- edfi extensions
        v:_ext as v_ext

    from idea_events
)

select * from renamed
