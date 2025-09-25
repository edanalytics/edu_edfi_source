with app_events as (
    {{ source_edfi3('application_events') }}
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

        v:id::string as record_guid,
        -- identity components
        v:credentialEventDate::date                                                                       as credential_event_date,
        {{ extract_descriptor('v:credentialEventTypeDescriptor::string') }}                               as credential_event_type,
        v:credentialReference:credentialIdentifier::string                                                as credential_identifier,
        {{ extract_descriptor('v:credentialReference:stateOfIssueStateAbbreviationDescriptor::string') }} as state_of_issue,
        -- non-identity components
        v:credentialEventReason::string as credential_event_reason
        -- references
        v:credentialReference as credential_reference
        -- edfi extensions
        v:_ext as v_ext

    from app_events
)
select * from renamed
