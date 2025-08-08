with recruitment_events as (
    {{ source_edfi3('recruitment_events') }}
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
        v:eventDate::date                                             as event_date,
        v:eventTitle::string                                          as event_title,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        -- non-identity components
        v:eventDescription::string as event_description,
        v:eventLocation::string    as event_location,
        -- descriptors
        {{ extract_descriptor('v:recruitmentEventTypeDescriptor::string') }} as recruitment_event_type,
        -- references
        v:educationOrganizationReference as education_organization_reference,
        -- edfi extensions
        v:_ext as v_ext

    from recruitment_events
)
select * from renamed