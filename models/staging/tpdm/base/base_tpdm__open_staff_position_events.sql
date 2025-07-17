with open_staff_position_events as (
    {{ source_edfi3('open_staff_position_events') }}
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
        v:eventDate::date                                                          as event_date,
        {{ extract_descriptor('v:openStaffPositionEventTypeDescriptor::string') }} as open_staff_position_event_type,  -- name event_type instead?
        v:openStaffPositionReference:educationOrganizationId::int                  as ed_org_id,
        v:openStaffPositionReference:requisitionNumber::string                     as requisition_number,
        -- descriptors
        {{ extract_descriptor('v:openStaffPositionEventStatusDescriptor::string') }} as open_staff_position_event_status,  -- name event_status instead?
        -- references
        v:openStaffPositionReference as open_staff_position_reference,
        -- edfi extensions
        v:_ext as v_ext

    from open_staff_position_events
)
select * from renamed
