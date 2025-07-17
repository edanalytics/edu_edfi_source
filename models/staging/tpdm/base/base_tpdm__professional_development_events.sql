with professional_development_events as (
    {{ source_edfi3('professional_development_events') }}
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
        v:professionalDevelopmentTitle::string as professional_development_title,
        v:namespace::string                    as namespace,
        -- non-identity components
        v:multipleSession::boolean              as is_multiple_session,
        v:professionalDevelopmentReason::string as professional_development_reason,
        v:required::boolean                     as is_required,
        v:totalHoura::integer                   as total_hours,  -- should this be float or number type instead?
        -- descriptors
        {{ extract_descriptor('v:professionalDevelopmentOfferedByDescriptor::string') }} as offered_by,
        -- references
        v:openStaffPositionReference as open_staff_position_reference,
        -- edfi extensions
        v:_ext as v_ext

    from professional_development_events
)
select * from renamed
