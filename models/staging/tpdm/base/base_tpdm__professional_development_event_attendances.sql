with professional_development_event_attendances as (
    {{ source_edfi3('professional_development_event_attendances') }}
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
        -- identity components
        v:attendanceDate::date                                                       as attendance_date,
        v:personReference:personId::string                                           as person_id,
        {{ extract_descriptor('v:personReference:sourceSystemDescriptor::string') }} as person_source_system,
        v:professionalDevelopmentEventReference:namespace::string                    as professional_development_event_namespace,
        v:professionalDevelopmentEventReference:professionalDevelopmentTitle::string as professional_development_title,
        -- non-identity components
        v:attendanceEventReason::string as attendance_event_reason,
        -- descriptors
        {{ extract_descriptor('v:attendanceEventCategoryDescriptor::string') }} as attendance_event_category,
        -- references
        v:personReference                       as person_reference,
        v:professionalDevelopmentEventReference as professional_development_event_reference,
        -- edfi extensions
        v:_ext as v_ext
    from professional_development_event_attendances
)
select * from renamed
