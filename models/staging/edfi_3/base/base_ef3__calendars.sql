with calendars as (
    {{ source_edfi3('calendars') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string                                  as record_guid,
        v:calendarCode::string                        as calendar_code,
        v:schoolReference:schoolId::integer           as school_id,
        v:schoolYearTypeReference:schoolYear::integer as school_year,
        {{ extract_descriptor('v:calendarTypeDescriptor::string') }} as calendar_type,
        v:schoolReference as school_reference,
        v:gradeLevels     as v_grade_levels,

        -- edfi extensions
        v:_ext as v_ext
    from calendars
)
select * from renamed