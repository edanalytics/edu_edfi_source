with staff_school as (
    {{ source_edfi3('staff_school_associations') }}
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
        v:id::string                              as record_guid,
        v:staffReference:staffUniqueId::string    as staff_unique_id,
        v:schoolReference:schoolId::int           as school_id,
        v:calendarReference:calendarCode::string  as calendar_code,
        v:calendarReference:schoolId::int         as calendar_school_id,
        v:calendarReference:schoolYear::int       as calendar_school_year,
        v:schoolYearTypeReference:schoolYear::int as school_year,
        -- descriptors
        {{ extract_descriptor('v:programAssignmentDescriptor') }} as program_assignment,
        -- references
        v:calendarReference as calendar_reference,
        v:schoolReference   as school_reference,
        v:staffReference    as staff_reference,
        -- lists 
        v:academic_subjects as v_academic_subjects,
        v:gradeLevels       as v_grade_levels,

        -- edfi extensions
        v:_ext as v_ext
    from staff_school
)
select * from renamed