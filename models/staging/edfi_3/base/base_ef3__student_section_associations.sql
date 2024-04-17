with student_section as (
    {{ source_edfi3('student_section_associations') }}
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
        v:id::string                                 as record_guid,
        v:studentReference:studentUniqueId::string   as student_unique_id,
        v:sectionReference:localCourseCode::string   as local_course_code,
        v:sectionReference:schoolId::integer         as school_id,
        v:sectionReference:schoolYear::integer       as school_year,
        v:sectionReference:sectionIdentifier::string as section_id,
        v:sectionReference:sessionName::string       as session_name,
        v:beginDate::date                            as begin_date,
        v:endDate::date                              as end_date,
        v:homeroomIndicator::boolean                 as is_homeroom,
        v:teacherStudentDataLinkExclusion::boolean   as teacher_student_data_link_exclusion,
        -- descriptors
        {{ extract_descriptor('v:attemptStatusDescriptor::string') }} as attempt_status,
        {{ extract_descriptor('v:repeatIdentifierDescriptor::string') }} as repeat_identifier,
        -- references
        v:studentReference as student_reference,
        v:sectionReference as section_reference,

        -- edfi extensions
        v:_ext as v_ext
    from student_section
)
select * from renamed