with staff_section as (
    {{ source_edfi3('staff_section_associations') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string                                 as record_guid,
        -- section key
        v:sectionReference:localCourseCode::string   as local_course_code,
        v:sectionReference:schoolId::int             as school_id,
        v:sectionReference:schoolYear::int           as school_year,
        v:sectionReference:sectionIdentifier::string as section_id,
        v:sectionReference:sessionName::string                        as session_name,
        -- staff key
        v:staffReference:staffUniqueId::string       as staff_unique_id,
        -- fields
        v:beginDate::date                            as begin_date,
        v:endDate::date                              as end_date,
        v:highlyQualifiedTeacher::boolean            as is_highly_qualified_teacher,
        v:percentageContribution::float              as percentage_contribution,
        v:teacherStudentDataLinkExclusion::boolean   as teacher_student_data_link_exclusion,
        -- descriptors
        {{ extract_descriptor('v:classroomPositionDescriptor::string') }} as classroom_position,
        -- references
        v:sectionReference as section_reference,
        v:staffReference   as staff_reference,

        -- edfi extensions
        v:_ext as v_ext
    from staff_section
)
select * from renamed