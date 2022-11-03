with course_offerings as (
    {{ source_edfi3('course_offerings') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string                                   as record_guid,
        v:courseReference:educationOrganizationId::int as ed_org_id,
        v:courseReference:courseCode::string           as course_code,
        v:schoolReference:schoolId::int                as school_id,
        v:sessionReference:schoolId::int               as session_school_id,
        v:sessionReference:schoolYear::int             as school_year,
        v:sessionReference:sessionName::string         as session_name,
        v:localCourseCode::string                      as local_course_code,
        v:localCourseTitle::string                     as local_course_title,
        v:instructionalTimePlanned::float              as instructional_time_planned,
        v:courseReference  as course_reference,
        v:schoolReference  as school_reference,
        v:sessionReference as session_reference,
        v:courseLevelCharacteristics as v_course_level_characteristics,
        v:curriculumUseds            as v_curriculum_used,
        v:offeredGradeLevels         as v_offered_grade_levels,

        -- edfi extensions
        v:_ext as v_ext
    from course_offerings
)
select * from renamed