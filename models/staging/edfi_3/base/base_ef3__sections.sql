with sections as (
    {{ source_edfi3('sections') }}
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
        v:id::string                as record_guid,
        v:sectionIdentifier::string as section_id,
        v:sectionName::string       as section_name,
        -- course offering key
        v:courseOfferingReference:schoolId::integer             as school_id,
        v:courseOfferingReference:localCourseCode::string       as local_course_code,
        v:courseOfferingReference:sessionName::string           as session_name,
        v:courseOfferingReference:schoolYear::integer           as school_year,
        -- location key
        v:locationReference:classroomIdentificationCode::string as classroom_identification_code,
        v:locationReference:schoolId::integer                   as classroom_location_school_id,
        -- location school key
		v:locationSchoolReference:schoolId::integer             as school_location_school_id,
        -- values
        v:availableCreditConversion::float  as available_credit_conversion,
        v:availableCredits::float           as available_credits,
        v:sequenceOfCourse::integer         as sequence_of_course,
        v:officialAttendancePeriod::boolean as is_official_attendance_period,
        -- descriptors
        {{ extract_descriptor('v:availableCreditTypeDescriptor::string') }}    as available_credit_type,
        {{ extract_descriptor('v:educationalEnvironmentDescriptor::string') }} as educational_environment_type,
        {{ extract_descriptor('v:instructionLanguageDescriptor::string') }}    as instruction_language,
        {{ extract_descriptor('v:mediumOfInstructionDescriptor::string') }}    as medium_of_instruction,
        {{ extract_descriptor('v:populationServedDescriptor::string') }}       as population_served,
        {{ extract_descriptor('v:sectionTypeDescriptor::string') }}            as section_type,
        -- references
        v:courseOfferingReference as course_offering_reference,
        v:locationReference       as location_reference,
        v:locationSchoolReference as location_school_reference,
        -- lists
        v:characteristics            as v_section_characteristics,
        v:classPeriods               as v_class_periods,
        v:courseLevelCharacteristics as v_course_level_characteristics,
        v:offeredGradeLevels         as v_offered_grade_levels,
        v:programs                   as v_programs,

        -- edfi extensions
        v:_ext as v_ext
    from sections
)
select * from renamed