with courses as (
    {{ source_edfi3('courses') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string as record_guid,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:courseCode::string                    as course_code,
        v:courseTitle::string                   as course_title,
        v:courseDescription::string             as course_description,
        v:dateCourseAdopted::date               as date_course_adopted,
        v:highSchoolCourseRequirement::boolean  as is_high_school_course_requirement,
        v:maxCompletionsForCtredit::int         as max_completions_for_credit,
        v:maximumAvailableCreditConversion::int as maximum_available_credit_conversion,
        v:maximumAvailableCredits::int          as maximum_available_credits,
        v:minimumAvailableCreditConversion::int as minimum_available_credit_conversion,
        v:minimumAvailableCredits::int          as minimum_available_credits,
        v:numberOfParts::int                    as number_of_parts,
        v:timeRequiredForCompletion::int        as time_required_for_completion,
        {{ extract_descriptor('v:academicSubjectDescriptor::string') }}            as academic_subject,
        {{ extract_descriptor('v:careerPathwayDescriptor::string') }}              as career_pathway,
        {{ extract_descriptor('v:courseDefinedByDescriptor::string') }}            as course_defined_by,
        {{ extract_descriptor('v:courseGPAApplicabilityDescriptor::string') }}     as gpa_applicability,
        {{ extract_descriptor('v:maximumAvailableCreditTypeDescriptor::string') }} as maximum_available_credit_type,
        {{ extract_descriptor('v:minimumAvailableCreditDescriptor::string') }}     as minimum_available_credit_type,
        -- unflattened lists
        v:identificationCodes  as v_identification_codes,
        v:competencyLevels     as v_competency_levels,
        v:learningObjectives   as v_learning_objectives,
        v:learningStandards    as v_learning_standards,
        v:levelCharacteristics as v_level_characteristics,
        v:offeredGradeLevels   as v_offered_grade_levels,

        -- edfi extensions
        v:_ext as v_ext
    from courses
)
select * from renamed