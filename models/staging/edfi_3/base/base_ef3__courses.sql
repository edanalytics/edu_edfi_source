with courses as (
    {{ source_edfi3('courses') }}
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
        {{json_extract('id', 'string')}} as record_guid,
        
        {{json_extract('educationOrganizationReference.educationOrganizationId', 'int')}} as ed_org_id,
        {{json_extract('educationOrganizationReference.link.rel', 'string')}}             as ed_org_type,
        {{json_extract('courseCode', 'string')}}                    as course_code,
        {{json_extract('courseTitle', 'string')}}                   as course_title,
        {{json_extract('courseDescription', 'string')}}             as course_description,
        {{json_extract('dateCourseAdopted', 'date')}}               as date_course_adopted,
        {{json_extract('highSchoolCourseRequirement', 'boolean')}}  as is_high_school_course_requirement,
        {{json_extract('maxCompletionsForCredit', 'int')}}          as max_completions_for_credit,
        {{json_extract('maximumAvailableCreditConversion', 'int')}} as maximum_available_credit_conversion,
        {{json_extract('maximumAvailableCredits', 'int')}}          as maximum_available_credits,
        {{json_extract('minimumAvailableCreditConversion', 'int')}} as minimum_available_credit_conversion,
        {{json_extract('minimumAvailableCredits', 'int')}}          as minimum_available_credits,
        {{json_extract('numberOfParts', 'int')}}                    as number_of_parts,
        {{json_extract('timeRequiredForCompletion', 'int')}}        as time_required_for_completion,
        {{extract_descriptor(json_extract('academicSubjectDescriptor', 'string'))}}            as academic_subject,
        {{extract_descriptor(json_extract('careerPathwayDescriptor','string'))}}                as career_pathway,
        {{extract_descriptor(json_extract('courseDefinedByDescriptor', 'string'))}}            as course_defined_by,
        {{extract_descriptor(json_extract('courseGPAApplicabilityDescriptor', 'string'))}}     as gpa_applicability,
        {{extract_descriptor(json_extract('maximumAvailableCreditTypeDescriptor', 'string'))}} as maximum_available_credit_type,
        {{extract_descriptor(json_extract('minimumAvailableCreditDescriptor', 'string'))}}      as minimum_available_credit_type,
        -- references
        {{json_extract('educationOrganizationReference')}} as education_organization_reference,
        -- unflattened lists
        {{json_extract('identificationCodes')}}  as v_identification_codes,
        {{json_extract('competencyLevels')}}     as v_competency_levels,
        {{json_extract('learningObjectives')}}   as v_learning_objectives,
        {{json_extract('learningStandards')}}    as v_learning_standards,
        {{json_extract('levelCharacteristics')}} as v_level_characteristics,
        {{json_extract('offeredGradeLevels')}}   as v_offered_grade_levels,

        -- edfi extensions
        {{json_extract('_ext')}} as v_ext
    from courses
)
select * from renamed
