with course_transcripts as (
    {{ source_edfi3('course_transcripts') }}
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
				--identity components
				{{ extract_descriptor('v:courseAttemptResultDescriptor::string') }} as course_attempt_result,
        v:courseReference:educationOrganizationId::int                      as course_ed_org_id,
        v:courseReference:courseCode::string                                as course_code,
        v:studentAcademicRecordReference:educationOrganizationId::int       as student_academic_record_ed_org_id,
        v:studentAcademicRecordReference:schoolYear::int                    as school_year,
        v:studentAcademicRecordReference:studentUniqueId::string            as student_unique_id,
        {{ extract_descriptor('v:studentAcademicRecordReference:termDescriptor::string') }} as academic_term,
				--non-identity components
        v:courseTitle::string                             as course_title,
        v:alternativeCourseCode::string                   as alternative_course_code,
        v:alternativeCourseTitle::string                  as alternative_course_title,
        v:finalLetterGradeEarned::string                  as final_letter_grade_earned,
        v:finalNumericGradeEarned::float                  as final_numeric_grade_earned,
        v:earnedCredits::float                            as earned_credits,
        v:earnedCreditConversion::float                   as earned_credit_conversion,
        v:attemptedCredits::float                         as attempted_credits,
        v:attemptedCreditConversion::float                as attempted_credit_conversion,
        v:assigningOrganizationIdentificationCode::string as assigning_organization_identification_code,
        v:courseCatalogURL::string                        as course_catalog_url,
        -- descriptors
        {{ extract_descriptor('v:courseRepeatCodeDescriptor::string') }}      as course_repeat_code,
        {{ extract_descriptor('v:attemptedCreditTypeDescriptor::string') }}   as attempted_credit_type,
        {{ extract_descriptor('v:earnedCreditTypeDescriptor::string') }}      as earned_credit_type,
        {{ extract_descriptor('v:methodCreditEarnedDescriptor::string') }}    as method_credit_earned,
        {{ extract_descriptor('v:whenTakenGradeLevelDescriptor::string') }}   as when_taken_grade_level,
				v:externalEducationOrganizationReference:educationOrganizationId::int as external_ed_org_reference_ed_org_id,
        -- references
        v:courseReference                        as course_reference,
        v:studentAcademicRecordReference         as student_academic_record_reference,
        v:externalEducationOrganizationReference as external_education_organization_reference,
				-- non-identity collection components
				v:earnedAdditionalCredits              as v_earned_additional_credits,
        v:academicSubjects                     as v_academic_subjects,
        v:alternativeCourseIdentificationCodes as v_alternative_course_identification_codes,
        v:creditCategories                     as v_credit_categories,

        -- edfi extensions
        v:_ext as v_ext
    from course_transcripts
)
select * from renamed
