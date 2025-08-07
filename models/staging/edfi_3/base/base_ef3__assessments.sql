with assessments as (
    {{ source_edfi3('assessments') }}
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

        v:id::string                   as record_guid,
        ods_version,
        data_model_version,
        v:assessmentIdentifier::string as assessment_identifier,
        v:namespace::string            as namespace,
        v:assessmentTitle::string      as assessment_title,
        v:assessmentFamily::string     as assessment_family,
        v:assessmentForm::string       as assessment_form,
        v:assessmentVersion::string    as assessment_version,
        v:maxRawScore::float           as max_raw_score,
        v:nomenclature::string         as nomenclature,
        v:adaptiveAssessment::boolean  as is_adaptive_assessment,
        v:revisionDate::date           as revision_date,
        v:period:beginDate::date       as assessment_period_begin_date,
        v:period:endDate::date         as assessment_period_end_date,
        v:contentStandard              as content_standard,
        -- descriptors
        {{ extract_descriptor('v:assessmentCategoryDescriptor::string') }}      as assessment_category,
        {{ extract_descriptor('v:period:assessmentPeriodDescriptor::string') }} as assessment_period,
        case
            when {{ json_array_size('v:academicSubjects') }} > 1
                then False
            else True
        end                            as is_single_subject_identifier,
        --references
        v:educationOrganizationReference as education_organization_reference,
        -- unflattened lists
        v:academicSubjects    as v_academic_subjects,
        v:assessedGradeLevels as v_assessed_grade_levels,
        v:performanceLevels   as v_performance_levels,
        v:scores              as v_scores,
        v:identificationCodes as v_identification_codes,
        v:languages           as v_languages,
        v:platformTypes       as v_platform_types,
        v:programs            as v_programs,
        v:sections            as v_sections,
        v:authors             as v_authors,
        -- unused
        v:contentStandard     as v_content_standard,
        -- edfi extensions
        v:_ext                as v_ext
    from assessments
)
select * from renamed
