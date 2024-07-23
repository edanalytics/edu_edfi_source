with performance_evaluations as (
    {{ source_edfi3('performance_evaluations') }}
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

        v:id::string                                                  as record_guid,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:performanceEvaluationTitle::string                          as performance_evaluation_title,
        v:performanceEvaluationDescription::string                    as performance_evaluation_description,
        v:schoolYearTypeReference:schoolYear::int                     as school_year,
        -- descriptors
        {{ extract_descriptor('v:termDescriptor::string') }}                      as academic_term,
        {{ extract_descriptor('v:performanceEvaluationTypeDescriptor::string') }} as performance_evaluation_type,
        {{ extract_descriptor('v:academicSubjectDescriptor::string') }}           as academic_subject,
        {{ extract_descriptor('v:evaluationPeriodDescriptor::string') }}          as evaluation_period_descriptor,
        -- unflattened lists
        v:gradeLevels  as v_grade_levels,
        v:ratingLevels as v_rating_levels,
        -- references
        v:educationOrganizationReference as education_organization_reference
        v:schoolYearTypeReference        as school_year_type_reference
    from performance_evaluations
)
select * from renamed
