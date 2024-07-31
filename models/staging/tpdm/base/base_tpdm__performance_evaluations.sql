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

        v:id::string as record_guid,
        -- identity components
        v:educationOrganizationReference:educationOrganizationId::int             as ed_org_id,
        {{ extract_descriptor('v:evaluationPeriodDescriptor::string') }}          as evaluation_period,
        v:performanceEvaluationTitle::string                                      as performance_evaluation_title,
        {{ extract_descriptor('v:performanceEvaluationTypeDescriptor::string') }} as performance_evaluation_type,
        v:schoolYearTypeReference:schoolYear::int                                 as school_year,
        {{ extract_descriptor('v:termDescriptor::string') }}                      as academic_term,
        -- non-identity components
        v:performanceEvaluationDescription::string as performance_evaluation_description,
        -- descriptors
        {{ extract_descriptor('v:academicSubjectDescriptor::string') }} as academic_subject,
        -- unflattened lists
        v:gradeLevels  as v_grade_levels,
        v:ratingLevels as v_rating_levels,
        -- references
        v:educationOrganizationReference as education_organization_reference,
        v:schoolYearTypeReference        as school_year_type_reference,
        -- edfi extensions
        v:_ext as v_ext
    from performance_evaluations
)
select * from renamed
