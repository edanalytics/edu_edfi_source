with educator_prep_programs as (
    {{ source_edfi3('performance_evaluation') }}
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

        v:performanceEvaluationTitle::string       as performance_evaluation_title,
        v:performanceEvaluationDescription::string as performance_evaluation_description,
        -- descriptors
        {{ extract_descriptor('v:termDescriptor::string') }}                      as term,
        {{ extract_descriptor('v:performanceEvaluationTypeDescriptor::string') }} as performance_evaluation_type,
        {{ extract_descriptor('v:academicSubjectDescriptor::string') }}           as academic_subjec,
        {{ extract_descriptor('v:gradeLevels::string') }}                         as grade_levels,
        {{ extract_descriptor('v:programGateways::string') }}                     as program_gateways,
        {{ extract_descriptor('v:evaluationPeriodDescriptor::string') }}          as evaluation_period_descriptor,
        -- unflattened lists
        v:ratingLevels as rating_levels,
        -- references
        v:educationOrganizationReference as education_organization_reference
        v:schoolYearTypeReference        as school_year_type_reference
    from educator_prep_programs
)
select * from renamed
