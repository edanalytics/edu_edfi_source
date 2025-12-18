with program_evaluations as (
    {{ source_edfi3('program_evaluations') }}
),

renamed as (
    select

        -- generic
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,
        ods_version,
        data_model_version,

        v:id::string                                    as record_guid,
        v:evaluationMaxNumericRating::float             as evaluation_max_numeric_rating,
        v:evaluationMinNumericRating::float             as evaluation_min_numeric_rating,
        v:programEvaluationDescription::string          as program_evaluation_description,
        v:programEvaluationTitle::string                as program_evaluation_title,
        v:programReference:educationOrganizationId::int as program_ed_org_id,
        v:programReference:programName::string          as program_name,

        -- descriptors
        {{ extract_descriptor('v:programEvaluationPeriodDescriptor::string') }}      as program_evaluation_period,
        {{ extract_descriptor('v:programEvaluationTypeDescriptor::string') }}        as program_evaluation_type,
        {{ extract_descriptor('v:programReference:programTypeDescriptor::string') }} as program_type,

        -- references
        v:programReference as program_reference,

        -- lists
        v:levels as v_program_evaluation_levels,

        -- ed-fi extensions
        v:_ext as v_ext

    from program_evaluations
)

select * from renamed
