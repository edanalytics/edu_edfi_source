with educator_prep_programs as (
    {{ source_edfi3('evaluation_rating') }}
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

        v:evaluationDate::date    as evaluation_date,
        v:comments::string        as comments,
        v:actualDuration::string  as actual_duration,
        -- descriptors
        {{ extract_descriptor('v:evaluationRatingStatusDescriptor::string') }} as evaluation_rating_status,
        {{ extract_descriptor('v:evaluationRatingLevelDescriptor::string') }}  as evaluation_rating_level,
        -- unflattened lists
        v:reviewers as reviewers,
        v:results   as results,
        -- references
        v:evaluationReference                  as evaluation_reference,
        v:performanceEvaluationRatingReference as performance_evaluation_rating_reference,
        v:sectionReference                     as section_reference
    from educator_prep_programs
)
select * from renamed
