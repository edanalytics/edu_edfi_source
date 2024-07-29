with evaluation_ratings as (
    {{ source_edfi3('evaluation_ratings') }}
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
        v:evaluationReference:educationOrganizationId::int                                                as ed_org_id,
        v:evaluationDate::timestamp                                                                       as evaluation_date,
        {{ extract_descriptor('v:evaluationReference:evaluationPeriodDescriptor::string') }}              as evaluation_period,
        v:evaluationReference:evaluationTitle::string                                                     as evaluation_title,
        v:evaluationReference:performanceEvaluationTitle::string                                          as performance_evaluation_title,
        {{ extract_descriptor('v:evaluationReference:performanceEvaluationTypeDescriptor::string') }}     as performance_evaluation_type,
        v:performanceEvaluationRatingReference:personId::string                                           as person_id,
        v:performanceEvaluationRatingReference:schoolYear::int                                            as school_year,
        {{ extract_descriptor('v:performanceEvaluationRatingReference:sourceSystemDescriptor::string') }} as source_system,
        {{ extract_descriptor('v:performanceEvaluationRatingReference:termDescriptor::string') }}         as academic_term,
        -- non-identity components
        v:sectionReference:sectionIdentifier::string as section_id,
        -- descriptors
        {{ extract_descriptor('v:evaluationRatingStatusDescriptor::string') }} as evaluation_rating_status,
        {{ extract_descriptor('v:evaluationRatingLevelDescriptor::string') }}  as evaluation_rating_level,
        -- unflattened lists
        v:reviewers as v_reviewers,
        v:results   as v_results,
        -- references
        v:evaluationReference                  as evaluation_reference,
        v:performanceEvaluationRatingReference as performance_evaluation_rating_reference,
        v:sectionReference                     as section_reference,
        -- edfi extensions
        v:_ext as v_ext
    from evaluation_ratings
)
select * from renamed
