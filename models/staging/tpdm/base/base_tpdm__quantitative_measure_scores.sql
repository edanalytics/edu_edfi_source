with quantitative_measure_scores as (
    {{ source_edfi3('quantitative_measure_scores') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        filename,
        file_row_number,
        is_deleted,

        v:id::string as record_guid,
        -- identity components
        v:quantitativeMeasureReference:quantitativeMeasureIdentifier::string                                       as quantitative_measure_identifier,
        v:quantitativeMeasureReference:educationOrganizationId::int                                                as quant_msr_ed_org_id,
        v:quantitativeMeasureReference:evaluationElementTitle::string                                              as quant_msr_evaluation_element_title,
        v:quantitativeMeasureReference:evaluationObjectiveTitle::string                                            as quant_msr_evaluation_objective_title,
        {{ extract_descriptor('v:quantitativeMeasureReference:evaluationPeriodDescriptor::string') }}              as quant_msr_evaluation_period,
        v:quantitativeMeasureReference:evaluationTitle::string                                                     as quant_msr_evaluation_title,
        v:quantitativeMeasureReference:performanceEvaluationTitle::string                                          as quant_msr_performance_evaluation_title,
        {{ extract_descriptor('v:quantitativeMeasureReference:performanceEvaluationTypeDescriptor::string') }}     as quant_msr_performance_evaluation_type,
        v:quantitativeMeasureReference:schoolYear::int                                                             as quant_msr_school_year,
        {{ extract_descriptor('v:quantitativeMeasureReference:termDescriptor::string') }}                          as quant_msr_academic_term,
        
        v:evaluationElementRatingReference:evaluationDate::date                                                    as eval_elem_rating_evaluation_date,
        v:evaluationElementRatingReference:personId::string                                                        as eval_elem_rating_person_id,  
        v:evaluationElementRatingReference:educationOrganizationId::int                                            as eval_elem_rating_ed_org_id,
        v:evaluationElementRatingReference:evaluationElementTitle::string                                          as eval_elem_rating_evaluation_element_title,
        v:evaluationElementRatingReference:evaluationObjectiveTitle::string                                        as eval_elem_rating_evaluation_objective_title,
        {{ extract_descriptor('v:evaluationElementRatingReference:evaluationPeriodDescriptor::string') }}          as eval_elem_rating_evaluation_period,
        v:evaluationElementRatingReference:evaluationTitle::string                                                 as eval_elem_rating_evaluation_title,
        v:evaluationElementRatingReference:performanceEvaluationTitle::string                                      as eval_elem_rating_performance_evaluation_title,
        {{ extract_descriptor('v:evaluationElementRatingReference:performanceEvaluationTypeDescriptor::string') }} as eval_elem_rating_evaluation_type,
        v:evaluationElementRatingReference:schoolYear::int                                                         as eval_elem_rating_school_year,
        {{ extract_descriptor('v:evaluationElementRatingReference:termDescriptor::string') }}                      as eval_elem_rating_academic_term,
        {{ extract_descriptor('v:evaluationElementRatingReference:sourceSystemDescriptor::string') }}              as eval_elem_rating_source_system,
        -- non-identity components
        v:scoreValue::string    as score_value,
        v:standardError::string as standard_error,
        -- references
        v:quantitativeMeasureReference     as quantitative_measure_reference,
        v:evaluationElementRatingReference as evaluation_element_rating_reference,
        -- edfi extensions
        v:_ext as v_ext

    from quantitative_measure_scores
)
select * from renamed