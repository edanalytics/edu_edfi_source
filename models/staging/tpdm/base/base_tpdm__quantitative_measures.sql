with quantitative_measures as (
    {{ source_edfi3('quantitative_measures') }}
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
        v:evaluationElementReference:evaluationTitle::string                                        as evaluation_title,
        v:evaluationElementReference:evaluationElementTitle::string                                 as evaluation_element_title,
        v:evaluationElementReference:evaluationObjectiveTitle::string                               as evaluation_objective_title,
        {{ extract_descriptor('v:evaluationElementReference:evaluationPeriodDescriptor::string') }} as evaluation_period,
        v:evaluationElementReference:performanceEvaluationTitle::string                             as performance_evaluation_title,
        v:evaluationElementReference:performanceEvaluationTypeDescriptor::string                    as performance_evaluation_type,
        v:evaluationElementReference:educationOrganizationId::int                                   as ed_org_id,
        v:evaluationElementReference:schoolYear::int                                                as school_year,
        {{ extract_descriptor('v:evaluationElementReference:termDescriptor::string') }}             as academic_term,
        v:quantitativeMeasureIdentifier::string                                                     as quantitative_measure_identifier,
        -- descriptors
        {{ extract_descriptor('v:quantitativeMeasureTypeDescriptor::string') }}     as quantitative_measure_type,
        {{ extract_descriptor('v:quantitativeMeasureDatatypeDescriptor::string') }} as quantitative_measure_datatype,
        -- references
        v:evaluationElementReference as evaluation_element_reference,
        -- edfi extensions
        v:_ext as v_ext

    from quantitative_measures
)
select * from renamed