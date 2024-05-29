with objective_assessments as (
    {{ source_edfi3('objective_assessments') }}
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
        v:id::string                                       as record_guid,
        v:assessmentReference:assessmentIdentifier::string as assessment_identifier,
        v:assessmentReference:namespace::string            as namespace,
        v:identificationCode::string                       as objective_assessment_identification_code,
        v:description::string                              as objective_assessment_description,
        v:maxRawScore::float                               as max_raw_score,
        v:nomenclature::string                             as nomenclature,
        v:percentOfAssessment::float                       as percent_of_assessment,
        -- descriptors
        {{ extract_descriptor('v:academicSubjectDescriptor::string') }} as academic_subject_descriptor,
        -- references
        v:assessmentReference                as assessment_reference,
        v:parentObjectiveAssessmentReference as parent_objective_assessment_reference,
        -- unflattened lists
        v:assessmentItems    as v_assessment_items,
        v:learningObjectives as v_learning_objectives,
        v:learningStandards  as v_learning_standards,
        v:performanceLevels  as v_performance_levels,
        v:scores             as v_scores,
        -- edfi extensions
        v:_ext               as v_ext
    from objective_assessments
)
select * from renamed