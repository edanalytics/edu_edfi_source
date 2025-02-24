with student_assessments as (
    {{ source_edfi3('student_assessments') }}
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
        ods_version,
        data_model_version,
        v:assessmentReference:assessmentIdentifier::string as assessment_identifier,
        v:assessmentReference:namespace::string            as namespace,
        v:schoolYearTypeReference:schoolYear::int          as school_year,
        try_to_timestamp(v:administrationDate::string)     as administration_date,
        try_to_timestamp(v:administrationEndDate::string)  as administration_end_date,
        v:studentAssessmentIdentifier::string              as student_assessment_identifier,
        v:studentReference:studentUniqueId::string         as student_unique_id,
        v:eventDescription::string                         as event_description,
        v:serialNumber::string                             as serial_number,
        -- descriptors
        {{ extract_descriptor('v:administrationEnvironmentDescriptor::string') }} as administration_environment,
        {{ extract_descriptor('v:administrationLanguageDescriptor::string') }}    as administration_language,
        {{ extract_descriptor('v:eventCircumstanceDescriptor::string') }}         as event_circumstance,
        {{ extract_descriptor('v:platformTypeDescriptor::string') }}              as platform_type,
        {{ extract_descriptor('v:reasonNotTestedDescriptor::string') }}           as reason_not_tested,
        {{ extract_descriptor('v:retestIndicatorDescriptor::string') }}           as retest_indicator,
        {{ extract_descriptor('v:whenAssessedGradeLevelDescriptor::string') }}    as when_assessed_grade_level,
        -- references
        v:assessmentReference         as assessment_reference,
        v:studentReference            as student_reference,
        v:schoolYearTypeReference     as school_year_type_reference,
        -- unflattened lists
        v:scoreResults                as v_score_results,
        v:performanceLevels           as v_performance_levels,
        v:items                       as v_items,
        v:studentObjectiveAssessments as v_student_objective_assessments,
        v:accommodations              as v_accommodations,
        -- edfi extensions
        v:_ext as v_ext
    from student_assessments
    where administration_date != ''
)
select * from renamed
