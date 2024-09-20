with learning_standards as (
    {{ source_edfi3('learning_standards') }}
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
        -- ids
        v:id::string                              as record_guid,
        ods_version,
        data_model_version,
        v:learningStandardId::string              as learning_standard_id,
        v:identificationCodes                     as v_learning_standard_identification_codes,
        -- descriptions
        {{ extract_descriptor('v:learningStandardCategoryDescriptor::string') }} as learning_standard_category,
        {{ extract_descriptor('v:learningStandardScopeDescriptor::string') }}    as learning_standard_scope,
        v:learningStandardItemCode::string        as learning_standard_item_code,
        v:courseTitle::string                     as course_title,
        v:description::string                     as learning_standard_description,
        v:academicSubjects                        as v_academic_subjects,
        v:contentStandard                         as v_content_standard,
        v:gradeLevels                             as v_grade_levels,
        v:prerequisiteLearningStandards           as v_prerequisite_learning_standards,
        v:successCriteria::string                 as success_criteria,
        v:namespace::string                       as namespace,
        v:uri::string                             as uri,
        -- references
        v:parentLearningStandardReference         as parent_learning_standard_reference,
        -- edfi extensions
        v:_ext                                    as v_ext
    from learning_standards
)
select * from renamed
