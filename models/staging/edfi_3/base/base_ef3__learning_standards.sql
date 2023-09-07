with learning_standards as (
    {{ source_edfi3('learning_standards') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        -- ids
        v:id::string                              as record_guid,
        v:learningStandardId::string              as learning_standard_id,
        v:learningStandardIdentificationCode      as learning_standard_identification_code,
        -- descriptions
        v:learningStandardItemCode::string        as learning_standard_item_code,
        v:learningStandardCategory                as learning_standard_category,
        v:learningStandardScope                   as learning_standard_scope,
        v:academicSubjects                        as academic_subjects,
        v:contentStandard                         as content_standard,
        v:courseTitle::string                     as course_title,
        v:description::string                     as course_description,
        v:gradeLevels                             as grade_level,
        -- references
        v:parentLearningStandardReference         as parent_learning_standard_reference,
        v:successCriteria::string                 as success_criteria,
        v:namespace::string                       as namespace,
        v:uri::string                             as uri,
        -- edfi extensions
        v:_ext                                    as v_ext
    from learning_standards
)
select * from renamed
