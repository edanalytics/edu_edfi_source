with grades as (
    {{ source_edfi3('grades') }}
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
        ods_version,
        data_model_version,
        -- section key
        v:studentSectionAssociationReference:beginDate::date         as section_begin_date,
        v:studentSectionAssociationReference:localCourseCode::string as local_course_code,
        v:studentSectionAssociationReference:schoolId::integer       as school_id,
        v:studentSectionAssociationReference:schoolYear::integer     as school_year,
        v:studentSectionAssociationReference:sectionId::string       as section_id,
        v:studentSectionAssociationReference:sessionName::string     as session_name,
        v:studentSectionAssociationReference:studentUniqueId::string as student_unique_id,
        --grading period key
		v:gradingPeriodReference:schoolId::integer       as grading_period_school_id,
		v:gradingPeriodReference:schoolYear::integer     as grading_period_school_year,
        v:gradingPeriodReference:periodSequence::integer as period_sequence,
        {{ extract_descriptor('v:gradingPeriodReference:gradingPeriodDescriptor::string') }} as grading_period_descriptor,
        -- grade data
        v:letterGradeEarned::string   as letter_grade_earned,
        v:numericGradeEarned::float   as numeric_grade_earned,
        v:diagnosticStatement::string as diagnostic_statement,
        {{ extract_descriptor('v:gradeTypeDescriptor::string') }} as grade_type,
        {{ extract_descriptor('v:performanceBaseConversionDescriptor::string') }} as performance_base_conversion,
        -- embedded lists
        v:learningStandardGrades as v_learning_standard_grades,
        -- references
        v:studentSectionAssociationReference as student_section_association_reference,
        v:gradingPeriodReference             as grading_period_reference,

        -- edfi extensions
        v:_ext as v_ext
    from grades
)
select * from renamed
