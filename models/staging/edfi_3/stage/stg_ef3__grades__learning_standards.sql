with stg_grades as (
    select * from {{ ref('stg_ef3__grades') }}
),
flattened as (
    select
        stg_grades.tenant_code,
        stg_grades.api_year,
        stg_grades.k_student,
        stg_grades.k_school,
        stg_grades.k_grading_period,
        stg_grades.k_course_section,
        stg_grades.grade_type,
        {{ gen_skey('k_learning_standard', alt_ref='v_lsg.value:learningStandardReference') }},
        v_lsg.value:learningStandardReference:learningStandardId::string as learning_standard_id,
        v_lsg.value:letterGradeEarned::string as learning_standard_letter_grade_earned,
        v_lsg.value:numericGradeEarned::string as learning_standard_numeric_grade_earned,
        {{ edu_edfi_source.extract_descriptor('v_lsg.value:performanceBaseConversionDescriptor::string') }} as performance_base_conversion_descriptor
    from stg_grades,
        lateral flatten(input=>v_learning_standard_grades) as v_lsg
)
select * from flattened
