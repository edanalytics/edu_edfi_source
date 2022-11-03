with stg_academic_records as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student_academic_record,
        {{ extract_descriptor('value:gradePointAverageTypeDescriptor::string') }} as gpa_type,
        value:gradePointAverageValue::float as gpa_value,
        value:isCumulative::boolean as is_cumulative,
        value:maxGradePointAverageValue::float  as max_gpa_value
    from stg_academic_records
        , lateral flatten(input=>v_grade_point_averages)
)
select * from flattened
