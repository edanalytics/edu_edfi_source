with stage_obj_assessments as (
    select * from {{ ref('stg_ef3__objective_assessments') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_objective_assessment,
        {{ extract_descriptor('value:assessmentReportingMethodDescriptor::string') }} as performance_level_name,
        {{ extract_descriptor('value:performanceLevelDescriptor::string') }} as performance_level_value,
        {{ extract_descriptor('value:resultDatatypeTypeDescriptor::string') }} as performance_level_data_type
    from stage_obj_assessments,
        lateral flatten(input=>v_performance_levels)
)
select * from flattened