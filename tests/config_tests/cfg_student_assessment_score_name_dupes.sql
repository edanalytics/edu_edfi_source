{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with score_results as (
    select * from {{ ref('stg_ef3__student_assessments__score_results') }}
),
performance_levels as (
    select
        tenant_code,
        api_year,
        k_student_assessment,
        k_assessment,
        assessment_identifier,
        namespace,
        -- normalize column names to stack with scores
        performance_level_name as score_name,
        performance_level_result as score_result
    from {{ ref('stg_ef3__student_assessments__performance_levels') }}
),
stack_results as (
    select * from score_results
    union all 
    select * from performance_levels
),
dupes as (
    select
        tenant_code,
        api_year,
        k_student_assessment,
        assessment_identifier,
        namespace,
        score_name,
        count(distinct score_result) as n_score_results,
        array_agg(score_result) as score_results_array
    from stack_results
    group by 1,2,3,4,5,6
    having n_score_results > 1
)
select * from dupes