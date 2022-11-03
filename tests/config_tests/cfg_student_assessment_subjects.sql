{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with int_stu_assessments_subjects as (
    select * from {{ ref('int_ef3__student_assessments__identify_subject') }}
),
distinct_subject_score_xwalk as ( 
    select
        distinct
            assessment_identifier,
            namespace,
            score_name
    from {{ ref('xwalk_student_assessment_subject') }}
),
flatten as (
    select distinct
        tenant_code,
        api_year,
        int_stu_assessments_subjects.assessment_identifier,
        int_stu_assessments_subjects.namespace,
        {{ extract_descriptor('value:assessmentReportingMethodDescriptor::string') }} as score_name,
        value:result::string as score_result,
        case
            when value:result::string is null
                then False
            else True
        end as is_existing_mapping
    from int_stu_assessments_subjects
        left join distinct_subject_score_xwalk
            on int_stu_assessments_subjects.assessment_identifier = distinct_subject_score_xwalk.assessment_identifier
            and int_stu_assessments_subjects.namespace = distinct_subject_score_xwalk.namespace
        , lateral flatten(input=>v_score_results, outer => True)
            where {{ extract_descriptor('value:assessmentReportingMethodDescriptor::string') }} = distinct_subject_score_xwalk.score_name
            and int_stu_assessments_subjects.academic_subject is null
)
select * from flatten