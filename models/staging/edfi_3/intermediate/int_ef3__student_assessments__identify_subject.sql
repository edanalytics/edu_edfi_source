with base_stu_assessments as (
    select * from {{ ref('base_ef3__student_assessments') }}
    {% if is_incremental() %}
    -- Only get new or updated records since the last run
    where last_modified_timestamp > (select max(pull_timestamp) from {{ this }})
    {% endif %}
),
stg_assessments_single_subj as (
    select
        tenant_code,
        api_year,
        assessment_identifier,
        namespace,
        academic_subject
    from {{ ref('stg_ef3__assessments') }}
    -- the reason to do this is if we missed a multi subject assessment identifier in the subject xwalk,
    -- we would be assigning all subject rows to the student assessment record, which would be very wrong
    where is_single_subject_identifier
),
-- this will be implementation specific
subject_xwalk as (
    select * from {{ ref('xwalk_student_assessment_subject') }}
),
distinct_score_name as (
    select
        distinct
            assessment_identifier,
            namespace,
            score_name
    from subject_xwalk
),
score_result_to_subject as (
    select 
        base_stu_assessments.tenant_code,
        base_stu_assessments.api_year,
        base_stu_assessments.pull_timestamp,
        base_stu_assessments.file_row_number,
        base_stu_assessments.assessment_identifier,
        base_stu_assessments.namespace,
        student_assessment_identifier,
        value:result::string as score_result
    from base_stu_assessments
    join distinct_score_name
            on base_stu_assessments.assessment_identifier = distinct_score_name.assessment_identifier
            and base_stu_assessments.namespace = distinct_score_name.namespace
        , lateral flatten(input=>v_score_results)
            where {{ extract_descriptor('value:assessmentReportingMethodDescriptor::string') }} = score_name
),
adding_subject as (
    select
        base_stu_assessments.*,
        coalesce(subject_xwalk.academic_subject, stg_assessments_single_subj.academic_subject) as academic_subject
    from base_stu_assessments
    left join score_result_to_subject
        on base_stu_assessments.assessment_identifier = score_result_to_subject.assessment_identifier
        and base_stu_assessments.namespace = score_result_to_subject.namespace
        and base_stu_assessments.student_assessment_identifier = score_result_to_subject.student_assessment_identifier
        and base_stu_assessments.tenant_code = score_result_to_subject.tenant_code
        and base_stu_assessments.api_year = score_result_to_subject.api_year
        and base_stu_assessments.pull_timestamp = score_result_to_subject.pull_timestamp
        and base_stu_assessments.file_row_number = score_result_to_subject.file_row_number
    left join subject_xwalk
        on score_result_to_subject.assessment_identifier = subject_xwalk.assessment_identifier
        and score_result_to_subject.namespace = subject_xwalk.namespace
        and score_result_to_subject.score_result = subject_xwalk.score_result
    left join stg_assessments_single_subj
        on base_stu_assessments.assessment_identifier = stg_assessments_single_subj.assessment_identifier
        and base_stu_assessments.namespace = stg_assessments_single_subj.namespace
        and base_stu_assessments.tenant_code = stg_assessments_single_subj.tenant_code
        and base_stu_assessments.api_year = stg_assessments_single_subj.api_year
)
select * from adding_subject
