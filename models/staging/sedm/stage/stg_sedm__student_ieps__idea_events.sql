with stg_student_ieps as (
    select * from {{ ref('stg_sedm__student_ieps') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_student_iep,
        k_student,
        k_student_xyear,
        student_iep_association_id,
        iep_finalized_date,
        iep_begin_date,
        iep_end_date,
        iep_amended_date,
        {{ gen_skey('k_idea_event', alt_ref='value:ideaEventReference') }},
        value:ideaEventReference:ideaEventID as idea_event_id
    from stg_student_ieps
        {{ json_flatten('v_idea_events') }}
)
select * from flattened