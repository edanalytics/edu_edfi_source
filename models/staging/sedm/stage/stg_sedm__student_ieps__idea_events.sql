with stg_student_ieps as (
    select * from {{ ref('stg_sedm__student_ieps') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_student_iep,
        {{ gen_skey('k_idea_event', alt_ref='value:ideaEventReference') }},
        value:ideaEventReference:ideaEventID as idea_event_id,
        value:ideaEventReference:studentUniqueId as student_unique_id,
        value:ideaEventReference:educationOrganizationId as ed_org_id,
        {{ extract_descriptor('value:ideaEventReference:ideaEventDescriptor') }} as idea_event
    from stg_student_ieps
        {{ json_flatten('v_idea_events') }}
)
select * from flattened