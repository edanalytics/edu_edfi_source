with base_discipline_actions as (
    select * from {{ ref('base_ef3__discipline_actions') }}
    where not is_deleted
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_school', alt_ref='assignment_school_reference', alt_k_name='k_school__assignment') }},
        {{ gen_skey('k_school', alt_ref='responsibility_school_reference', alt_k_name='k_school__responsibility') }},
        api_year as school_year,
        base_discipline_actions.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_discipline_actions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='discipline_action_id, discipline_date, k_student',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
