with base_student_path_milestone_statuses as (
    select * from {{ ref('base_tpdm__student_path_milestone_statuses') }}
),
keyed as (
    select
        {{ gen_skey('k_student_path') }},
        {{ gen_skey('k_path_milestone') }},
        base_student_path_milestone_statuses.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_path_milestone_statuses
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student_path, k_path_milestone',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted