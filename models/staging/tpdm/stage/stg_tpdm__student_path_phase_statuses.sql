with base_student_path_phase_statuses as (
    select * from {{ ref('base_tpdm__student_path_phase_statuses') }}
),
keyed as (
    select
        {{ gen_skey('k_path_phase') }},
        {{ gen_skey('k_student_path') }},
        base_student_path_phase_statuses.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_path_phase_statuses
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_path_phase, k_student_path',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted