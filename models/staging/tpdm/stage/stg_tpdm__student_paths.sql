with base_student_paths as (
    select * from {{ ref('base_tpdm__student_paths') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'lower(path_name)',
                'lower(student_unique_id)'
            ]
        ) }} as k_student_path,        
        {{ gen_skey('k_path') }},
        {{ gen_skey('k_student') }},
        base_student_paths.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_paths
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student_path',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted