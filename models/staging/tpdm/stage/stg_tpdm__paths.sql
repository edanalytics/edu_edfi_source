with base_paths as (
    select * from {{ ref('base_tpdm__paths') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'lower(path_name)'
            ]
        ) }} as k_path,
        {{ gen_skey('k_ed_org') }},
        {{ gen_skey('k_graduation_plan') }},
        base_paths.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_paths
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_path',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted