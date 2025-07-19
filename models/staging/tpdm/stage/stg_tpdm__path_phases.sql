with base_path_phases as (
    select * from {{ ref('base_tpdm__path_phases') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'lower(path_name)',
                'lower(path_phase_name)'
            ]
        ) }} as k_path_phase,
        {{ gen_skey('k_path') }},
        base_path_phases.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_path_phases
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_path_phase',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted