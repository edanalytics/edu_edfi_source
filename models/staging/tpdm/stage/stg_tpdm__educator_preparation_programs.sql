with base_epp as (
    select * from {{ ref('base_tpdm__educator_preparation_programs') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(ed_org_id)',
                'lower(program_name)',
                'lower(program_type)'
            ]
        ) }} as k_educator_prep_program,
        {{ edorg_ref(annualize=False) }},
        base_epp.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_epp
)
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_educator_prep_program',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
