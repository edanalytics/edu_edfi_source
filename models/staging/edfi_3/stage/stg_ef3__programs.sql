with base_programs as (
    select * from {{ ref('base_ef3__programs') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'ed_org_id',
                'lower(program_name)',
                'lower(program_type)'
            ]
        )}} as k_program,
        {{ edorg_ref(annualize=False) }},
        api_year as school_year,
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_programs
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_program',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped