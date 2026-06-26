with base_local_accounts as (
    select * from {{ ref('base_ef3__local_accounts') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(local_account_identifier)',
                'ed_org_id',
                'fiscal_year',
            ]
        ) }} as k_local_account,
        {{ edorg_ref(annualize=False) }},
        {{ gen_skey('k_chart_of_account') }},
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_local_accounts
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_local_account',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
