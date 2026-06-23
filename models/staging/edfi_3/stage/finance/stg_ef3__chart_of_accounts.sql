with base_chart_of_accounts as (
    select * from {{ ref('base_ef3__chart_of_accounts') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(chart_of_account_identifier)',
                'ed_org_id',
                'fiscal_year',
            ]
        ) }} as k_chart_of_account,
        {{ edorg_ref(annualize=False) }},
        {{ gen_skey('k_balance_sheet_dimension') }},
        {{ gen_skey('k_function_dimension') }},
        {{ gen_skey('k_fund_dimension') }},
        {{ gen_skey('k_object_dimension') }},
        {{ gen_skey('k_operational_unit_dimension') }},
        {{ gen_skey('k_program_dimension') }},
        {{ gen_skey('k_project_dimension') }},
        {{ gen_skey('k_source_dimension') }},
        *
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_chart_of_accounts
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_chart_of_account',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
