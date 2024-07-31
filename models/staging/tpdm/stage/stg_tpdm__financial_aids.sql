with base_financial_aids as (
    select * from {{ ref('base_tpdm__financial_aids') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(aid_type)',
                'beginDate',
                'lower(student_unique_id)'
            ]
        ) }} as k_financial_aid,
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        base_financial_aids.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_financial_aids
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_financial_aid',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
