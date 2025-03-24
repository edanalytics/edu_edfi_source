with base_financial_aids as (
    select * from {{ ref('base_tpdm__financial_aids') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(aid_type)',
                'begin_date',
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
            order_by='last_modified_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
