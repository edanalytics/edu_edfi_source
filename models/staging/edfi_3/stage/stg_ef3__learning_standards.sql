with base_learning_standards as (
    select * from {{ ref('base_ef3__learning_standards') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(learning_standard_id)']
        ) }} as k_learning_standard,
        {{ gen_skey('k_learning_standard', 
                    alt_ref='parent_learning_standard_reference', 
                    alt_k_name='k_learning_standard__parent') }},
        base_learning_standards.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_learning_standards
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_learning_standard',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped