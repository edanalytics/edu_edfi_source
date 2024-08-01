with post_secondary_institutions as (
    select * from {{ ref('base_tpdm__post_secondary_institutions') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.surrogate_key(
            ['tenant_code',
            'api_year',
            'post_secondary_institution_id']
        ) }} as k_post_secondary_institution,
        post_secondary_institutions.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from post_secondary_institutions
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_post_secondary_institution',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped
