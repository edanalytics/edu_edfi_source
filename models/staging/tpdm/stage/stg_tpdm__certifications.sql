with certifications as (
    select * from {{ ref('base_tpdm__certifications') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(certification_id)',
            'lower(namespace)']
        ) }} as k_certification,
        certifications.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from certifications
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_certification',
            order_by='last_modified_timestamp desc')
    }}
)
select * from deduped
where not is_deleted
