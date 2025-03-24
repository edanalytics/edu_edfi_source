with candidate_relationship_to_staff_associations as (
    select * from {{ ref('base_tpdm__candidate_relationship_to_staff_associations') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(candidate_id)',
            'lower(staff_unique_id)']
        ) }} as k_candidate_relationship_to_staff_associations,
        {{ gen_skey('k_candidate') }},
        {{ gen_skey('k_staff') }},
        candidate_relationship_to_staff_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from candidate_relationship_to_staff_associations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_candidate_relationship_to_staff_associations',
            order_by='last_modified_timestamp desc')
    }}
)
select * from deduped
where not is_deleted
