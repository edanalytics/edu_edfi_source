with base_tpdm__candidate_relationship_to_staff_associations as (
    select * from {{ ref('base_tpdm__candidate_relationship_to_staff_associations') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(candidate_id)',
            'lower(staff_unique_id)']
        ) }} as k_candidate_relationship_to_staff_associations,
        {{ gen_skey('k_candidate') }},
        {{ gen_skey('k_staff') }},
        base_tpdm__candidate_relationship_to_staff_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_tpdm__candidate_relationship_to_staff_associations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_candidate_relationship_to_staff_associations',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped
