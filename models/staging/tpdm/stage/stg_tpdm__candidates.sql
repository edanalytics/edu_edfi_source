with base_candidates as (
    select * from {{ ref('base_tpdm__candidates') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(candidate_id)']
        ) }} as k_candidate,
        {{ gen_skey('k_person') }},
        base_candidates.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_candidates
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_candidate',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped
