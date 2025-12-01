with base_candidates as (
    select * from {{ ref('base_tpdm__candidates') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(candidate_id)'
            ]
        ) }} as k_candidate,
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(candidate_id)'
            ]
        ) }} as k_candidate_xyear,
        {{ gen_skey('k_person') }},
        base_candidates.api_year as school_year,
        base_candidates.*,
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_candidates
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_candidate',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
