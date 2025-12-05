with stage_candidates as (
    select * from {{ ref('stg_tpdm__candidates') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_candidate,
        k_candidate_xyear,
        {{ extract_descriptor('value:raceDescriptor::string') }} as race
    from stage_candidates
        {{ json_flatten('v_races') }}
)
select * from flattened
