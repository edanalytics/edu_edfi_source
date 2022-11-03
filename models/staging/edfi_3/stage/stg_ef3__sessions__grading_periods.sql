with stg_sessions as (
    select * from {{ ref('stg_ef3__sessions') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_session,
        {{ gen_skey('k_grading_period', alt_ref='value:gradingPeriodReference') }}
    from stg_sessions
        , lateral flatten(input=>v_grading_periods)
)
select * from flattened