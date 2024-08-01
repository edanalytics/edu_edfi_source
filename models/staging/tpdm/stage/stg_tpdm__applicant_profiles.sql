with base_applicant_profiles as (
    select * from {{ ref('base_ef3__applicant_profiles') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(applicant_profile_id)']
        ) }} as k_applicant_profile,
        base_applicant_profiles.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_applicant_profiles
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_applicant_profile',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped
