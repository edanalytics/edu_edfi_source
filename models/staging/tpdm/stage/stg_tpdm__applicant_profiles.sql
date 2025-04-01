with base_applicant_profiles as (
    select * from {{ ref('base_tpdm__applicant_profiles') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
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
            order_by='last_modified_timestamp desc, pull_timestamp desc')
    }}
)
select * from deduped
where not is_deleted
