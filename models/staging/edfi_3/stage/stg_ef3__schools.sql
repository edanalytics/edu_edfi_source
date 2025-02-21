{{ config(
    materialized='incremental',
    unique_key='k_school',
    post_hook=["{{edu_edfi_source.stg_post_hook_delete()}}"]
) }}
with base_schools as (
    select * from {{ ref('base_ef3__schools') }}

    {% if is_incremental() %}
    -- Only get newly added or deleted records since the last run
    where last_modified_timestamp > (select max(last_modified_timestamp) from {{ this }})
    {% endif %}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'school_id'
            ]
        )}} as k_school,
        {{ gen_skey('k_lea') }},
        base_schools.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_schools
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_school',
            order_by='api_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
{# for incremental, keep deletes to be used in the MERGE and then dropped in the post_hook #}
{% if not is_incremental() %}
where not is_deleted
{% endif %}