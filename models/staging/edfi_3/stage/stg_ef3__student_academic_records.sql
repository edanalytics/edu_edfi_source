{{ config(
    materialized='incremental',
    unique_key=['k_student_academic_record'],
    post_hook=["{{edu_edfi_source.stg_post_hook_delete()}}"]
) }}
with base_academic_records as (
    select * from {{ ref('base_ef3__student_academic_records') }}

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
                'ed_org_id',
                'school_year',
                'lower(student_unique_id)',
                'lower(academic_term)'
            ]
        ) }} as k_student_academic_record,
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ edorg_ref(annualize=False) }},
        base_academic_records.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_academic_records
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student_academic_record',
            order_by='api_year desc, last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
{% if not is_incremental() %}
where not is_deleted
{% endif %}

