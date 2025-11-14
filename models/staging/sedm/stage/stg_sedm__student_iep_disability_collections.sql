with base_student_iep_disability_collections as (
    select * from {{ ref('base_sedm__student_iep_disability_collections') }}
),

keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(student_iep_association_id)',
                'lower(student_unique_id)',
                'iep_servicing_ed_org_id'
            ]
        ) }} as k_student_iep_disability_collection,
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_student_iep') }},
        api_year as school_year,
        base_student_iep_disability_collections.*
        {{ extract_extension(model_name=this.name, flatten=True) }}

    from base_student_iep_disability_collections
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation='keyed',
        partition_by='k_student_iep_disability_collection',
        order_by='last_modified_timestamp desc, pull_timestamp desc'
    ) }}
)

select * from deduped
where not is_deleted
