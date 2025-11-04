with base_student_iep_accommodations as (
    select * from {{ ref('base_sedm__student_iep_accommodations') }}
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
        ) }} as k_student_iep_accommodation_collection,
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(student_iep_association_id)',
                'iep_servicing_ed_org_id',
                'cast(studentIEPReference:iepFinalizedDate as string)',
                'lower(student_unique_id)'
            ]
        ) }} as k_student_iep_association,
        api_year as school_year,
        base_student_iep_accommodations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}

    from base_student_iep_accommodations
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation='keyed',
        partition_by='k_student_iep_accommodation_collection',
        order_by='last_modified_timestamp desc, pull_timestamp desc'
    ) }}
)

select * from deduped
where not is_deleted
