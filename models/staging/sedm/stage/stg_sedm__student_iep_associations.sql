with base_student_iep_associations as (
    select * from {{ ref('base_sedm__student_iep_associations') }}
),

keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(student_iep_association_id)',
                'iep_servicing_ed_org_id',
                'iep_finalized_date',
                'lower(student_unique_id)'
            ]
        ) }} as k_student_iep_association,
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ edorg_ref(annualize=False) }},
        api_year as school_year,
        base_student_iep_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}

    from base_student_iep_associations
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation='keyed',
        partition_by='k_student_iep_association',
        order_by='last_modified_timestamp desc, pull_timestamp desc'
    ) }}
)

select * from deduped
where not is_deleted
