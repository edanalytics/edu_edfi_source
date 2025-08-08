with staff_educator_preparation_program_associations as (
    select * from {{ ref('base_tpdm__staff_educator_preparation_program_associations') }}
),
keyed as (
    select
        {{ gen_skey('k_staff') }},
        {{ gen_skey('k_educator_prep_program') }},
        staff_educator_preparation_program_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from staff_educator_preparation_program_associations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_staff, k_educator_prep_program',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
