with candidate_educator_preparation_program_associations as (
    select * from {{ ref('base_tpdm__candidate_educator_preparation_program_associations') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'begin_date',
                'lower(candidate_id)',
                'ed_org_id',
                'lower(program_name)',
                'lower(program_type)'
            ]
        ) }} as k_candidate_educator_preparation_program_association,
        {{ gen_skey('k_candidate') }},
        {{ gen_skey('k_educator_preparation_program') }},
        candidate_educator_preparation_program_associations.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from candidate_educator_preparation_program_associations
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_candidate_educator_preparation_program_association',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
