with base_program_evaluations as (
  select * from {{ ref("base_ef3__program_evaluations") }}
),

keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(program_ed_org_id)',
                'lower(program_evaluation_period)',
                'lower(program_evaluation_title)',
                'lower(program_evaluation_type)',
                'lower(program_name)',
                'lower(program_type)'
            ]) }} as k_program_evaluation,
        {{ gen_skey("k_program") }},
        {#
           programEvaluation has an ed-org id via $.programReference,
           but does not contain an educationOrganizationReference
        #}
        {# {{ edorg_ref(annualize=False) }}, #}

        base_program_evaluations.*,
        {{ extract_extension(model_name=this.name, flatten=True) }}

    from base_program_evaluations
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation="keyed",
        partition_by="k_program_evaluation",
        order_by="last_modified_timestamp desc, pull_timestamp desc"
    ) }}
)

select * from deduped
where not is_deleted