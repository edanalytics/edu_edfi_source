-- this model deduplicates stg_ef3__descriptors across tenants & years, to simplify the code needed in edu_edfi_source.extract_descriptor
    -- to replace descriptor code_values with short or long descriptions
-- in the future, we could allow for more sophisticated order by logic, to not just select rows based on alphabetical tenant order
with stg_descriptors as (
    select * from {{ ref('stg_ef3__descriptors') }}
),

-- note, order by tenant and api to ensure consistency over time (although it will change as new tenants are added)
-- todo: better way to configure the order by?
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='stg_descriptors',
            partition_by='namespace, code_value',
            order_by='tenant_code, api_year desc'
        )
    }}
),

subset as (
  select
    namespace,
    code_value,
    description,
    short_description
  from deduped
)

select * from subset