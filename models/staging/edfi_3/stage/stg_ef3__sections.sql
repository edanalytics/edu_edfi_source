with base_sections as (
    select * from {{ ref('base_ef3__sections') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(local_course_code)',
                'school_id',
                'school_year',
                'lower(section_id)',
                'lower(session_name)'
            ]
        ) }} as k_course_section,
        {{ gen_skey('k_course_offering') }},
        -- pull k_school from the course offering definition.
        -- this is the school officially offering the course
        {{ gen_skey('k_school', alt_ref='course_offering_reference') }},
        {{ gen_skey('k_location') }},
        -- pull a separate k_school from location school
        -- this is the physical location where the class is taught, 
        -- which could theoretically be different from the school offering the section
        {{ gen_skey('k_school', alt_ref='location_school_reference', alt_k_name='k_school__location') }},
        base_sections.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_sections
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_course_section',
            order_by='pull_timestamp desc'
        )
    }}

)
select * from deduped