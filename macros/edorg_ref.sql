-- is this useful?
-- maybe for creating standalone keys, but not for
-- embedding the resolved key into another
{% macro edorg_ref(annualize=False) -%}
  {% set static_cols = ['tenant_code'] %}
  {% if annualize %}
    {% do static_cols.append('api_year')%}}
  {% endif %}
    case 
        when education_organization_reference:link:rel::string  = 'School'
        then null
        when education_organization_reference:link:rel::string  = 'LocalEducationAgency'
        then {{dbt_utils.generate_surrogate_key(static_cols + ['education_organization_reference:educationOrganizationId'])}}
    end as k_lea,
    case 
        when education_organization_reference:link:rel::string  = 'School'
        then {{dbt_utils.generate_surrogate_key(static_cols + ['education_organization_reference:educationOrganizationId'])}}
        when education_organization_reference:link:rel::string  = 'LocalEducationAgency'
        then null
    end as k_school
{%- endmacro %}