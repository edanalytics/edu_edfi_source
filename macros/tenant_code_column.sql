{% macro tenant_code_col() %}

    {% set tenant_code_col = 'tenant_code' %}
    {% if var('edu:partner_code_prefix') %}
        {% set tenant_code_col = "concat('" + var('edu:partner_code_prefix') + "',tenant_code)" %}
    {% endif %}

    {{ return(tenant_code_col) }}

{% endmacro %}