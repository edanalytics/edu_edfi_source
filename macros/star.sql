{#
Cross-platform implementation of Snowflake's EXCLUDE and RENAME functionality

Arguments:
    from: The table to select from
    except: A list of columns to exclude
    rename: A paired list of columns to rename ([old_name, new_name])

Notes:
    This macro is more complex than it otherwise would be because Databricks does
    not support RENAME in select statements.
#}

{% macro star(from, except=[], rename=[]) %}
    {{ return(adapter.dispatch('star', 'edu_edfi_source')(from, except, rename)) }}
{% endmacro %}


{% macro snowflake__star(from, except, rename) -%}

    select * 
    {% if except | length > 0 -%}
    exclude ({{ except | join(',') }})
    {% endif %}
    {% if rename | length > 0 -%} 
    rename (
    {% for col_pair in rename -%}
        {{ col_pair[0] | trim }} as {{ col_pair[1] | trim }} {% if not loop.last %},{% else %}){% endif %}
    {% endfor -%}
    {% endif %}
    from {{from}}

{%- endmacro %}


{% macro databricks__star(from, except, rename) -%}

    select
    {% for col_pair in rename -%}
    {{ col_pair[0] | trim }} as {{ col_pair[1] | trim }},
    {{ except.append(col_pair[0] | trim) or "" }}
    {% endfor -%}
    *
    {% if except | length > 0 -%} 
    except ({{ except | join(', ') }})
    {% endif -%}
    from {{from}}

{%- endmacro %}