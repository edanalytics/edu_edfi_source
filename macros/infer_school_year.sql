{#
Infer school_year from entry_date and exit_withdraw_date when both dates are in the same school year. School years run from July 1 to June 30.

Arguments:
    school_year_col: The name of the school_year column (default: 'school_year')
    entry_date_col: The name of the entry_date column (default: 'entry_date')
    exit_date_col: The name of the exit_withdraw_date column (default: 'exit_withdraw_date')

Returns:
    A coalesce expression that:
    - Uses the original school_year if it's not null
    - Infers school_year from dates if both entry_date and exit_withdraw_date are not null and in the same school year
    - Returns null otherwise

Example:
    select 
        {{ edu_edfi_source.infer_school_year() }} as school_year
    from my_table
#}

{% macro _school_year_from_date(date_col) -%}
    case when month({{ date_col }}) >= 7 then year({{ date_col }}) + 1 else year({{ date_col }}) end
{%- endmacro %}

{% macro infer_school_year(school_year_col='school_year', entry_date_col='entry_date', exit_date_col='exit_withdraw_date') %}
    coalesce(
        {{ school_year_col }},
        case 
            when {{ entry_date_col }} is not null 
                and {{ exit_date_col }} is not null
                and {{ edu_edfi_source._school_year_from_date(entry_date_col) }} = {{ edu_edfi_source._school_year_from_date(exit_date_col) }}
            then {{ edu_edfi_source._school_year_from_date(entry_date_col) }}
            else null
        end
    )
{%- endmacro %}
