{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}


{% macro safe_divide(numerator, denominator, default=0) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null
        then {{ default }}
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}


{% macro pct_of_total(value_col, partition_cols) %}
    round(
        ({{ value_col }} / nullif(sum({{ value_col }}) over (partition by {{ partition_cols }}), 0) * 100)::numeric,
        2
    )
{% endmacro %}


{% macro yoy_change_pct(value_col, partition_cols, order_col) %}
    round(
        (
            {{ value_col }}
            - lag({{ value_col }}, 12) over (partition by {{ partition_cols }} order by {{ order_col }})
        ) / nullif(
            lag({{ value_col }}, 12) over (partition by {{ partition_cols }} order by {{ order_col }}),
            0
        ) * 100,
    2)
{% endmacro %}
