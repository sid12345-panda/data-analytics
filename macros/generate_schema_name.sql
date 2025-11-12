{% macro generate_schema_name(custom_schema_name, node) -%}
  {# Use provided schema if explicitly set #}
  {% if custom_schema_name is not none %}
      {{ return(custom_schema_name) }}
  {% else %}
      {# Fallback to the target.schema (from your active dbt connection) #}
      {{ return(target.schema) }}
  {% endif %}
{%- endmacro %}