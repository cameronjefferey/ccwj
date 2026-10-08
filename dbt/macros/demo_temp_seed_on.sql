{#
    Whether the temporary demo book is added on top of the bot mirror.

    ``var('demo_temp_seed')`` is the string from the ``DEMO_TEMP_SEED``
    environment variable (see dbt_project.yml). A Jinja ``{% if %}`` on
    that string is wrong: the string "false" is truthy. Call this macro.
#}
{% macro demo_temp_seed_on() %}
    {%- set raw = var('demo_temp_seed', false) -%}
    {%- if raw == true -%}
        {{ return(true) }}
    {%- elif raw == false or raw is none -%}
        {{ return(false) }}
    {%- else -%}
        {{ return((raw | string | trim | lower) in ['1', 'true', 'yes', 'on']) }}
    {%- endif -%}
{% endmacro %}
