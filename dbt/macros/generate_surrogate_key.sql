{% macro generate_surrogate_key(field_list) %}

    {% if not field_list %}
        {{ exceptions.raise_compiler_error("generate_surrogate_key requires at least one field") }}
    {% endif %}

    {%- set fields = [] -%}
    {%- for field in field_list -%}
        {%- set _ = fields.append(
            "coalesce(cast(" ~ field ~ " as varchar), '')"
        ) -%}
    {%- endfor -%}

    md5(
        {{ fields | join(" || '-' || ") }}
    )

{% endmacro %}


{% macro create_indexes(model) %}
    {% if model.config.materialized == 'table' %}
        {% set index_name = model.name ~ '_loaded_at_idx' %}
        create index if not exists {{ index_name }}
        on {{ model }} (loaded_at);
    {% endif %}
{% endmacro %}
