{% macro whybroken_show_baseline(model_name=none) %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}

  {% set query %}
    SELECT
      model_name,
      column_name,
      row_count,
      null_count,
      distinct_count,
      min_value,
      max_value,
      avg_value,
      captured_at
    FROM {{ wb_schema }}.whybroken_baseline
    {% if model_name is not none %}
    WHERE model_name = '{{ model_name }}'
    {% endif %}
    ORDER BY model_name, column_name
  {% endset %}

  {% set result = run_query(query) %}
  {{ return(result) }}

{% endmacro %}


{% macro whybroken_show_models() %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}

  {% set query %}
    SELECT
      model_name,
      row_count,
      column_count,
      captured_at
    FROM {{ wb_schema }}.whybroken_baseline
    WHERE column_name = '*'
    ORDER BY model_name
  {% endset %}

  {% set result = run_query(query) %}
  {{ return(result) }}

{% endmacro %}


{% macro whybroken_show_column_stats(model_name) %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}

  {% set query %}
    SELECT
      column_name,
      null_count,
      distinct_count,
      min_value,
      max_value,
      avg_value,
      captured_at
    FROM {{ wb_schema }}.whybroken_baseline
    WHERE model_name = '{{ model_name }}'
      AND column_name != '*'
    ORDER BY column_name
  {% endset %}

  {% set result = run_query(query) %}
  {{ return(result) }}

{% endmacro %}
