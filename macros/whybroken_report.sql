{# ------------------------------------------------------------------ #}
{# WhyBroken reporting macros                                         #}
{# Utility macros for inspecting WhyBroken results interactively      #}
{# Usage: dbt run-operation whybroken_show_anomalies --args '{limit: 50}' #}
{# ------------------------------------------------------------------ #}


{% macro whybroken_show_anomalies(limit=20) %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}

  {% set query %}
    SELECT
      run_id,
      model_name,
      anomaly_type,
      column_name,
      severity,
      description,
      current_value,
      previous_value,
      delta_pct,
      detected_at
    FROM {{ wb_schema }}.whybroken_anomalies
    ORDER BY detected_at DESC
    LIMIT {{ limit }}
  {% endset %}

  {% set result = run_query(query) %}
  {{ return(result) }}

{% endmacro %}


{% macro whybroken_show_runs(limit=10) %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}

  {% set query %}
    SELECT
      r.run_id,
      r.started_at,
      r.model_count,
      r.dbt_version,
      COALESCE(a.anomaly_count, 0) AS anomaly_count
    FROM {{ wb_schema }}.whybroken_runs r
    LEFT JOIN (
      SELECT run_id, COUNT(*) AS anomaly_count
      FROM {{ wb_schema }}.whybroken_anomalies
      GROUP BY run_id
    ) a ON r.run_id = a.run_id
    ORDER BY r.started_at DESC
    LIMIT {{ limit }}
  {% endset %}

  {% set result = run_query(query) %}
  {{ return(result) }}

{% endmacro %}


{% macro whybroken_show_model_history(model_name, limit=10) %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}

  {% set query %}
    SELECT
      s.run_id,
      s.model_name,
      s.row_count,
      s.column_count,
      s.captured_at
    FROM {{ wb_schema }}.whybroken_snapshots s
    WHERE s.model_name = '{{ model_name }}'
    ORDER BY s.captured_at DESC
    LIMIT {{ limit }}
  {% endset %}

  {% set result = run_query(query) %}
  {{ return(result) }}

{% endmacro %}


{% macro whybroken_show_column_drift(model_name, column_name, limit=10) %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}

  {% set query %}
    SELECT
      cs.run_id,
      cs.model_name,
      cs.column_name,
      cs.null_count,
      cs.distinct_count,
      cs.min_value,
      cs.max_value,
      cs.avg_value,
      cs.captured_at
    FROM {{ wb_schema }}.whybroken_column_stats cs
    WHERE cs.model_name = '{{ model_name }}'
      AND cs.column_name = '{{ column_name }}'
    ORDER BY cs.captured_at DESC
    LIMIT {{ limit }}
  {% endset %}

  {% set result = run_query(query) %}
  {{ return(result) }}

{% endmacro %}
