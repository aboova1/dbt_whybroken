{% macro whybroken_detect_anomalies_batch(run_id) %}

  {% set row_count_threshold = var('whybroken_row_count_threshold', 50) %}
  {% set row_count_critical = var('whybroken_row_count_critical', 100) %}
  {% set avg_value_threshold = var('whybroken_avg_value_threshold', 30) %}
  {% set avg_value_critical = var('whybroken_avg_value_critical', 80) %}
  {% set avg_value_high = var('whybroken_avg_value_high', 50) %}
  {% set null_spike_threshold = var('whybroken_null_spike_threshold', 10) %}
  {% set fail_on_critical = var('whybroken_fail_on_critical', false) %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}
  {% set ts = whybroken.whybroken_current_timestamp() %}

  {% set row_anomaly_query %}
    INSERT INTO {{ wb_schema }}.whybroken_anomalies
    SELECT
      c.run_id,
      c.model_name,
      'row_count_change',
      '*',
      CASE
        WHEN ABS(((c.row_count - p.row_count) * 100.0 / NULLIF(p.row_count, 0))) > {{ row_count_critical }} THEN 'critical'
        ELSE 'high'
      END,
      'Row count changed by ' || CAST(ROUND((c.row_count - p.row_count) * 100.0 / NULLIF(p.row_count, 0), 1) AS {{ whybroken.whybroken_type_string() }}) || '% (' || CAST(p.row_count AS {{ whybroken.whybroken_type_string() }}) || ' -> ' || CAST(c.row_count AS {{ whybroken.whybroken_type_string() }}) || ')',
      CAST(c.row_count AS {{ whybroken.whybroken_type_string() }}),
      CAST(p.row_count AS {{ whybroken.whybroken_type_string() }}),
      (c.row_count - p.row_count) * 100.0 / NULLIF(p.row_count, 0),
      {{ ts }}
    FROM {{ wb_schema }}.whybroken_snapshots c
    INNER JOIN (
      SELECT model_name, MAX(captured_at) AS max_ts
      FROM {{ wb_schema }}.whybroken_snapshots
      WHERE run_id != '{{ run_id }}'
      GROUP BY model_name
    ) prev_max ON c.model_name = prev_max.model_name
    INNER JOIN {{ wb_schema }}.whybroken_snapshots p
      ON p.model_name = prev_max.model_name
      AND p.captured_at = prev_max.max_ts
      AND p.run_id != '{{ run_id }}'
    WHERE c.run_id = '{{ run_id }}'
      AND p.row_count > 0
      AND ABS((c.row_count - p.row_count) * 100.0 / p.row_count) > {{ row_count_threshold }}
  {% endset %}
  {% do run_query(row_anomaly_query) %}

  {% set col_anomaly_query %}
    INSERT INTO {{ wb_schema }}.whybroken_anomalies
    SELECT
      c.run_id,
      c.model_name,
      CASE
        WHEN ABS((c.avg_value - p.avg_value) * 100.0 / NULLIF(ABS(p.avg_value), 0)) > {{ avg_value_threshold }} THEN 'avg_value_change'
        ELSE 'null_spike'
      END,
      c.column_name,
      CASE
        WHEN ABS((c.avg_value - p.avg_value) * 100.0 / NULLIF(ABS(p.avg_value), 0)) > {{ avg_value_critical }} THEN 'critical'
        WHEN ABS((c.avg_value - p.avg_value) * 100.0 / NULLIF(ABS(p.avg_value), 0)) > {{ avg_value_high }} THEN 'high'
        WHEN ABS((c.avg_value - p.avg_value) * 100.0 / NULLIF(ABS(p.avg_value), 0)) > {{ avg_value_threshold }} THEN 'medium'
        ELSE 'high'
      END,
      CASE
        WHEN ABS((c.avg_value - p.avg_value) * 100.0 / NULLIF(ABS(p.avg_value), 0)) > {{ avg_value_threshold }}
          THEN 'Average of ' || c.column_name || ' changed by ' || CAST(ROUND((c.avg_value - p.avg_value) * 100.0 / NULLIF(ABS(p.avg_value), 0), 1) AS {{ whybroken.whybroken_type_string() }}) || '%'
        ELSE 'Null count for ' || c.column_name || ' increased from ' || CAST(p.null_count AS {{ whybroken.whybroken_type_string() }}) || ' to ' || CAST(c.null_count AS {{ whybroken.whybroken_type_string() }})
      END,
      COALESCE(CAST(c.avg_value AS {{ whybroken.whybroken_type_string() }}), CAST(c.null_count AS {{ whybroken.whybroken_type_string() }})),
      COALESCE(CAST(p.avg_value AS {{ whybroken.whybroken_type_string() }}), CAST(p.null_count AS {{ whybroken.whybroken_type_string() }})),
      COALESCE((c.avg_value - p.avg_value) * 100.0 / NULLIF(ABS(p.avg_value), 0), (c.null_count - p.null_count) * 100.0 / NULLIF(p.null_count, 0)),
      {{ ts }}
    FROM {{ wb_schema }}.whybroken_column_stats c
    INNER JOIN (
      SELECT model_name, column_name, MAX(captured_at) AS max_ts
      FROM {{ wb_schema }}.whybroken_column_stats
      WHERE run_id != '{{ run_id }}'
      GROUP BY model_name, column_name
    ) prev_max ON c.model_name = prev_max.model_name AND c.column_name = prev_max.column_name
    INNER JOIN {{ wb_schema }}.whybroken_column_stats p
      ON p.model_name = prev_max.model_name
      AND p.column_name = prev_max.column_name
      AND p.captured_at = prev_max.max_ts
      AND p.run_id != '{{ run_id }}'
    WHERE c.run_id = '{{ run_id }}'
      AND (
        (c.avg_value IS NOT NULL AND p.avg_value IS NOT NULL AND p.avg_value != 0
          AND ABS((c.avg_value - p.avg_value) * 100.0 / ABS(p.avg_value)) > {{ avg_value_threshold }})
        OR
        (c.null_count > p.null_count AND (c.null_count - p.null_count) > {{ null_spike_threshold }})
      )
  {% endset %}
  {% do run_query(col_anomaly_query) %}

  {% if fail_on_critical %}
    {% set critical_check %}
      SELECT COUNT(*) AS cnt FROM {{ wb_schema }}.whybroken_anomalies
      WHERE run_id = '{{ run_id }}' AND severity = 'critical'
    {% endset %}
    {% set critical_results = run_query(critical_check) %}
    {% if critical_results and critical_results.rows[0][0] > 0 %}
      {{ exceptions.raise_compiler_error("WhyBroken: Critical anomalies detected! Run whybroken.whybroken_show_anomalies() for details.") }}
    {% endif %}
  {% endif %}

{% endmacro %}


{% macro whybroken_detect_anomalies(run_id, model_name) %}
{% endmacro %}
