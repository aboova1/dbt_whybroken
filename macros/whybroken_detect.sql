{% macro whybroken_detect_anomalies(run_id, model_name) %}

  {% set wb_schema = whybroken.whybroken_fq_schema() %}

  {% set prev_query %}
    SELECT run_id FROM {{ wb_schema }}.whybroken_snapshots
    WHERE model_name = '{{ model_name }}'
      AND run_id != '{{ run_id }}'
    ORDER BY captured_at DESC
    LIMIT 1
  {% endset %}
  {% set prev_result = run_query(prev_query) %}

  {% if prev_result.rows | length == 0 %}
    {{ return('') }}
  {% endif %}

  {% set prev_run_id = prev_result.rows[0][0] %}

  {# --- Row count anomaly --- #}
  {% set row_check %}
    SELECT
      c.row_count AS current_rows,
      p.row_count AS previous_rows,
      CASE WHEN p.row_count > 0
        THEN ((c.row_count - p.row_count) * 100.0 / p.row_count)
        ELSE 0
      END AS delta_pct
    FROM {{ wb_schema }}.whybroken_snapshots c
    JOIN {{ wb_schema }}.whybroken_snapshots p
      ON c.model_name = p.model_name
    WHERE c.run_id = '{{ run_id }}'
      AND c.model_name = '{{ model_name }}'
      AND p.run_id = '{{ prev_run_id }}'
  {% endset %}
  {% set row_result = run_query(row_check) %}

  {% if row_result.rows | length > 0 %}
    {% set rrow = row_result.rows[0] %}
    {% set delta = rrow[2] | float %}
    {% if delta > 50 or delta < -50 %}
      {% set severity = 'critical' if (delta > 100 or delta < -80) else 'high' %}
      {% set insert_anomaly %}
        INSERT INTO {{ wb_schema }}.whybroken_anomalies
        VALUES (
          '{{ run_id }}',
          '{{ model_name }}',
          'row_count_change',
          '*',
          '{{ severity }}',
          'Row count changed by {{ delta | round(1) }}% ({{ rrow[1] }} -> {{ rrow[0] }})',
          '{{ rrow[0] }}',
          '{{ rrow[1] }}',
          {{ delta }},
          {{ whybroken.whybroken_current_timestamp() }}
        )
      {% endset %}
      {% do run_query(insert_anomaly) %}
    {% endif %}
  {% endif %}

  {# --- Column stat anomalies --- #}
  {% set stat_check %}
    SELECT
      c.column_name,
      c.avg_value AS current_avg,
      p.avg_value AS previous_avg,
      CASE WHEN p.avg_value != 0 AND p.avg_value IS NOT NULL
        THEN ((c.avg_value - p.avg_value) * 100.0 / ABS(p.avg_value))
        ELSE 0
      END AS delta_pct,
      c.null_count AS current_nulls,
      p.null_count AS previous_nulls
    FROM {{ wb_schema }}.whybroken_column_stats c
    JOIN {{ wb_schema }}.whybroken_column_stats p
      ON c.model_name = p.model_name
      AND c.column_name = p.column_name
    WHERE c.run_id = '{{ run_id }}'
      AND c.model_name = '{{ model_name }}'
      AND p.run_id = '{{ prev_run_id }}'
      AND c.avg_value IS NOT NULL
      AND p.avg_value IS NOT NULL
  {% endset %}
  {% set stat_result = run_query(stat_check) %}

  {% for srow in stat_result.rows %}
    {% set delta = srow[3] | float %}

    {% if delta > 30 or delta < -30 %}
      {% set severity = 'critical' if (delta > 80 or delta < -80) else 'high' if (delta > 50 or delta < -50) else 'medium' %}
      {% set insert_anomaly %}
        INSERT INTO {{ wb_schema }}.whybroken_anomalies
        VALUES (
          '{{ run_id }}',
          '{{ model_name }}',
          'avg_value_change',
          '{{ srow[0] }}',
          '{{ severity }}',
          'Average of {{ srow[0] }} changed by {{ delta | round(1) }}%',
          '{{ srow[1] }}',
          '{{ srow[2] }}',
          {{ delta }},
          {{ whybroken.whybroken_current_timestamp() }}
        )
      {% endset %}
      {% do run_query(insert_anomaly) %}
    {% endif %}

    {% set curr_nulls = srow[4] | int %}
    {% set prev_nulls = srow[5] | int %}
    {% if curr_nulls > prev_nulls and (curr_nulls - prev_nulls) > 10 %}
      {% set null_delta = ((curr_nulls - prev_nulls) * 100.0 / (prev_nulls if prev_nulls > 0 else 1)) | round(1) %}
      {% set insert_anomaly %}
        INSERT INTO {{ wb_schema }}.whybroken_anomalies
        VALUES (
          '{{ run_id }}',
          '{{ model_name }}',
          'null_spike',
          '{{ srow[0] }}',
          'high',
          'Null count for {{ srow[0] }} increased from {{ prev_nulls }} to {{ curr_nulls }}',
          '{{ curr_nulls }}',
          '{{ prev_nulls }}',
          {{ null_delta }},
          {{ whybroken.whybroken_current_timestamp() }}
        )
      {% endset %}
      {% do run_query(insert_anomaly) %}
    {% endif %}
  {% endfor %}

{% endmacro %}
