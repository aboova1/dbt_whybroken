# dbt_whybroken

Automatic data pipeline anomaly detection for dbt. Captures model snapshots after every run and flags row count swings, value drift, and null spikes — no external infrastructure required.

Works on **Databricks, Snowflake, BigQuery, Postgres, Redshift, and DuckDB**.

## Install

Add to your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/aboova1/dbt_whybroken.git"
    revision: main
```

Then run:

```
dbt deps
```

## Setup

Add one line to your `dbt_project.yml`:

```yaml
on-run-end:
  - "{{ whybroken.whybroken_capture() }}"
```

That's it. Run `dbt run` and WhyBroken automatically:

1. Creates a `{your_schema}_whybroken` sidecar schema
2. Captures row counts and column stats for every successful model
3. Compares against the previous run and logs anomalies

## Query results

```sql
-- Recent anomalies
SELECT * FROM your_schema_whybroken.whybroken_anomalies
ORDER BY detected_at DESC;

-- Model history
SELECT * FROM your_schema_whybroken.whybroken_snapshots
ORDER BY captured_at DESC;

-- Column-level stats over time
SELECT * FROM your_schema_whybroken.whybroken_column_stats
WHERE model_name = 'fct_orders'
ORDER BY captured_at DESC;

-- Run log
SELECT * FROM your_schema_whybroken.whybroken_runs
ORDER BY started_at DESC;
```

## What it detects

| Anomaly | Trigger | Severity |
|---------|---------|----------|
| Row count change | >50% swing vs previous run | high/critical |
| Average value drift | >30% shift on numeric columns | medium/high/critical |
| Null spike | >10 new nulls in a column | high |

## How it works

WhyBroken runs as an `on-run-end` hook — pure SQL macros, no Python, no external services. After each `dbt run`, it queries `information_schema` and your model tables to collect stats, then compares against the last captured run stored in the sidecar schema.
