# dbt_whybroken

Automatic data anomaly detection for dbt. Captures row counts and column-level statistics after every `dbt run`, compares against previous runs, and flags significant changes -- no external infrastructure required.

## Quick Start

**1. Install the package**

Add to your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/aboova1/dbt_whybroken.git"
    revision: main
```

Then run:

```bash
dbt deps
```

**2. Add the on-run-end hook**

Add one line to your `dbt_project.yml`:

```yaml
on-run-end:
  - "{{ whybroken.whybroken_capture() }}"
```

**3. Run dbt**

```bash
dbt run
```

That's it. WhyBroken is now monitoring every model in your project.

## How It Works

WhyBroken runs as an `on-run-end` hook using pure SQL macros -- no Python runtime, no external services.

After each `dbt run`, it:

1. Creates a `{your_schema}_whybroken` sidecar schema (first run only)
2. Queries each successful model to capture row counts, column counts, null counts, distinct counts, and min/max/avg for numeric columns
3. Compares the new snapshot against the previous run
4. Flags significant changes as anomalies with severity levels
5. Logs anomalies directly in dbt output so you see problems immediately

## Anomaly Detection

| Anomaly | Default Trigger | Severity |
|---------|----------------|----------|
| Row count spike/drop | >50% change vs previous run | high / critical |
| Average value drift | >30% shift on numeric columns | medium / high / critical |
| Null spike | >10 new nulls in a column | high |

Severity is assigned based on magnitude. For example, a row count change over 50% is flagged as high, while over 100% is escalated to critical.

## Configuration

All thresholds are configurable via dbt vars in your `dbt_project.yml`:

```yaml
vars:
  whybroken_row_count_threshold: 50      # % change to flag row count anomaly (default: 50)
  whybroken_row_count_critical: 100      # % change for critical severity (default: 100)
  whybroken_avg_value_threshold: 30      # % change to flag avg value anomaly (default: 30)
  whybroken_avg_value_high: 50           # % change for high severity (default: 50)
  whybroken_avg_value_critical: 80       # % change for critical severity (default: 80)
  whybroken_null_spike_threshold: 10     # count increase to flag null spike (default: 10)
  whybroken_fail_on_critical: false      # fail the dbt run on critical anomalies (default: false)
```

To make dbt exit with an error when critical anomalies are detected:

```yaml
vars:
  whybroken_fail_on_critical: true
```

## Query Macros

WhyBroken provides macros to inspect captured data directly from the command line.

**View recent anomalies:**

```bash
dbt run-operation whybroken.whybroken_show_anomalies --args '{limit: 20}'
```

**View run history:**

```bash
dbt run-operation whybroken.whybroken_show_runs --args '{limit: 10}'
```

**View snapshot history for a specific model:**

```bash
dbt run-operation whybroken.whybroken_show_model_history --args '{model_name: "fct_orders", limit: 10}'
```

**View column-level drift over time:**

```bash
dbt run-operation whybroken.whybroken_show_column_drift --args '{model_name: "fct_orders", column_name: "total_amount", limit: 10}'
```

## Querying Tables Directly

You can also query the underlying tables in the `{your_schema}_whybroken` schema:

```sql
SELECT * FROM your_schema_whybroken.whybroken_anomalies
ORDER BY detected_at DESC
LIMIT 20

SELECT * FROM your_schema_whybroken.whybroken_snapshots
ORDER BY captured_at DESC

SELECT * FROM your_schema_whybroken.whybroken_column_stats
WHERE model_name = 'fct_orders'
ORDER BY captured_at DESC

SELECT * FROM your_schema_whybroken.whybroken_runs
ORDER BY started_at DESC
```

## Tables Created

WhyBroken creates four tables in a sidecar schema named `{your_schema}_whybroken`:

| Table | Description |
|-------|-------------|
| `whybroken_runs` | Log of each dbt run with start time, model count, and anomaly count |
| `whybroken_snapshots` | Per-model row counts and column counts captured at each run |
| `whybroken_column_stats` | Column-level statistics: null count, distinct count, min, max, avg |
| `whybroken_anomalies` | Detected anomalies with type, severity, affected model, and details |

## Supported Adapters

- Databricks
- Snowflake
- BigQuery
- Postgres
- Redshift
- DuckDB
