# webapp

Streamlit dashboard on the ClickHouse warehouse (database `dwh`) built by this platform.

## Run

```bash
pip install -r webapp/requirements.txt
streamlit run webapp/app.py
```

Connection: either `.streamlit/secrets.toml` with a `[clickhouse]` table (`host`, `user`,
`password`, `database`), or the environment variables `CH_HOST`, `CH_USER` (default
`default`), `CH_PASSWORD`, `CH_DATABASE` (default `dwh`). `host` has no `https://`.
If ClickHouse Cloud is idle or blocks the client IP, the app shows a message explaining
what to check.

## Layout

- `app.py`: entry point and navigation.
- `common.py`: ClickHouse connection and shared helpers.
- `roles/`: one page per user group (Manager, Dispatch, Fleet & Safety, Finance, Sales, Data Platform).
- `views/`: the logic of each dashboard (overview, fleet, alerts, shipments, data quality,
  route profit, seasonal demand, safety, customers, BR coverage, data explorer).
- `pages/`: numbered per-view page files.

The Data Quality view is the app's own check of the warehouse data; it is separate from the
pipeline checks in `spark_jobs/dq/` that write `dwh.dq_results`.
