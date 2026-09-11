# webapp — TODO

Not implemented yet. Stack/framework decision was deliberately deferred.

When picking it up:
- Decide backend framework (e.g. FastAPI) and whether a separate frontend is needed.
- It will likely read from the warehouse/lake this platform builds (Postgres and/or
  the `processed` MinIO bucket) — see the root README for what's already running.
- Add a `docker/webapp/Dockerfile` and a `webapp` service to the root `docker-compose.yml`
  following the same pattern as the other services.
