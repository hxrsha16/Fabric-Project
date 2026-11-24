# UrbanPulse — Enterprise Data & AI Platform
### A zero-cost, interview-ready implementation guide

> **Goal:** one substantial, metadata-driven data platform with 6+ sources, real authentication, incremental loading, medallion architecture, data quality, orchestration, Microsoft Fabric deployment, BI, and an AI layer (RAG + text-to-SQL + pipeline-monitoring assistant) — all runnable for **$0**.

---

## 0. TL;DR

| Decision | Choice |
|---|---|
| Project | **UrbanPulse** — NYC city-operations intelligence (311 + taxi + bike-share + weather + census), plus a "platform telemetry" source so the platform monitors itself |
| Sources | 6 core + 1 "config-only" proof source |
| Authentication | API-key header (Socrata), API key in query (Census), **GitHub App JWT → installation token**, plus auth on *your own* AI/serving API |
| Engine | **PySpark + Delta Lake**, same code on your laptop and in Fabric notebooks |
| Metadata/audit store | PostgreSQL (local) with an adapter so Fabric can use Delta tables |
| Orchestration | Local CLI runner + **Fabric Pipelines** (trial) |
| AI | Ollama (local LLM) + Qdrant (vectors) + FastAPI (secured) |
| BI | Power BI (Windows) or Metabase (any OS) |
| Primary design rule | **Local-first, Fabric as a deployment target** — because the Fabric trial expires |

---

## 1. Clean analysis of the earlier plan

The earlier plan (the `.md` you pasted) has a good core idea — metadata-driven ingestion with different load patterns per source. It also has problems you should fix before investing months.

### What to keep
- Metadata-driven framework (`source_config`, `object_config`, watermarks, audit).
- Choosing sources that force **different** ingestion patterns (API incremental, file manifest, snapshot, rolling window).
- Medallion layout, audit tables, and the three-part AI assistant (docs RAG, data Q&A, pipeline monitoring).
- Honest note that GBFS is snapshot data, not CDC.

### What to change

| Problem in earlier plan | Why it matters | Fix in this guide |
|---|---|---|
| **Almost no real authentication.** 4 of 5 chosen sources are public/no-auth | You explicitly asked for "proper authentication" | Add Socrata app token, Census API key, GitHub App JWT flow, and auth on your own API |
| **Too many technologies** (Kafka, dbt, DuckDB, PySpark, LangChain, Fabric, Docker, Postgres…) | Breadth without depth looks shallow in interviews | One engine (Spark/Delta). dbt and Kafka become *optional stretch*, not core |
| **Fabric trial treated as co-equal environment** | Trial lasts **60 days**; afterwards notebooks, pipelines, lakehouses become inactive until on paid capacity | Build and finish locally first, start the Fabric trial clock only when you're ready to deploy (Phase 6) |
| **World Bank as a main source** | Country-level data barely joins to NYC data | Use **US Census ACS** (ZIP/ZCTA-level, joins to 311 `incident_zip`). Keep World Bank as the "added by config only" proof |
| **Two Gold implementations implied** (dbt/DuckDB local vs Spark in Fabric) | Duplicated logic, drift | Gold = versioned Spark SQL files, identical locally and in Fabric. DuckDB used only for read-only serving to AI |
| **Power BI assumed** | Power BI Desktop is Windows-only; sharing needs licensing | Power BI if you're on Windows, Metabase (free, Docker) otherwise |
| **Source URLs assumed stable** (e.g. GBFS feed) | Feeds move | Discover via the MobilityData catalogue; verify every URL in Phase 1 |
| **Pasted text contained stray image links/ads** | Noise | Ignore; nothing in this guide depends on it |

### Free-source verdict

| Source | Verdict | Reason |
|---|---|---|
| NYC Open Data — 311 (Socrata) | ✅ Core | Large, updatable records, system fields, pagination, optional app token |
| NYC TLC trip records | ✅ Core | Monthly Parquet files, schema evolution (e.g. 2025 added `cbd_congestion_fee`), ~2-month publishing delay |
| GBFS (Citi Bike) | ✅ Core | Snapshot data, nested JSON, frequent polling |
| Open-Meteo | ✅ Core | Hourly time-series; free hosted API is for non-commercial use with daily limits |
| US Census ACS | ✅ Core | Free key; reference data that joins to 311 by ZIP; "upsert by year" pattern |
| GitHub REST (GitHub App) | ✅ Core | Real JWT auth, Link-header pagination, platform telemetry (Actions runs) |
| World Bank Indicators | ✅ Proof source | No auth; **add it using only YAML** to demonstrate the framework is truly metadata-driven |
| OpenAlex, USGS Water | ⏸ Stretch | Good APIs, but weak thematic fit with NYC operations |

> **Licence note:** "free API" ≠ "free to redistribute". Keep a `docs/data_licences.md` listing each source's terms and attribution. Don't commit raw data to Git.

---

## 2. What is actually free (and what isn't)

| Item | Cost | Notes |
|---|---|---|
| Python, PySpark, Delta (`delta-spark`), DuckDB, PostgreSQL, Docker, Qdrant, Ollama, Metabase | Free | Open-source |
| GitHub + Actions | Free tier | Fine for a public/private portfolio repo |
| Microsoft Fabric trial | Free for **60 days** | After it ends, non-Power-BI items (notebooks, pipelines, lakehouses) go inactive until moved to a paid capacity; Power BI items remain in a Pro workspace |
| Power BI Desktop | Free | Windows only. Sharing/publishing needs licences |
| Azure Key Vault | Small cost | **Optional.** Design a secrets abstraction with env-var backend by default |

### Fabric trial strategy (important)
1. **Check your remaining days now** — open the Account manager (profile photo, top right) → Trial status.
2. If your trial is already running and nearly over, still follow this guide locally; use Fabric for a short deployment sprint only if days remain, otherwise treat it as "Fabric-ready" and document it.
3. If you haven't started a trial, **don't start it until Phase 6**. Cancelling a trial deletes the trial capacity and its workspaces, and you might not get another one.
4. Because Fabric content can disappear, **everything lives in Git**: notebooks as thin wrappers over a Python package, SQL files, pipeline definitions (via Fabric Git integration — check current docs for supported item types), plus screenshots and a recorded demo.

---

## 3. Target architecture

```
SOURCES                              INGESTION FRAMEWORK (metadata-driven)
─────────                            ─────────────────────────────────────
NYC 311 (Socrata, app token) ──┐
TLC taxi Parquet (files)     ──┤     config/*.yaml ──► metadata store (Postgres / Delta)
GBFS Citi Bike (snapshots)   ──┤              │
Open-Meteo (rolling window)  ──┼───►  Auth manager → HTTP client (retry, backoff, rate limit)
Census ACS (API key)         ──┤              │
GitHub App (JWT) telemetry   ──┤       Strategy plug-ins:
World Bank (config-only)     ──┘       watermark | file_manifest | snapshot | rolling_window | full_upsert
                                              │
                                              ▼
                         BRONZE (Delta, raw + run_id + ingested_at + source metadata)
                                              │  dedupe, cast, DQ rules, rejects → quarantine
                                              ▼
                         SILVER (Delta MERGE, SCD1/SCD2, schema history)
                                              │  Spark SQL models
                                              ▼
                         GOLD (facts, dimensions, daily KPIs, semantic views)
                          │                         │                        │
                     Power BI / Metabase     DuckDB read-only views      Audit + DQ tables
                                                    │                        │
                                         AI LAYER (FastAPI + API-key/JWT auth)
                                         Docs RAG · Text-to-SQL · Pipeline assistant
                                         (Ollama + Qdrant)
```

---

## 4. Sources in detail

| # | Source | Auth | Load strategy | Key / watermark | Engineering concept demonstrated |
|---|---|---|---|---|---|
| 1 | **NYC 311** (Socrata dataset `erm2-nwe9`, verify ID on the portal) | App token in `X-App-Token` header | `incremental_watermark` | key `unique_key`; watermark `:updated_at` (select with `$select=:*,*`) | `$limit/$offset` pagination with stable `$order`, updates to existing rows, lookback window, MERGE |
| 2 | **TLC Yellow + Green + zone lookup** | None | `file_manifest` | `(taxi_type, year, month)` + file size/ETag | Monthly batch, manifest, schema evolution, partitioning |
| 3 | **GBFS** `station_information`, `station_status` | None | `snapshot` (poll every 5–15 min) | `(station_id, last_reported/snapshot_ts)` | Nested JSON, append-only snapshots, SCD2 for station attributes |
| 4 | **Open-Meteo** archive/forecast | None (non-commercial) | `rolling_window` | `(location_id, observation_hour)` | Re-fetch last N days to absorb revisions; rate limiting |
| 5 | **US Census ACS 5-year** | API key (query param `key`) | `full_upsert` by year | `(zcta, year, variable)` | Reference/dimension data, per-year upsert |
| 6 | **GitHub App** — Actions workflow runs, commits | **JWT (RS256) → installation token** | `incremental_watermark` | `updated_at` / `created` filter | Real token lifecycle, `Link`-header pagination, rate-limit headers |
| 7 | **World Bank** (proof) | None | `full_upsert` | `(country, indicator, year)` | Added with **zero Python changes** |

### Scope controls (keep it laptop-friendly)
- 311: start with `initial_load_from: 2023-01-01`, widen later.
- TLC: last 12 months of yellow + zone lookup first.
- Weather: 5 borough centroid coordinates.
- GBFS: poll every 15 minutes during development.
- Recommended machine: 16 GB RAM, ~40 GB free disk.

---

## 5. Lean tech stack

| Area | Tool | Introduced in |
|---|---|---|
| Language | Python 3.11 | Phase 0 |
| HTTP | `requests` / `httpx`, `tenacity` (retry), `PyJWT[crypto]` | Phase 1 |
| Config | YAML + `pydantic` models, `python-dotenv` | Phase 0 |
| Processing/storage | PySpark + Delta Lake (`delta-spark`) — requires Java 17 | Phase 3 |
| Metadata/audit | PostgreSQL (Docker) via SQLAlchemy | Phase 2 |
| Data quality | Custom rule engine (YAML-driven) | Phase 4 |
| Serving | DuckDB (read-only, `delta_scan`) | Phase 7 |
| Orchestration | CLI runner, Fabric Pipelines | Phases 2, 6 |
| BI | Power BI or Metabase | Phase 5 |
| AI | Ollama, Qdrant, FastAPI, `sqlglot` | Phase 7 |
| CI/CD | GitHub Actions, `ruff`, `pytest` | Phase 0 onward |

**Optional stretch (don't start until core is done):** dbt-duckdb for tests/lineage docs, Kafka streaming for GBFS, Airflow/Dagster, Terraform.

> **Windows users:** run Spark and Docker inside **WSL2 (Ubuntu)** to avoid `winutils`/path issues. Keep the repo in the Linux filesystem (`~/UrbanPulse`), not `/mnt/c/...`.

---

## 6. Repository structure

```
UrbanPulse/
├── config/
│   ├── sources.yaml
│   ├── objects.yaml
│   ├── dq_rules.yaml
│   └── schemas/                  # column contracts per object
├── src/urbanpulse/
│   ├── common/                   # logging, settings, secrets, errors
│   ├── auth/                     # api_key.py, github_app.py, token_cache.py
│   ├── clients/                  # http_client.py, socrata.py, gbfs.py, ...
│   ├── strategies/               # watermark.py, file_manifest.py, snapshot.py, rolling_window.py, full_upsert.py
│   ├── metadata/                 # store.py (interface), postgres_store.py, delta_store.py
│   ├── bronze/ silver/ gold/
│   ├── quality/                  # rules.py, runner.py
│   ├── orchestration/            # run.py (CLI), dependency resolution
│   └── serving/                  # duckdb views, sql_guard.py, api.py
├── sql/
│   ├── metadata/                 # DDL
│   ├── silver/
│   └── gold/
├── notebooks/                    # THIN wrappers only (Fabric)
├── ai/
│   ├── docs_ingest/ embeddings/ retrieval/ text_to_sql/ monitoring_agent/ evaluation/
├── infrastructure/
│   └── docker-compose.yml
├── tests/{unit,integration,data_quality}/
├── docs/{architecture.md, data_dictionary.md, data_licences.md, runbooks/, decisions/}
├── .github/workflows/ci.yml
├── .env.example                  # never commit .env
├── pyproject.toml
└── README.md
```

---

## 7. Metadata model

### 7.1 Tables

| Table | Purpose |
|---|---|
| `source_config` | Source name, type, base URL, auth type, secret references, enabled flag |
| `object_config` | Endpoint, destination tables, load strategy, keys, watermark column, lookback, params, schedule group |
| `watermark_state` | Last successful watermark per object (advanced only after success) |
| `retry_config` | Max attempts, backoff, retryable status codes (per source/object) |
| `pipeline_run` | Run ID, trigger, start/end, status, error |
| `table_load_audit` | Rows extracted / inserted / updated / rejected, duration, watermark before/after |
| `dq_rule_config` / `dq_result` | Rules and each run's outcomes |
| `schema_history` | Detected schema versions and changes |
| `file_manifest` | Files seen/loaded (TLC) with size/ETag/status |

### 7.2 Core DDL (PostgreSQL)

```sql
-- sql/metadata/001_core.sql
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE meta.source_config (
  source_id      TEXT PRIMARY KEY,
  source_type    TEXT NOT NULL,            -- socrata | rest | file_http | gbfs
  base_url       TEXT NOT NULL,
  auth_type      TEXT NOT NULL,            -- none | api_key_header | api_key_query | github_app
  auth_params    JSONB NOT NULL DEFAULT '{}',   -- secret NAMES only, never values
  is_enabled     BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE meta.object_config (
  object_id        TEXT PRIMARY KEY,       -- e.g. nyc311.service_requests
  source_id        TEXT NOT NULL REFERENCES meta.source_config,
  endpoint         TEXT NOT NULL,
  load_strategy    TEXT NOT NULL,          -- incremental_watermark | file_manifest | snapshot | rolling_window | full_upsert
  primary_keys     TEXT[] NOT NULL,
  watermark_column TEXT,
  lookback_minutes INT  NOT NULL DEFAULT 0,
  page_size        INT  NOT NULL DEFAULT 5000,
  params           JSONB NOT NULL DEFAULT '{}',
  bronze_table     TEXT NOT NULL,
  silver_table     TEXT NOT NULL,
  run_group        TEXT NOT NULL DEFAULT 'daily',
  depends_on       TEXT[] NOT NULL DEFAULT '{}',
  is_enabled       BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE meta.watermark_state (
  object_id       TEXT PRIMARY KEY REFERENCES meta.object_config,
  watermark_value TEXT,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by_run  UUID
);

CREATE TABLE meta.pipeline_run (
  run_id      UUID PRIMARY KEY,
  trigger     TEXT NOT NULL,               -- manual | schedule | ci
  started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status      TEXT NOT NULL,               -- running | success | partial | failed
  error       TEXT
);

CREATE TABLE meta.table_load_audit (
  audit_id        BIGSERIAL PRIMARY KEY,
  run_id          UUID NOT NULL REFERENCES meta.pipeline_run,
  object_id       TEXT NOT NULL,
  layer           TEXT NOT NULL,           -- bronze | silver | gold
  status          TEXT NOT NULL,
  rows_extracted  BIGINT DEFAULT 0,
  rows_inserted   BIGINT DEFAULT 0,
  rows_updated    BIGINT DEFAULT 0,
  rows_rejected   BIGINT DEFAULT 0,
  watermark_before TEXT,
  watermark_after  TEXT,
  started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at     TIMESTAMPTZ,
  error           TEXT
);
```

### 7.3 YAML (source of truth in Git, seeded into the metadata store)

```yaml
# config/sources.yaml
sources:
  nyc311:
    type: socrata
    base_url: https://data.cityofnewyork.us
    auth: { type: api_key_header, header: X-App-Token, secret: SOCRATA_APP_TOKEN }
  tlc:
    type: file_http
    base_url: https://d37ci6vzurychx.cloudfront.net
    auth: { type: none }
  open_meteo:
    type: rest
    base_url: https://archive-api.open-meteo.com
    auth: { type: none }
  census:
    type: rest
    base_url: https://api.census.gov
    auth: { type: api_key_query, param: key, secret: CENSUS_API_KEY }
  github:
    type: rest
    base_url: https://api.github.com
    auth:
      type: github_app
      app_id_secret: GH_APP_ID
      installation_id_secret: GH_INSTALLATION_ID
      private_key_secret: GH_APP_PRIVATE_KEY_PATH
  world_bank:                 # added in Phase 8 — config only
    type: rest
    base_url: https://api.worldbank.org
    auth: { type: none }
```

```yaml
# config/objects.yaml
objects:
  nyc311.service_requests:
    source: nyc311
    endpoint: /resource/erm2-nwe9.json
    strategy: incremental_watermark
    primary_keys: [unique_key]
    watermark_column: ":updated_at"
    lookback_minutes: 120
    page_size: 50000
    params:
      select: ":*,*"
      initial_load_from: "2023-01-01T00:00:00"
    bronze_table: bronze.nyc_311_raw
    silver_table: silver.service_requests

  tlc.yellow_trips:
    source: tlc
    endpoint: "/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    strategy: file_manifest
    primary_keys: [VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID]
    params: { start: "2025-01", taxi_type: yellow }
    bronze_table: bronze.tlc_trips_raw
    silver_table: silver.taxi_trips

  openmeteo.hourly_weather:
    source: open_meteo
    endpoint: /v1/archive
    strategy: rolling_window
    primary_keys: [location_id, observation_time]
    lookback_minutes: 10080            # re-fetch last 7 days
    params:
      hourly: temperature_2m,precipitation,wind_speed_10m
      timezone: America/New_York
      locations: { manhattan: [40.78,-73.97], brooklyn: [40.65,-73.95], queens: [40.73,-73.80], bronx: [40.85,-73.87], staten_island: [40.58,-74.15] }
    bronze_table: bronze.weather_hourly_raw
    silver_table: silver.weather_observations

  world_bank.indicators:           # proof of "config only"
    source: world_bank
    endpoint: /v2/country/{country}/indicator/{indicator}
    strategy: full_upsert
    primary_keys: [country, indicator, year]
    params: { countries: [USA, IND], indicators: [NY.GDP.MKTP.CD, SP.POP.TOTL], format: json, per_page: 1000 }
    bronze_table: bronze.world_bank_indicators_raw
    silver_table: silver.economic_indicators
```

---

## 8. Implementation phases

> **Pace:** ~8–10 hours/week → roughly 10–12 weeks. Each phase has a *Definition of Done* — don't move on until it's met, and commit at every milestone.

### Phase 0 — Foundations (Week 1)

**Tasks**
1. Install: Git, Python 3.11, Docker, Java 17, (Windows: WSL2 Ubuntu).
2. Create repo, `pyproject.toml`, virtualenv, `ruff` + `pytest`, pre-commit.
3. Create `.gitignore` including `.env`, `*.pem`, `data/`, `spark-warehouse/`, `metastore_db/`.
4. `.env.example` listing required secret *names*:
   ```
   SOCRATA_APP_TOKEN=
   CENSUS_API_KEY=
   GH_APP_ID=
   GH_INSTALLATION_ID=
   GH_APP_PRIVATE_KEY_PATH=
   POSTGRES_URL=postgresql+psycopg://urbanpulse:urbanpulse@localhost:5432/urbanpulse
   ```
5. Register for keys: Socrata app token (NYC Open Data developer settings), Census API key. Reuse the GitHub App you already built.
6. Secrets abstraction:
   ```python
   # src/urbanpulse/common/secrets.py
   import os
   from typing import Protocol

   class SecretProvider(Protocol):
       def get(self, name: str) -> str: ...

   class EnvSecretProvider:
       def get(self, name: str) -> str:
           try:
               return os.environ[name]
           except KeyError as e:
               raise RuntimeError(f"Missing secret: {name}") from e
   # Later: KeyVaultSecretProvider / Fabric provider behind the same interface
   ```
7. Structured logging (JSON) with `run_id` and `object_id` in every log line.
8. First CI workflow (lint + tests).

**Definition of Done:** repo on GitHub, CI green, `pytest` runs, no secret ever committed.

---

### Phase 1 — Authentication & API client framework (Weeks 2–3)

**Tasks**
1. **Resilient HTTP client**: timeouts, retries with exponential backoff + jitter, honour `Retry-After` on 429, retry only 429/5xx/timeouts, request logging without secrets.
   ```python
   # src/urbanpulse/clients/http_client.py
   import requests
   from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception

   RETRYABLE = {429, 500, 502, 503, 504}

   def _is_retryable(exc: BaseException) -> bool:
       if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
           return True
       if isinstance(exc, requests.HTTPError) and exc.response is not None:
           return exc.response.status_code in RETRYABLE
       return False

   class HttpClient:
       def __init__(self, base_url, auth, timeout=60):
           self.base_url, self.auth, self.timeout = base_url.rstrip("/"), auth, timeout
           self.session = requests.Session()

       @retry(stop=stop_after_attempt(5),
              wait=wait_exponential_jitter(initial=1, max=60),
              retry=retry_if_exception(_is_retryable), reraise=True)
       def get(self, path, params=None):
           req = requests.Request("GET", self.base_url + path, params=params)
           req = self.auth.apply(req)               # auth strategy mutates headers/params
           resp = self.session.send(self.session.prepare_request(req), timeout=self.timeout)
           resp.raise_for_status()
           return resp
   ```
2. **Auth strategies** with one interface `apply(request) -> request`: `NoAuth`, `ApiKeyHeader`, `ApiKeyQuery`, `GitHubAppAuth`.
3. **GitHub App auth (token lifecycle)**:
   ```python
   # src/urbanpulse/auth/github_app.py
   import time, jwt, requests
   from datetime import datetime, timezone

   class GitHubAppAuth:
       def __init__(self, app_id, installation_id, private_key_pem):
           self.app_id, self.installation_id, self.key = app_id, installation_id, private_key_pem
           self._token, self._expires = None, 0.0

       def _app_jwt(self):
           now = int(time.time())
           return jwt.encode({"iat": now - 60, "exp": now + 540, "iss": str(self.app_id)},
                             self.key, algorithm="RS256")

       def _refresh(self):
           r = requests.post(
               f"https://api.github.com/app/installations/{self.installation_id}/access_tokens",
               headers={"Authorization": f"Bearer {self._app_jwt()}",
                        "Accept": "application/vnd.github+json"}, timeout=30)
           r.raise_for_status()
           body = r.json()
           self._token = body["token"]
           self._expires = datetime.fromisoformat(body["expires_at"].replace("Z", "+00:00")).timestamp()

       def apply(self, req):
           if not self._token or time.time() > self._expires - 120:   # refresh 2 min early
               self._refresh()
           req.headers["Authorization"] = f"Bearer {self._token}"
           req.headers["Accept"] = "application/vnd.github+json"
           return req
   ```
4. **Paginators** (strategy pattern): `OffsetPaginator` (Socrata), `LinkHeaderPaginator` (GitHub), `PageNumberPaginator` (World Bank `[meta, data]` response), `NoPagination` (GBFS, files).
5. **Source adapters** returning iterators of record batches: `SocrataAdapter`, `GbfsAdapter`, `OpenMeteoAdapter`, `CensusAdapter`, `GitHubAdapter`, `HttpFileAdapter`.
6. **Verify every endpoint** by hand with `curl` before coding it: Socrata dataset ID and system fields, the current Citi Bike GBFS discovery URL (find it via the MobilityData systems catalogue), Census variable names for the ACS year you choose, Open-Meteo parameters.
7. Unit tests with mocked HTTP (`responses` or `respx`): retry on 503, no retry on 401/404, token refresh when expired, pagination stops correctly.

**Definition of Done:** each adapter can pull one page from its real source from the CLI; tests prove retry/refresh/pagination; no tokens appear in logs.

---

### Phase 2 — Metadata-driven ingestion engine (Weeks 3–5)

**Tasks**
1. `docker-compose.yml` with Postgres (and later Qdrant, Metabase):
   ```yaml
   services:
     postgres:
       image: postgres:16
       environment: { POSTGRES_USER: urbanpulse, POSTGRES_PASSWORD: urbanpulse, POSTGRES_DB: urbanpulse }
       ports: ["5432:5432"]
       volumes: ["pgdata:/var/lib/postgresql/data"]
   volumes: { pgdata: {} }
   ```
2. Apply `sql/metadata/*.sql`; write a **config loader** that validates YAML with pydantic and upserts into `source_config` / `object_config`.
3. `MetadataStore` interface (`get_watermark`, `commit_watermark`, `start_run`, `finish_run`, `record_audit`) with `PostgresStore` now; `DeltaStore` in Phase 6.
4. **Strategy plug-ins** — each implements `plan(obj, state)` and `extract(...)`:

   | Strategy | Behaviour |
   |---|---|
   | `incremental_watermark` | `WHERE watermark_col > last_wm - lookback`, ordered by `(watermark_col, key)`, paginated; new watermark = max seen |
   | `file_manifest` | Generate expected `(type, year, month)` list, HEAD each URL, skip loaded files with same size/ETag, load new/changed, record in `file_manifest` |
   | `snapshot` | Always append with `snapshot_ts` (use feed `last_updated`); no watermark |
   | `rolling_window` | Re-fetch `[now - lookback, now]` every run; Silver merge absorbs revisions |
   | `full_upsert` | Fetch all configured keys/years; MERGE on primary key |

5. **Golden rule — commit watermark last:**
   ```python
   def run_object(obj, run_id, store, strategy, bronze_writer):
       wm_before = store.get_watermark(obj.object_id)
       audit = store.start_audit(run_id, obj.object_id, "bronze", wm_before)
       try:
           new_wm, rows = wm_before, 0
           for batch in strategy.extract(obj, wm_before):       # yields pyarrow/pandas batches
               bronze_writer.append(batch, run_id=run_id)        # durable write FIRST
               rows += len(batch)
               new_wm = strategy.advance(new_wm, batch)
           store.commit_watermark(obj.object_id, new_wm, run_id) # only after all writes succeed
           store.finish_audit(audit, "success", rows_extracted=rows, watermark_after=new_wm)
       except Exception as e:
           store.finish_audit(audit, "failed", error=str(e))     # watermark NOT advanced
           raise
   ```
   Re-running after a failure is safe because Silver MERGE is idempotent.
5. **Orchestration CLI:** resolves `depends_on`, runs groups, isolates failures (one failed object doesn't kill others), supports re-running only failed objects:
   ```
   python -m urbanpulse.orchestration.run --group daily
   python -m urbanpulse.orchestration.run --object nyc311.service_requests --full-refresh
   python -m urbanpulse.orchestration.run --failed-from <run_id>
   ```

**Definition of Done:** run the pipeline twice → second run loads only new/changed data; kill it mid-run → rerun recovers with no duplicates and correct audit rows; adding an object requires YAML only.

---

### Phase 3 — Medallion architecture on Delta (Weeks 5–7)

**Bronze**
- Append-only Delta, partitioned by `_ingest_date`.
- Added columns: `_run_id`, `_ingested_at`, `_source_id`, `_object_id`, `_source_file` (TLC), `_batch_id`.
- API sources: keep the original payload; flatten only for trivially typed fields. File sources: keep source columns, enable `mergeSchema`.

**Silver**
- Cast types, normalise names, trim strings, parse timestamps, standardise boroughs/ZIPs.
- Deduplicate within batch, then MERGE:
  ```python
  from pyspark.sql import Window, functions as F
  from delta.tables import DeltaTable

  w = Window.partitionBy("unique_key").orderBy(F.col("updated_at").desc())
  src = (bronze_df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn"))

  (DeltaTable.forName(spark, "silver.service_requests").alias("t")
     .merge(src.alias("s"), "t.unique_key = s.unique_key")
     .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")   # ignore stale replays
     .whenNotMatchedInsertAll()
     .execute())
  ```
- **SCD Type 2** for `silver.bike_stations` (name, capacity, location changes) with `valid_from`, `valid_to`, `is_current`.
- **Schema evolution:** compare incoming schema vs. registered contract; log additions in `schema_history`; fail on type-breaking changes; allow additive columns (e.g. TLC's 2025 `cbd_congestion_fee`).
- **Quarantine:** invalid rows → `silver._rejects` with `reject_reason`, never silently dropped.

**Gold (versioned Spark SQL in `sql/gold/`)**

| Table | Grain |
|---|---|
| `dim_date`, `dim_location` (borough/ZIP/zone), `dim_service_type`, `dim_weather_condition`, `dim_station` (SCD2 current/history) | — |
| `fact_service_requests` | one 311 request |
| `fact_taxi_trips` | one trip (partitioned by month) |
| `fact_bike_availability` | station × snapshot |
| `fact_weather` | borough × hour |
| `fact_mobility_daily` | borough × day |
| `fact_city_operations_daily` | borough × day: 311 counts, closure time, taxi trips, rainfall, bike shortage rate, population-normalised metrics from Census |
| `fact_platform_runs` | CI/pipeline telemetry from GitHub + `table_load_audit` |

**Reconciliation:** each run compares source row counts ↔ Bronze ↔ Silver (+ rejects) and stores the result; mismatches raise an alert.

**Definition of Done:** full Bronze → Silver → Gold from raw sources on your laptop; a record updated at source in 311 changes in Silver and Gold on the next run; reconciliation report exists.

---

### Phase 4 — Data quality, auditing, monitoring (Week 7)

**`config/dq_rules.yaml`**
```yaml
silver.service_requests:
  - { name: pk_not_null,  type: not_null, column: unique_key,  severity: error }
  - { name: pk_unique,    type: unique,   columns: [unique_key], severity: error }
  - { name: lat_range,    type: range,    column: latitude, min: 40.4, max: 41.0, severity: warn }
  - { name: freshness,    type: freshness, column: updated_at, max_age_hours: 48, severity: warn }
silver.taxi_trips:
  - { name: fare_nonneg,  type: range,    column: total_amount, min: 0, severity: warn }
  - { name: dropoff_after_pickup, type: expression, expr: "tpep_dropoff_datetime >= tpep_pickup_datetime", severity: error }
  - { name: zone_ref,     type: referential, column: PULocationID, ref_table: silver.taxi_zones, ref_column: LocationID, severity: warn }
```
- `error` severity → quarantine rows and mark run `partial`; `warn` → record only.
- Persist every outcome in `dq_result` (rule, rows checked, rows failed, run_id).
- Metrics to track per run: duration, rows by layer, rejects, retries, API calls, rate-limit remaining, watermark lag.
- Alerts (free): log + GitHub Issue creation via your GitHub App on failed runs — a nice demo of reusing the auth work.

**Definition of Done:** break a rule on purpose → run is marked `partial`, bad rows are in quarantine, `dq_result` explains why.

---

### Phase 5 — Analytics & BI (Weeks 8–9)

1. Gold semantic views: `vw_city_ops_daily`, `vw_311_by_type`, `vw_taxi_vs_weather`, `vw_bike_shortages`, `vw_pipeline_health`.
2. Dashboard pages:
   - **City Operations:** 311 volume/closure time by borough, top complaint types, per-capita rates.
   - **Mobility & Weather:** taxi trips on rainy vs dry days, bike shortage heatmap.
   - **Platform Health:** run success rate, rejects, freshness, API rate-limit headroom.
3. **Power BI** (Windows): import Gold (Parquet/Delta export) locally; in Fabric use Direct Lake over the Lakehouse (Phase 6). **Metabase** (any OS): add to Docker Compose and connect to a Gold export or Postgres summary tables.
4. Write 8–10 documented business questions with the SQL that answers them (reused as AI evaluation set later).

**Definition of Done:** dashboard answers the five business questions in §10; screenshots saved in `docs/`.

---

### Phase 6 — Microsoft Fabric deployment (Weeks 9–10, **start the 60-day clock here**)

1. Start trial → create workspace → create **Lakehouse** (`lh_urbanpulse`); organise Bronze/Silver/Gold using schemas if enabled, else table prefixes.
2. Build your package as a wheel (`python -m build`) and attach it to a Fabric **Environment**; keep notebooks as thin wrappers:
   ```python
   # NB_01_Ingest (parameterised)
   object_id = "nyc311.service_requests"   # parameter cell
   from urbanpulse.orchestration.run import run_single
   run_single(object_id=object_id, backend="fabric")
   ```
3. Implement `DeltaStore` (metadata/audit as Delta tables in the Lakehouse) behind the same `MetadataStore` interface.
4. **Secrets:** use a Fabric-supported secret mechanism (Azure Key Vault is the standard route and costs a few cents; otherwise connection objects/variables per current Fabric docs). Implement `FabricSecretProvider` behind `SecretProvider`. Never paste tokens into notebooks.
5. **Fabric Pipeline:** `Lookup` (read enabled objects for the run group) → `ForEach` (parallel, bounded) → `Notebook` activity (Bronze→Silver) → Gold notebook → DQ notebook; failure path writes audit + notifies. Add a schedule trigger.
6. Connect GitHub (Fabric Git integration; check which item types are currently supported) so notebooks/pipelines are versioned.
7. **Power BI:** semantic model on Gold (Direct Lake if available), one DAX measure set (YoY, rolling 7-day, per-capita).
8. **Evidence pack (do this before day 45):** screenshots of lakehouse, pipeline runs, monitoring, semantic model; a 3–5 minute screen recording; `docs/fabric_deployment.md` with an exit plan ("after trial ends, platform continues locally; Fabric artefacts are reproducible from Git").

**Definition of Done:** scheduled Fabric pipeline completes end-to-end; the same package runs locally and in Fabric with only a backend flag changed.

---

### Phase 7 — AI layer (Weeks 10–12)

**Setup (local, free):**
```bash
# Ollama (install from ollama.com), then:
ollama pull llama3.2:3b          # light chat model (works on modest RAM)
ollama pull nomic-embed-text     # embeddings
# If you have 16 GB+ RAM, try a larger model for SQL, e.g. qwen2.5-coder:7b
```
Add Qdrant to Docker Compose (`qdrant/qdrant`, port 6333).

#### 7A. Documentation RAG
1. Corpus: `docs/*.md`, data dictionary, source API notes, runbooks, ADRs, metadata YAML.
2. Chunk by markdown heading (~400–800 tokens, small overlap), store `source_path`, `heading`, `commit_sha` as payload.
3. Embed with `nomic-embed-text`, upsert to Qdrant, retrieve top-k (k=4–6), answer with citations of file + heading; if retrieval score is low → say "not found in docs".
4. **Evaluation:** build 25–40 Q→expected-source pairs; report **hit-rate@k** and **MRR**, plus manual faithfulness checks. Record results in `ai/evaluation/results.md` — this is what separates a demo from engineering.

#### 7B. Text-to-SQL analytics assistant (with guardrails)
- LLM sees only the **schema + descriptions of approved Gold views** (not raw tables), plus few-shot examples from your Phase 5 question set.
- Execution through DuckDB opened **read-only**, views over Gold Delta (`delta_scan`).
- Validation before execution:
  ```python
  import sqlglot
  from sqlglot import exp

  ALLOWED = {"gold.vw_city_ops_daily", "gold.vw_311_by_type", "gold.vw_taxi_vs_weather", "gold.vw_bike_shortages"}
  MAX_ROWS = 1000

  def validate_and_limit(sql: str) -> str:
      tree = sqlglot.parse_one(sql, read="duckdb")
      if not isinstance(tree, exp.Select):
          raise ValueError("Only SELECT statements are allowed")
      banned = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Command, exp.Alter)
      if any(tree.find_all(*banned)):
          raise ValueError("Statement type not allowed")
      ctes = {c.alias for c in tree.find_all(exp.CTE)}
      for t in tree.find_all(exp.Table):
          name = ".".join(p for p in (t.db, t.name) if p)
          if t.name not in ctes and name not in ALLOWED:
              raise ValueError(f"Table not allowed: {name}")
      if not tree.args.get("limit"):
          tree = tree.limit(MAX_ROWS)
      return tree.sql(dialect="duckdb")
  ```
- Add query timeout, row cap, and log every question → SQL → result size.
- **Evaluation:** golden set of 20–30 questions with reference SQL; score execution success and result match. Report accuracy honestly, including failure categories.

#### 7C. Pipeline-monitoring assistant
- Tools (functions the LLM may call): `get_last_run_status()`, `get_failed_objects(run_id)`, `get_rejects_summary(object_id)`, `get_stale_tables(max_age_hours)`, `get_dq_failures(run_id)` — all read-only queries over audit/DQ tables.
- Answers like "Why did the last run fail?" come from tool results, not from model guesswork; include the run ID in the answer.

#### 7D. Secured serving API (authentication on *your* side)
- FastAPI endpoints: `POST /ask/docs`, `POST /ask/data`, `POST /ask/ops`.
- API-key auth (store only hashed keys) with per-key roles: `viewer` (docs + data) vs `operator` (adds ops tools). Rate-limit per key; audit-log requests.
- Never expose Ollama/Qdrant ports publicly.

**Definition of Done:** three assistants answer correctly on your evaluation sets; malicious SQL (`DROP`, other tables, `;` chains) is rejected by tests; unauthenticated calls return 401.

---

### Phase 8 — CI/CD, testing, documentation, polish (Weeks 12+)

**CI workflow**
```yaml
# .github/workflows/ci.yml
name: ci
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: "17" }
      - run: pip install -e ".[dev]"
      - run: ruff check .
      - run: pytest tests/unit -q
      - run: python -m urbanpulse.config.validate   # validates YAML configs against schemas
```
- Integration tests: tiny sample payloads → Bronze → Silver → Gold on local Spark (run on schedule or manually, not necessarily every push).
- Data-quality tests: Gold invariants (no negative counts, daily totals reconcile to Silver).
- **Prove the framework:** add the **World Bank** source using only YAML + (if needed) one SQL file — commit it as a visible "zero code change" demo.
- Optional: nightly workflow that runs a sampled pipeline and opens a GitHub issue on failure.

**Documentation checklist**
- `README.md`: problem, architecture diagram, quickstart (`docker compose up`, 3 commands), screenshots, tech choices, limitations.
- `docs/architecture.md`, `docs/data_dictionary.md`, `docs/data_licences.md`.
- `docs/decisions/` — ADRs (see below).
- `docs/runbooks/` — "pipeline failed", "token expired", "schema change detected", "backfill 311".

---

## 9. Final table inventory

| Layer | Tables |
|---|---|
| Metadata | `source_config`, `object_config`, `watermark_state`, `retry_config`, `file_manifest`, `pipeline_run`, `table_load_audit`, `dq_rule_config`, `dq_result`, `schema_history` |
| Bronze | `nyc_311_raw`, `tlc_trips_raw`, `tlc_zones_raw`, `gbfs_station_information_raw`, `gbfs_station_status_raw`, `weather_hourly_raw`, `census_acs_raw`, `github_workflow_runs_raw`, `world_bank_indicators_raw` |
| Silver | `service_requests`, `taxi_trips`, `taxi_zones`, `bike_stations` (SCD2), `bike_station_snapshots`, `weather_observations`, `demographics_zcta`, `platform_workflow_runs`, `economic_indicators`, `_rejects` |
| Gold | `dim_date`, `dim_location`, `dim_service_type`, `dim_weather_condition`, `dim_station`, `fact_service_requests`, `fact_taxi_trips`, `fact_bike_availability`, `fact_weather`, `fact_mobility_daily`, `fact_city_operations_daily`, `fact_platform_runs` |

---

## 10. Business questions the finished platform answers

1. Which boroughs/ZIPs have the highest 311 complaint rate **per capita**, and which complaint types drive it?
2. How does rainfall change daily taxi trip volume and average trip distance?
3. Which bike stations run empty/full most often, and when?
4. Is there a relationship between taxi activity and certain 311 complaint types (e.g. noise)?
5. How long do 311 requests take to close by agency and borough, and is it improving?
6. *(Platform)* Which sources are stalest, which fail most, and how many rows are rejected?

---

## 11. Architecture Decision Records to write (interview gold)

| ADR | Decision & trade-off to explain |
|---|---|
| 001 | Metadata-driven config vs per-source notebooks |
| 002 | Watermark committed after durable write; lookback window and why late updates are still caught |
| 003 | Different strategies per source (why not force CDC onto snapshots/files) |
| 004 | Delta MERGE with `updated_at` guard to handle replays |
| 005 | SCD2 only where history matters (stations), SCD1 elsewhere |
| 006 | Local-first with Fabric as a deployment target (trial expiry risk) |
| 007 | Spark SQL for Gold on both engines vs dbt (and when dbt would win) |
| 008 | Text-to-SQL guardrails: allow-listed views, parser validation, read-only connection |
| 009 | Local LLM trade-offs (privacy/cost vs accuracy) and how evaluation exposed them |
| 010 | Secrets abstraction (env → Key Vault) |

---

## 12. How to present this in interviews

**README headline:** "Metadata-driven lakehouse platform ingesting 6 NYC/open sources with API-key and GitHub-App (JWT) authentication, incremental/snapshot/file-based loading, medallion architecture on Delta Lake, YAML-driven data quality, Fabric deployment, and a guarded AI analytics layer."

**CV bullets — fill in your *real* numbers after measuring them:**
- Designed a metadata-driven ingestion framework (YAML → Postgres/Delta) supporting 5 load strategies; onboarded a new source via configuration only.
- Implemented OAuth-style GitHub App authentication (JWT → installation tokens) with proactive refresh, plus API-key auth, retry/backoff and rate-limit handling.
- Built Bronze/Silver/Gold on Delta Lake with idempotent MERGE, SCD2, schema-evolution detection, quarantine tables and reconciliation.
- Deployed to Microsoft Fabric with parameterised notebooks and pipelines; same package runs locally.
- Built a RAG assistant (Qdrant + Ollama) and a guarded text-to-SQL service; evaluated retrieval (hit-rate@k) and SQL accuracy on a golden set.

**Questions you'll be ready for:** "What happens if a run fails halfway?", "How do you handle late-arriving updates?", "How do you stop the LLM running `DROP TABLE`?", "Why not use CDC for all sources?", "How would this scale to 100× data / 100 sources?", "What would you change for production?" (answer: managed secrets, capacity sizing, observability stack, access control, cost monitoring).

---

## 13. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Scope creep | Core = Phases 0–7. Everything else is stretch |
| Spark setup pain on Windows | Use WSL2 + Linux filesystem; Docker as fallback |
| Source API/feed changes | Verify endpoints in Phase 1; keep adapter tests with recorded fixtures |
| Rate limits / non-commercial terms | Use tokens where offered, cache raw responses, keep usage personal/portfolio |
| Fabric trial expiry | Local-first design, Git-versioned artefacts, evidence pack by day 45 |
| LLM hallucination / weak SQL | Allow-list + validation + evaluation set; report accuracy honestly |
| Leaking secrets in Git | `.gitignore`, pre-commit secret scan (e.g. `gitleaks`), rotate anything ever exposed |
| Laptop resource limits | Date-window 311, 12 months of TLC, small Ollama models |

---

## 14. Stretch goals (only after core is done)

- Kafka/Redpanda streaming for GBFS and event-driven ingestion.
- dbt-duckdb for model tests + lineage docs over Gold.
- Orchestrate locally with Airflow or Dagster.
- Borough enrichment of bike stations via point-in-polygon with borough boundary files (shapely/geopandas).
- OpenAlex / USGS connectors as additional "config-only" sources.
- Data contracts + automated schema-drift alerts.
- Terraform for Azure resources, cost dashboard.

---

## 15. Reference links to verify while building

- NYC Open Data / Socrata SODA docs (pagination, SoQL, system fields, app tokens): `dev.socrata.com`
- NYC TLC trip record data (monthly Parquet, data dictionaries, zone lookup): `nyc.gov/site/tlc/about/tlc-trip-record-data.page`
- GBFS specification and system catalogue: `gbfs.org`, MobilityData `systems.csv`
- Open-Meteo docs and terms: `open-meteo.com`
- US Census API (ACS 5-year, variables, key signup): `api.census.gov`
- GitHub Apps authentication docs: `docs.github.com` → "Authenticating as a GitHub App installation"
- Microsoft Fabric trial and "ending your trial": `learn.microsoft.com/fabric`
- Ollama model library: `ollama.com/library`; Qdrant docs: `qdrant.tech`

---

## 16. Your very first steps

1. Check your Fabric trial's **Days left** (Account manager).
2. Create the GitHub repo and complete **Phase 0** (repo, CI, secrets pattern, `.env.example`).
3. Request the Socrata app token and Census API key.
4. Start Phase 1 with the HTTP client + GitHub App auth, and verify each source endpoint by hand.

Build one milestone at a time, commit often, and write the ADR for each decision *when you make it* — that's what makes the finished repo convincing.
