# RTIE — Regulatory Trace & Intelligence Engine

RTIE is a read-only multi-agent AI system built on Oracle OFSAA FSAPPS that explains the complete logic behind regulatory capital computations — tracing PL/SQL functions, T2T transformations, and RRF rules to give engineers instant, fully cited answers without touching the underlying system.

---

## Prerequisites

- **Python 3.11+** — required runtime
- **Poetry** — dependency management (`pip install poetry`)
- **Docker & Docker Compose** — for Redis and PostgreSQL
- **Oracle Database** — access to an OFSAA FSAPPS instance (read-only credentials)
- **Azure OpenAI API** — GPT-4o deployment with API key and endpoint
- **LangSmith Account** — for observability and tracing (optional but recommended)

---

## Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd RTIE
   ```

2. **Start infrastructure services**:
   ```bash
   docker-compose up -d
   ```
   This starts Redis (port 6379) and PostgreSQL (port 5432) with the memory tables auto-initialized.

3. **Configure environment variables**:
   ```bash
   cp .env.dev .env.dev.local
   ```
   Edit `.env.dev` with your actual Oracle, Azure OpenAI, and LangSmith credentials.

4. **Install Python dependencies**:
   ```bash
   poetry install
   ```

5. **Verify connectivity**:
   ```bash
   curl http://localhost:8000/health
   ```

---

## How to Run

1. **Start infrastructure** (if not already running):
   ```bash
   docker-compose up -d
   ```

2. **Install dependencies**:
   ```bash
   poetry install
   ```

3. **Run the application**:
   ```bash
   poetry run uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
   ```

4. **Send a query**:
   ```bash
   curl -X POST http://localhost:8000/v1/query \
     -H "Content-Type: application/json" \
     -d '{
       "query": "Explain the logic of FN_CALC_RWA",
       "session_id": "test-session-001",
       "engineer_id": "engineer@company.com"
     }'
   ```

---

## How to Add a New Module/Batch

RTIE organizes Oracle OFSAA modules under `db/modules/`. To add a new module:

1. Create a directory under `db/modules/` matching the module name:
   ```
   db/modules/YOUR_MODULE_NAME/
   ├── functions/
   │   ├── FN_YOUR_FUNCTION.sql
   │   └── ...
   └── procedures/
       ├── SP_YOUR_PROCEDURE.sql
       └── ...
   ```

2. Place individual PL/SQL source files in the appropriate subdirectory (`functions/` or `procedures/`).

3. The naming convention is: `{TYPE_PREFIX}_{OBJECT_NAME}.sql` (e.g., `FN_CALC_RWA.sql`, `SP_PROCESS_DATA.sql`).

4. These files serve as local references. RTIE always fetches live source from Oracle `ALL_SOURCE` at query time, with Redis caching.

---

## Slash Command Reference

| Command | Args | Description |
|---------|------|-------------|
| `/refresh-cache <name>` | Object name | Fetch latest source from Oracle and update Redis cache for one object |
| `/refresh-cache-all` | None | Re-sync all functions and procedures for the configured schema |
| `/cache-status <name>` | Object name | Show cached_at, oracle_last_ddl_time, version_hash for one object |
| `/cache-list` | None | List all `logic:*` keys currently stored in Redis |
| `/cache-clear <name>` | Object name | Delete one object's cache entry from Redis |
| `/refresh-schema` | None | Detect DDL changes in Oracle, sync to local schema files, show diff report |

---

## Architecture Overview

RTIE uses a **LangGraph StateGraph** with six specialized agents executing in a deterministic linear pipeline:

1. **Orchestrator** — Classifies incoming queries using Azure OpenAI GPT-4o. Determines whether input is a slash command or a logic explanation request. Extracts the target object name and schema.

2. **Metadata Interpreter** — Resolves PL/SQL objects in Oracle `ALL_OBJECTS`, fetches source code from Redis cache (or Oracle `ALL_SOURCE` on cache miss), and builds recursive dependency call trees up to 3 levels deep.

3. **Logic Explainer** — Sends the full source code and call tree to Azure OpenAI GPT-4o, which returns a structured, fully-cited explanation with step-by-step breakdowns, formulas, and regulatory references. LangSmith tracing is enabled.

4. **Validator** — Three validators in one agent:
   - **Cache Validator**: Compares cached DDL timestamps with Oracle `ALL_OBJECTS.LAST_DDL_TIME`
   - **Query Relevance Validator**: Pure Python check that the object name appears in the explanation
   - **Output Validator**: Verifies all referenced functions exist in the call tree; computes confidence score

5. **Cache Manager** — Handles all six slash commands for cache refresh, status, listing, clearing, and schema DDL change detection.

6. **Renderer** — Assembles the final structured response with explanation, confidence score, warnings, source citations, and an UNVERIFIED badge if validation fails.

### Pipeline Flow

```
START -> parse_query -> resolve_object -> fetch_logic -> cache_validator
      -> fetch_dependencies -> explain_logic -> query_relevance_validator
      -> output_validator -> render_response -> END
```

Slash commands bypass the graph entirely and route directly to the Cache Manager.

---

## Environment Variables Reference

| Variable | Description |
|----------|-------------|
| `ORACLE_HOST` | Oracle database hostname |
| `ORACLE_PORT` | Oracle listener port (default: 1521) |
| `ORACLE_SID` | Oracle System Identifier |
| `ORACLE_USER` | Oracle username (read-only) |
| `ORACLE_PASSWORD` | Oracle password |
| `REDIS_HOST` | Redis server hostname |
| `REDIS_PORT` | Redis server port (default: 6379) |
| `POSTGRES_HOST` | PostgreSQL hostname |
| `POSTGRES_PORT` | PostgreSQL port (default: 5432) |
| `POSTGRES_DB` | PostgreSQL database name |
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `AZURE_OPENAI_API_KEY` | Azure OpenAI API key |
| `AZURE_OPENAI_ENDPOINT` | Azure OpenAI endpoint URL |
| `AZURE_OPENAI_DEPLOYMENT` | Azure OpenAI model deployment name (e.g. gpt-4o) |
| `LANGCHAIN_TRACING_V2` | Enable LangSmith tracing (true/false) |
| `LANGCHAIN_API_KEY` | LangSmith API key |
| `LANGCHAIN_PROJECT` | LangSmith project name |
| `ENVIRONMENT` | Runtime environment (dev/staging/prod) |
