# Governed Commercial Banking Analytics Assistant

MVP for a governed commercial banking analytics chatbot. The assistant is designed to answer business metadata questions, resolve analytical requests through certified definitions, generate safe SQL, execute it against synthetic commercial banking data, and return governed analytical answers with optional charts.

## Current Scope

This checkpoint covers Phase 0 through Phase 17:

- FastAPI backend scaffold.
- Local DuckDB connection utilities.
- Deterministic synthetic commercial banking dataset generator.
- Governed metadata catalog for table, column, metric, dimension, join, lineage, synonym, and access policy records.
- Health, data profile, and governed metadata endpoints.
- Local governed metadata search index for retrieval before semantic resolution.
- Intent classification endpoint for definition, metadata, lineage, analytics, chart, clarification, and unsupported requests.
- Governed semantic resolution with clarification options and query-plan skeletons.
- Governed SQL generation and validation from resolved query plans only.
- LangGraph orchestration across information, clarification, unsupported, and governed analytics flows.
- Governed read-only SQL execution and bounded tabular result retrieval.
- Governed analytical answer generation with key points and result evidence.
- Governed Plotly chart generation for grouped and trend analytics.
- FastAPI-served frontend chat MVP with governed clarification, result table, SQL evidence, and chart rendering.
- Conversation persistence with saved sessions, turn history, and frontend session restore.
- Governance audit reports for certification, source tables, lineage, access policies, joins, and SQL validation evidence.
- Export and share snapshots for governed answers, result tables, charts, SQL, and audit packets.
- Admin curation workflow for glossary terms, certified metrics, certified dimensions, and semantic synonyms with validation, audit events, and search-index refresh.
- Feedback capture and quality monitoring for assistant turns, issue reasons, and route-level quality signals.
- LLM-first chat orchestration with AWS Bedrock Titan support, governed metadata retrieval, conversational clarification, SQL, and source citations.
- Structured LLM intent decision contract with backend policy enforcement so definition, metadata, and lineage questions cannot generate SQL or charts.

Later phases will add evaluation set generation and production hardening.

## Quick Start

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Generate the synthetic DuckDB database:

```bash
python3 -m backend.app.synthetic_data.generate
```

Run the backend:

```bash
python3 -m uvicorn backend.app.main:app --reload
```

Open the FastAPI-served app:

```text
http://127.0.0.1:8000/
http://127.0.0.1:8000/health
http://127.0.0.1:8000/data/profile
http://127.0.0.1:8000/metadata/tables
http://127.0.0.1:8000/metadata/candidates?phrase=average%20balance
http://127.0.0.1:8000/metadata/search?query=average%20balance%20by%20segment
http://127.0.0.1:8000/admin/curation/events
http://127.0.0.1:8000/feedback/quality-summary
```

Run backend and frontend in separate terminals:

```bash
# Terminal 1: backend API
cd /Users/sunspai/Documents/BankingProject
python3 -m uvicorn backend.app.main:app --reload --host 127.0.0.1 --port 8000
```

```bash
# Terminal 2: frontend dev server with API proxy
cd /Users/sunspai/Documents/BankingProject
node frontend/dev-server.mjs
```

Then open:

```text
http://127.0.0.1:5173/
```

The frontend dev server serves files from `frontend/` and proxies API requests to `http://127.0.0.1:8000`. To use a different backend URL:

```bash
BACKEND_URL=http://127.0.0.1:8000 FRONTEND_PORT=5173 node frontend/dev-server.mjs
```

## Bedrock Titan Configuration

The backend uses an LLM client abstraction for chat orchestration. For local development and tests, `BANKING_ASSISTANT_BEDROCK_ENABLED=false` uses a local Titan-compatible fallback so the app runs without AWS credentials. To use AWS Bedrock Titan:

```bash
export BANKING_ASSISTANT_BEDROCK_ENABLED=true
export BANKING_ASSISTANT_AWS_REGION=us-east-1
export BANKING_ASSISTANT_BEDROCK_MODEL_ID=amazon.titan-text-express-v1
# Optional if you use AWS named profiles:
export BANKING_ASSISTANT_AWS_PROFILE=your-profile-name
```

Then start the backend normally:

```bash
python3 -m uvicorn backend.app.main:app --reload --host 127.0.0.1 --port 8000
```

The LLM is allowed to decide intent, clarification, semantic asset selection, SQL planning, and answer wording only from retrieved governed metadata. The LLM decision must include `response_mode`, `allow_sql`, and `allow_chart`; backend policy blocks SQL for definition, metadata, and lineage questions and blocks charts unless the intent is a chart request. Deterministic validation still blocks SQL that references non-governed, restricted, or unapproved columns.

Classify a user message:

```bash
curl -s -X POST http://127.0.0.1:8000/intent/classify \
  -H 'Content-Type: application/json' \
  -d '{"message":"Show average balance by segment"}'
```

Resolve governed semantics:

```bash
curl -s -X POST http://127.0.0.1:8000/semantic/resolve \
  -H 'Content-Type: application/json' \
  -d '{"message":"Give me average balance by segment"}'
```

Generate governed SQL after clarification:

```bash
curl -s -X POST http://127.0.0.1:8000/sql/generate \
  -H 'Content-Type: application/json' \
  -d '{
    "message": "Give me average balance by segment",
    "selected_metric_id": "metric.average_deposit_ledger_balance",
    "selected_dimension_ids": ["dimension.customer_segment"],
    "user_role": "technical_user"
  }'
```

Execute a governed query and return rows:

```bash
curl -s -X POST http://127.0.0.1:8000/query/execute \
  -H 'Content-Type: application/json' \
  -d '{
    "message": "Give me average balance by segment",
    "selected_metric_id": "metric.average_deposit_ledger_balance",
    "selected_dimension_ids": ["dimension.customer_segment"],
    "user_role": "technical_user",
    "limit": 25
  }'
```

Generate a governed analytical answer:

```bash
curl -s -X POST http://127.0.0.1:8000/answer/generate \
  -H 'Content-Type: application/json' \
  -d '{
    "message": "Give me average balance by segment",
    "selected_metric_id": "metric.average_deposit_ledger_balance",
    "selected_dimension_ids": ["dimension.customer_segment"],
    "user_role": "technical_user",
    "limit": 25
  }'
```

Generate a governed chart:

```bash
curl -s -X POST http://127.0.0.1:8000/chart/generate \
  -H 'Content-Type: application/json' \
  -d '{
    "message": "Plot loan utilization by month",
    "user_role": "technical_user",
    "limit": 12
  }'
```

Use the orchestrated assistant endpoint:

```bash
curl -s -X POST http://127.0.0.1:8000/chat/message \
  -H 'Content-Type: application/json' \
  -d '{
    "message": "Give me average balance by segment",
    "selected_metric_id": "metric.average_deposit_ledger_balance",
    "selected_dimension_ids": ["dimension.customer_segment"],
    "user_role": "technical_user"
  }'
```

## Synthetic Data

The generated database is written to:

```text
data/commercial_banking.duckdb
```

Core tables:

- `dim_customer`
- `dim_account`
- `dim_loan`
- `dim_branch`
- `dim_product`
- `dim_date`
- `fact_deposit_balance_daily`
- `fact_deposit_transaction`
- `fact_loan_balance_monthly`
- `fact_loan_payment`
- `fact_credit_risk_snapshot`
- `fact_relationship_profitability`

Governance tables:

- `metadata_table`
- `metadata_column`
- `metadata_business_term`
- `metadata_metric`
- `metadata_dimension`
- `metadata_join_path`
- `metadata_lineage`
- `metadata_synonym`
- `metadata_access_policy`
- `metadata_search_document`
- `metadata_curation_event`
- `assistant_feedback`

Example governed ambiguity checks:

```sql
SELECT target_type, target_id, confidence
FROM metadata_synonym
WHERE phrase = 'average balance'
ORDER BY confidence DESC;
```

```sql
SELECT target_type, target_id, confidence
FROM metadata_synonym
WHERE phrase = 'segment'
ORDER BY confidence DESC;
```

## Metadata And Search API

Phase 3 and Phase 4 expose the governed catalog and local retrieval index through read-only API endpoints:

- `GET /metadata/tables`
- `GET /metadata/tables/{table_name}`
- `GET /metadata/columns`
- `GET /metadata/joins`
- `GET /metadata/candidates?phrase=average%20balance`
- `GET /metadata/search?query=average%20balance%20by%20segment`
- `GET /metadata/search/documents`
- `GET /glossary`
- `GET /metrics`
- `GET /metrics/{metric_id}`
- `GET /dimensions`
- `GET /dimensions/{dimension_id}`
- `GET /lineage`
- `GET /access-policies`
- `POST /intent/classify`
- `POST /semantic/resolve`
- `POST /sql/generate`
- `POST /query/execute`
- `POST /answer/generate`
- `POST /chart/generate`
- `POST /chat/message`
- `GET /chat/conversations`
- `GET /chat/conversations/{conversation_id}`
- `POST /governance/audit`
- `POST /exports`
- `GET /exports`
- `GET /exports/{export_id}`
- `GET /exports/{export_id}/view`
- `GET /exports/{export_id}/download`
- `POST /admin/curation/business-terms`
- `POST /admin/curation/metrics`
- `POST /admin/curation/dimensions`
- `POST /admin/curation/synonyms`
- `GET /admin/curation/events`
- `POST /feedback`
- `GET /feedback`
- `GET /feedback/quality-summary`

## Admin Curation

Phase 16 adds an admin-only curation workflow for governed semantic assets. Curation requests must use `requested_by: "admin"` and are validated before they update the assistant context:

- Business terms can only point to governed tables and columns.
- Metrics must use a governed base table and governed required columns.
- Dimensions must map to governed table-column pairs.
- Synonyms must point to an existing metric, dimension, or business term.
- Every accepted change writes a `metadata_curation_event` audit record and rebuilds `metadata_search_document`.

Curate a synonym:

```bash
curl -s -X POST http://127.0.0.1:8000/admin/curation/synonyms \
  -H 'Content-Type: application/json' \
  -d '{
    "requested_by": "admin",
    "synonym_id": "synonym.relationship_segment",
    "phrase": "relationship segment",
    "target_type": "dimension",
    "target_id": "dimension.customer_segment",
    "confidence": 0.94,
    "notes": "Maps banker wording to the certified customer segment dimension."
  }'
```

Review curation events:

```bash
curl -s http://127.0.0.1:8000/admin/curation/events
```

## Feedback And Quality Monitoring

Phase 17 adds feedback capture for individual assistant turns. `POST /chat/message` now returns a `turn_id`; feedback records attach to that turn and carry a rating, reason code, optional comment, route, intent, and status.

Submit feedback:

```bash
curl -s -X POST http://127.0.0.1:8000/feedback \
  -H 'Content-Type: application/json' \
  -d '{
    "conversation_id": "conv_...",
    "turn_id": "turn_...",
    "rating": "negative",
    "reason_code": "wrong_metric",
    "comment": "The assistant selected a loan balance metric instead of a deposit balance metric.",
    "user_role": "business_user"
  }'
```

Review quality summary:

```bash
curl -s http://127.0.0.1:8000/feedback/quality-summary
```

Supported reason codes are `helpful`, `wrong_metric`, `wrong_dimension`, `wrong_sql`, `unclear_answer`, `bad_chart`, `missing_context`, and `other`.

## Frontend Chat MVP

The user UI is a chat-first experience served by FastAPI or by the separate frontend dev server:

```text
http://127.0.0.1:8000/
```

It uses the LLM-governed `/chat/message` endpoint and renders:

- Natural assistant follow-up questions when metric, dimension, table, or column meaning is unclear.
- SQL-backed analytical answers with result tables.
- Generated SQL in a collapsible section.
- Source citations for metrics, dimensions, tables, columns, glossary terms, and lineage context.
- Plotly charts when the user asks for a chart.

## Conversation Persistence

Phase 13 stores assistant sessions in the local DuckDB database using:

- `assistant_conversation`
- `assistant_turn`

When `POST /chat/message` is called without a `conversation_id`, the API creates one and returns it in the chat response. Subsequent messages can pass the same `conversation_id` to append turns to the existing session.

List recent sessions:

```bash
curl -s http://127.0.0.1:8000/chat/conversations
```

Load a full session:

```bash
curl -s http://127.0.0.1:8000/chat/conversations/{conversation_id}
```

## Governance Audit

Phase 14 adds an audit report to chat responses and exposes a standalone endpoint:

```bash
curl -s -X POST http://127.0.0.1:8000/governance/audit \
  -H 'Content-Type: application/json' \
  -d '{
    "chat_response": { "...": "payload returned by /chat/message" },
    "user_role": "technical_user"
  }'
```

The audit report summarizes:

- Certified metric and dimension resolution.
- Source tables, table owners, refresh frequency, grain, and column flags.
- Approved join paths used by the governed query plan.
- Column lineage from source systems into governed tables.
- Role-specific access policies and SQL visibility.
- SQL validation, retrieval context, assumptions, warnings, and graph trace.

## Exports And Sharing

Phase 15 stores export snapshots in the local DuckDB database using:

- `assistant_export`

Create an export from a chat response:

```bash
curl -s -X POST http://127.0.0.1:8000/exports \
  -H 'Content-Type: application/json' \
  -d '{
    "chat_response": { "...": "payload returned by /chat/message" },
    "export_format": "html",
    "user_role": "technical_user"
  }'
```

Supported `export_format` values:

- `html`: shareable report with answer, chart, result table, SQL, and audit packet.
- `csv`: result-table export for spreadsheet analysis.
- `json`: full structured response export for audit and integration.
