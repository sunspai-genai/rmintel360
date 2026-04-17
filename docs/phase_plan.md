# Phase Plan

## Phase 0: Backend Foundation

Deliverables:

- FastAPI application shell.
- Runtime configuration.
- DuckDB connection utility.
- Health endpoint.
- Data profile endpoint for generated tables.

## Phase 1: Synthetic Commercial Banking Data

Deliverables:

- Deterministic data generator.
- Commercial banking dimension tables.
- Deposit, loan, risk, transaction, and profitability fact tables.
- Local DuckDB database at `data/commercial_banking.duckdb`.

## Phase 2: Governed Metadata Catalog

Deliverables:

- Governed table catalog.
- Governed column catalog.
- Business glossary terms.
- Certified metrics.
- Certified dimensions.
- Approved join paths.
- Lineage records.
- Synonym mappings for semantic resolution.
- Role-aware access policy seed records.

Status: complete.

## Phase 3: Metadata And Glossary APIs

Deliverables:

- Catalog service over governed metadata tables.
- Table, column, join, glossary, metric, dimension, lineage, candidate, and access policy endpoints.
- Focused governed candidate lookup for clarification flows.
- API tests for governed ambiguity behavior.

Status: complete.

## Phase 4: Governed Metadata Search

Deliverables:

- Search document builder over glossary, metrics, dimensions, tables, columns, joins, and lineage.
- Local sparse vector representation stored in `metadata_search_document`.
- Search service and `/metadata/search` API endpoint.
- Retrieval ranking tuned for analytical, table-discovery, and lineage questions.
- Tests proving retrieval returns governed metric, dimension, and lineage context.

Status: complete.

## Phase 5: Intent Classification

Deliverables:

- Deterministic classifier for supported MVP intents.
- Routing contract for information flow, governed analytics flow, clarification flow, and unsupported flow.
- Retrieval-backed hints for candidate metrics, dimensions, tables, and lineage.
- `POST /intent/classify` endpoint.
- Tests for definition, metadata discovery, lineage, analytics, chart, clarification, and unsupported requests.

Status: complete.

## Phase 6: Semantic Resolution And Clarification

Deliverables:

- Governed semantic resolver for analytical and chart requests.
- Clarification options for ambiguous metrics and dimensions.
- Explicit blocking of SQL generation when semantics are unresolved.
- Query-plan skeleton with metric, dimensions, approved join paths, filters, and assumptions.
- `POST /semantic/resolve` endpoint.
- Tests for ambiguous and resolved analytical requests.

Status: complete.

## Phase 7: Governed SQL Generation And Validation

Deliverables:

- Deterministic SQL generation from resolved governed query plans.
- Blocking behavior when semantic resolution needs clarification or identifies non-SQL requests.
- SELECT-only validation with blocked write-operation tokens.
- Metadata validation for certified metrics, certified dimensions, certified join paths, and unrestricted columns.
- DuckDB compile validation before any execution path is introduced.
- Role-aware SQL visibility, hiding generated SQL text from default business users.
- `POST /sql/generate` endpoint.
- Tests for ambiguity blocking, generated aggregate SQL, trend SQL, business-user SQL hiding, and the API contract.

Status: complete.

## Phase 8: LangGraph Orchestration

Deliverables:

- LangGraph state machine for governed assistant routing.
- Classification node for intent and route selection.
- Information node for business definitions, metadata, table discovery, and lineage answers.
- Analytics node that invokes semantic resolution and governed SQL generation.
- Clarification node that keeps unresolved governed choices out of SQL generation.
- Unsupported node for out-of-scope requests.
- Graph trace output for explainable MVP routing.
- `POST /chat/message` endpoint.
- Tests for information, clarification, SQL generation, chart SQL, unsupported, and API contract paths.

Status: complete.

## Phase 9: Governed SQL Execution And Tabular Results

Deliverables:

- Read-only DuckDB execution service for generated and validated governed SQL.
- Blocking behavior when semantic resolution needs clarification or SQL validation fails.
- Bounded result retrieval with request-level row limits.
- JSON-safe tabular result shape with columns, rows, row count, limit, and truncation flag.
- Execution timing and concise result summary.
- Business-user SQL hiding preserved while still allowing governed result retrieval.
- `POST /query/execute` endpoint.
- Chat orchestration updated to execute resolved analytics by default, with `execute_sql=false` available for generate-only mode.
- Tests for ambiguity blocking, technical execution, business-user SQL hiding, execution API contract, and chat execution paths.

Status: complete.

## Phase 10: Analytical Answer Generation

Deliverables:

- Governed analytical answer generator over executed result tables.
- Narrative answer, key points, and result overview for grouped and trend results.
- Highest and lowest metric summaries for grouped results.
- First-to-latest change summary for monthly trend results.
- Evidence-preserving response that includes execution result, SQL validation, result table, assumptions, and warnings.
- Blocking behavior for ambiguous, unsupported, invalid, or failed requests.
- `POST /answer/generate` endpoint.
- Chat orchestration updated to return analytical answers by default after execution.
- Tests for ambiguity blocking, grouped balance answers, monthly trend answers, answer API contract, and chat answer flow.

Status: complete.

## Phase 11: Governed Chart Generation

Deliverables:

- Governed chart generator over analytical answer and execution results.
- Plotly JSON chart specification for frontend rendering.
- Automatic line chart selection for month/date trend results.
- Automatic bar chart selection for grouped categorical results.
- Blocking behavior for unresolved, unsupported, invalid, or empty-result requests.
- `POST /chart/generate` endpoint.
- Chat orchestration updated to include chart specs for chart-intent requests.
- Tests for ambiguity blocking, line charts, bar charts, chart API contract, and chat chart flow.

Status: complete.

## Phase 12: Frontend Chat MVP

Deliverables:

- FastAPI-served no-build frontend application.
- Chat composer connected to `POST /chat/message`.
- Business and technical role controls with execution and row-limit controls.
- Governed clarification option rendering and resubmission with selected metric and dimension IDs.
- Analytical answer, key point, result table, SQL, assumption, warning, and graph-trace rendering.
- Plotly chart rendering from backend `chart_spec` payloads.
- Static frontend tests for index and JavaScript asset serving.

Status: complete.

## Phase 13: Conversation Persistence And Session History

Deliverables:

- Local DuckDB conversation persistence tables for sessions and turns.
- Automatic conversation ID creation on `POST /chat/message`.
- Chat turn persistence with request payload, response payload, status, intent, route, SQL, chart type, and row-count summary.
- `GET /chat/conversations` endpoint for recent session history.
- `GET /chat/conversations/{conversation_id}` endpoint for full turn replay.
- Frontend session rail with new chat, recent sessions, restore, and continuation behavior.
- Tests for persisted turns, multi-turn continuation, and recent conversation listing.

Status: complete.

## Phase 14: Governance Audit Panel

Deliverables:

- Governance audit service over chat responses and governed metadata.
- Audit report with certification status, source tables, approved joins, lineage, access policies, SQL validation, retrieval evidence, assumptions, warnings, and graph trace.
- `POST /governance/audit` endpoint for rebuilding audit packets from chat responses.
- Chat response integration so every assistant turn carries an `audit_report`.
- Frontend audit cards and full audit packet expander on assistant responses.
- Tests for resolved-query audit, metadata-only clarification audit, and audit endpoint behavior.

Status: complete.

## Phase 15: Export And Share Analytical Results

Deliverables:

- Local DuckDB export snapshot table for persisted export artifacts.
- HTML report export with answer, key points, chart, result table, SQL, and governance audit packet.
- CSV export for tabular result tables.
- JSON export for structured response and audit payloads.
- `POST /exports`, `GET /exports`, `GET /exports/{export_id}`, `/view`, and `/download` endpoints.
- Frontend export actions for HTML, CSV, JSON, and share-link creation on assistant responses.
- Tests for HTML report creation, CSV download, and export listing by conversation.

Status: complete.

## Phase 16: Admin Curation Workflow

Deliverables:

- Admin-only curation service for business glossary terms, certified metrics, certified dimensions, and synonyms.
- Validation that curated metrics, dimensions, terms, and synonyms reference governed tables, columns, and target assets.
- Curation event table with before/after payloads, requested user, action, asset type, asset ID, status, and notes.
- Automatic metadata search-index refresh after accepted curation changes.
- `POST /admin/curation/business-terms`, `/metrics`, `/dimensions`, and `/synonyms` endpoints.
- `GET /admin/curation/events` endpoint for recent curation history.
- Frontend admin curation panel with sample payloads, apply action, and event history.
- Tests for non-admin rejection, invalid metadata rejection, successful curation, search refresh, and event listing.

Status: complete.

## Phase 17: Feedback And Quality Monitoring

Deliverables:

- Feedback persistence table for assistant turn ratings, issue reasons, comments, route, intent, and status.
- Chat responses now expose `turn_id` so feedback can attach to the exact assistant answer.
- `POST /feedback` endpoint for positive, negative, and neutral feedback.
- `GET /feedback` endpoint for recent feedback filtered by conversation, turn, or rating.
- `GET /feedback/quality-summary` endpoint with positive rate, issue rate, top issue reasons, route quality, and recent feedback.
- Frontend per-answer feedback actions for good/issue signals.
- Frontend quality summary in the admin panel.
- Tests for turn IDs, feedback capture, feedback listing, quality summaries, unknown turns, and mismatched conversation IDs.

Status: complete.

## Phase 18: LLM-Driven Chatbot Refactor

Deliverables:

- AWS Bedrock Titan client abstraction with local fallback for development and tests.
- Metadata retrieval package that gathers governed metrics, dimensions, tables, columns, business terms, lineage, and citations before LLM decisions.
- LLM-first LangGraph chat orchestration for intent, semantic asset selection, clarification, SQL-backed analytics, answer generation, and chart routing.
- Conversational clarification flow that lets the user answer naturally in the next chat turn.
- Chat responses include generated SQL and source citations.
- Structured LLM intent contract with `response_mode`, `allow_sql`, and `allow_chart` policy enforcement.
- Simplified frontend focused on a single chatbot experience.
- SQL validation hardened to compare generated SQL references against governed allowed columns and restricted flags.
- Admin metric curation validates calculation expressions against governed required columns.

Status: complete.

## Next Phases

1. Evaluation set generation for governed natural-language to SQL resolution.
2. Production hardening for authentication, migrations, deployment, and observability.
