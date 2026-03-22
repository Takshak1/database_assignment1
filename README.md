# Adaptive Object-Oriented Database Framework

This project builds an autonomous JSON data pipeline that:
- ingests records,
- analyzes field behavior,
- classifies each field to SQL, MongoDB, or Buffer,
- generates storage and CRUD plans,
- executes hybrid CRUD,
- aggregates SQL + Mongo outputs into one JSON response.

## Flow
![Request Flow](docs/request-flow.svg)

Flow: Ingest API -> Schema Registry -> Analyzer/Classifier -> Strategy -> CRUD Executor -> MySQL/MongoDB -> Result Aggregator -> Unified JSON response.

## Step Coverage (1 to 11)
1. Ingestion and normalization
2. SQL/Mongo routing
3. Metadata persistence
4. Structure and pipeline analysis
5. SQL normalization blueprint
6. Mongo embed/reference strategy
7. Storage strategy generation
8. Ingest endpoint and buffer promotion
9. CRUD query plan generation
10. Hybrid CRUD execution
11. Result aggregation

## Project Structure
Core modules and their roles:

- `main.py`: orchestrates ingestion pipeline.
- `schema_registry_api.py`: FastAPI endpoints.
- `schema_registry.py`: schema + metadata persistence.
- `schema_analyzer.py`: JSON structure analysis.
- `classifier.py`, `classification_engine.py`: field routing logic.
- `sql_normalization_engine.py`: relational table blueprint.
- `mongo_strategy_engine.py`: document strategy.
- `storage_strategy_generator.py`: SQL/Mongo physical strategy.
- `crud_query_engine.py`: SQL/Mongo read plan synthesis.
- `crud_executor.py`: execution engine.
- `result_aggregator.py`: SQL + Mongo merge layer.
- `buffer_queue.py`, `buffer_promoter.py`: delayed promotion for uncertain fields.
- `metadata_manager.py`: metadata tracking.

Supporting directories/files:
- `tests/`: requirement-wise and workflow tests.
- `docs/request-flow.svg`: visual request flow used in this README.
- `metadata.json`: generated metadata output.
- `requirements.txt`: Python dependency list.

## API Endpoints
- `POST /register_schema`
- `GET /schemas`
- `GET /schemas/{schema_id}`
- `POST /schemas/{schema_id}/query_plan`
- `POST /schemas/{schema_id}/crud`
- `POST /ingest/{schema_id}`

## Steps To Execute The Code
1. Create and activate virtual environment.
2. Install dependencies:
```bash
pip install -r requirements.txt
```
3. Verify setup:
```bash
python verify_setup.py
```
4. Start schema registry API:
```bash
uvicorn schema_registry_api:app --reload --port 8002
```
5. Run tests (recommended before pipeline run):
```bash
python -m pytest
```
6. Run pipeline (optional stream run):
```bash
python main.py
```

Optional: run only requirement-specific tests
```bash
python -m pytest tests/test_requirement1_normalization.py
python -m pytest tests/test_requirement2_table_keys.py
python -m pytest tests/test_requirement3_mongo_strategy.py
python -m pytest tests/test_requirement4_metadata_system.py
python -m pytest tests/test_requirement5_crud_generation.py
python -m pytest tests/test_requirement6_performance.py
python -m pytest tests/test_requirement7_sources.py
```

## Environment Variables
Defaults:
- `MYSQL_HOST=localhost`
- `MYSQL_USER=root`
- `MYSQL_PASSWORD=devil`
- `MYSQL_DATABASE=streaming_db`
- `MONGO_HOST=localhost`
- `MONGO_PORT=27017`
- `MONGO_DATABASE=streaming_db`
- `MONGO_COLLECTION=logs`

Use `.env` to override.

## Useful Commands
```bash
# health checks
python verify_setup.py
python db_connectivity_check.py

# tests
python -m pytest

# requirement-wise tests
python -m pytest tests/test_requirement1_normalization.py
python -m pytest tests/test_requirement2_table_keys.py
python -m pytest tests/test_requirement3_mongo_strategy.py
python -m pytest tests/test_requirement4_metadata_system.py
python -m pytest tests/test_requirement5_crud_generation.py
python -m pytest tests/test_requirement6_performance.py
python -m pytest tests/test_requirement7_sources.py
```

## Metadata and Reports
- `metadata.json`: generated metadata store.
- `analyze_metadata.py`: metadata summary/export utility.

Run:
```bash
python analyze_metadata.py
python analyze_metadata.py export
python analyze_metadata.py <field_name>
```

## Troubleshooting
- If setup fails: run `verify_setup.py` and `db_connectivity_check.py`.
- If ingestion fails: ensure simulation service is running on expected host/port.
- If DB failures occur: verify MySQL/Mongo status and `.env` credentials.

## Tests and Current Status
Requirement-focused tests are present under `tests/` and include normalization, key logic, Mongo strategy, metadata system, CRUD generation, performance, and sources coverage.
