# Adaptive Ingestion & Hybrid Backend Placement

## Project Description
This project implements an autonomous data ingestion system that dynamically routes JSON records to SQL or MongoDB based on field behavior.

## Setup & Installation

### Create a Virtual Environment

```bash
python -m venv venv
```

### Activate the Virtual Environment

**Windows (Git Bash):**
```bash
source venv/Scripts/activate
```

**Windows (CMD):**
```bash
venv\Scripts\activate
```

**Mac/Linux:**
```bash
source venv/bin/activate
```

### Deactivate Virtual Environment

```bash
deactivate
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Running the Application

Start the dummy JSON API server:

```bash
uvicorn simulation_code:app --reload --port 8000
```

The API will run at:

```
http://127.0.0.1:8000
```

## Key Features
- Streaming ingestion from an SSE/HTTP source (see `simulation_code.py`).
- Field-level analysis: type detection, semantic signals, uniqueness and stability (`analyzer.py`, `classifier.py`).
- Type-drift detection and quarantine logic (`drift_detector.py`).
- Hybrid storage with bi-temporal timestamps and routing to MySQL + MongoDB (`storage_manager.py`).
- Enhanced metadata storage and reporting (`metadata_manager.py`, `analyze_metadata.py`).

## Requirements
- Python 3.9+ (or compatible)
- See `requirements.txt` for Python dependencies. Install with:

```bash
python -m pip install -r requirements.txt
```

## Environment (Databases)
- The pipeline expects access to a MySQL server and a MongoDB server. You can configure connection details using environment variables or a `.env` file. Supported vars (defaults shown):

- `MYSQL_HOST` (default: `localhost`)
- `MYSQL_USER` (default: `root`)
- `MYSQL_PASSWORD` (default: `devil`)
- `MYSQL_DATABASE` (default: `streaming_db`)
- `MONGO_HOST` (default: `localhost`)
- `MONGO_PORT` (default: `27017`)
- `MONGO_DATABASE` (default: `streaming_db`)
- `MONGO_COLLECTION` (default: `logs`)

Create a `.env` file in the project root if you want to override these values.

## Quickstart  Local demo
1. Install requirements (see above).
2. Start the simulation SSE API (recommended port 8001):

```bash
# from project root
uvicorn simulation_code:app --reload --port 8001
```

3. In a separate terminal, run the pipeline which will connect to the simulation, analyze records, update metadata, and store into backends:

```bash
python main.py
```

Notes:
- `ingestion.py` fetches events from the simulation at `http://127.0.0.1:8001` by default.
- `main.py` will attempt to create SQL schema from the metadata and insert records into MySQL and MongoDB. Ensure both DBs are reachable before running.

## Analyze Metadata & Reports
- Generate a human-readable metadata report or export a detailed JSON report:

```bash
# Show summary report
python analyze_metadata.py

# Export detailed JSON report
python analyze_metadata.py export

# Show specific field detail
python analyze_metadata.py <field_name>
```

## Important Files
- `main.py` — Pipeline orchestrator: ingestion → analysis → classification → routing → storage.
- `simulation_code.py` — FastAPI-based SSE generator (used for local testing).
- `ingestion.py` — SSE/http client that yields records to the pipeline.
- `normalize.py` — Record normalization hook (currently passthrough).
- `analyzer.py` — Field statistics, uniqueness, type samples and stability calculations.
- `classifier.py` — Heuristics for placement decisions (SQL vs MongoDB).
- `drift_detector.py` — Sliding-window type-drift detection and quarantine logic.
- `storage_manager.py` — Connects to MySQL + MongoDB and performs inserts; includes bi-temporal examples.
- `metadata_manager.py` — Maintains enhanced field metadata in `metadata.json`.
- `analyze_metadata.py` — Reporting and metadata export utilities.
- `metadata.json` — Generated enhanced metadata (updated by `main.py`).

## Troubleshooting
- If ingestion can't reach the simulation server, verify the simulation is running on the expected port and the `BASE_URL` in `ingestion.py` matches.
- If DB connections fail, check credentials and ensure MySQL and MongoDB are running and accessible.
- For local MySQL testing, consider using Docker:

```bash
docker run --name local-mysql -e MYSQL_ROOT_PASSWORD=devil -e MYSQL_DATABASE=streaming_db -p 3306:3306 -d mysql:8
docker run --name local-mongo -p 27017:27017 -d mongo:6
```
