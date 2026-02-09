# Adaptive Ingestion & Hybrid Backend Placement

## Project Description
This project implements an autonomous data ingestion system that dynamically routes JSON records to SQL or MongoDB based on field behavior.

- **Step 1:** Ingestion from API, normalization, statistics analysis, classification.
- **Step 2:** Routing to SQL (MySQL) or MongoDB based on classifier.
- **Step 3:** Timestamps, uniqueness handling, metadata persistence.

## How to Run
1. Start the dummy JSON API:
```bash
uvicorn simulation_code:app --reload --port 8000
