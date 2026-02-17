# Solution Documentation

This page covers the key aspects of the Intermediate Fabric Data Platform. For full video walkthroughs, deep-dive explanations, and build-along sprints, see the [Fabric Dojo](https://skool.com/fabricdojo) community (premium).

## Architecture

![High-level architecture](int-doc-high_level_architecture.png)

The solution uses a 9-workspace layout across 3 areas (Datastores, Processing, Consumption) and 3 environments (DEV, TEST, PROD), with dedicated Fabric Capacities for cost isolation.

| Topic | Training |
|-------|----------|
| Capacity & workspace design | [PRJ001 Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=ed30c42142b64304b015e75ed075e4a0) / [Sprint 1](https://www.skool.com/fabricdojo/classroom/41bb7437?md=b27c3351ec26416a9d9cc76eaaecee3f) |

## Version Control & Deployment

Git integration connects each workspace area to a folder in this repo (`solution/datastores/`, `solution/processing/`, `solution/consumption/`). A Variable Library (`vl-int-variables`) parameterises all environment-specific values (workspace IDs, notebook IDs, lakehouse names) with value sets for DEV, TEST, and PROD.

| Topic | Training |
|-------|----------|
| Git integration & deployment pipelines | [PRJ002 Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=2208a7c8b4634672a9eea982175384e2) / [Sprint 2](https://www.skool.com/fabricdojo/classroom/41bb7437?md=4fb344e4ed75417b9fa52f5dcc09a0a2) |

## Data Ingestion

YouTube Data API v3 is the source. The ingest notebook (`nb-int-0-ingest-youtube`) pulls channel info, playlist items, and video statistics, writing raw JSON to the Bronze Lakehouse Files area. API keys are stored in Azure Key Vault.

| Topic | Training |
|-------|----------|
| Getting data into Fabric | [PRJ003 Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=22e7ee1b1b8446b88f59b9cb78fb35db) / [Sprint 3](https://www.skool.com/fabricdojo/classroom/41bb7437?md=d3218e80654e469daf8c4f903f9fb60b) |

## Lakehouse Development

Four lakehouses follow the medallion architecture: Bronze (raw), Silver (cleaned), Gold (modelled), and Admin (logging/quality). Schemas and tables are created by the `nb-int-lhcreate-all` setup notebook using a metadata-driven approach.

| Topic | Training |
|-------|----------|
| Lakehouse structure & medallion pattern | [PRJ004 Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=6e0c608ff85c48e7a3234d12e8186bfa) / [Sprint 4](https://www.skool.com/fabricdojo/classroom/41bb7437?md=923e221d9cf549a2bbd5314c6ce66478) |

## Data Transformation

PySpark notebooks move data through the medallion layers: Load (JSON to Bronze tables), Clean (null filtering, deduplication via Window functions to Silver), and Model (dimensional modelling with surrogate key management to Gold).

| Topic | Training |
|-------|----------|
| PySpark transformations | [PRJ005 Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=8e327eb0345e43b097be0a785eff312c) / [Sprint 5](https://www.skool.com/fabricdojo/classroom/41bb7437?md=0c0f7072cab9458496e5bc63ed4469ac) |

## Orchestration & Data Validation

Two pipelines orchestrate execution: `pp-int-run-youtube` runs the 5 data notebooks sequentially, and `pp-int-run-w-logging` wraps that with execution logging. Great Expectations validates the Gold layer tables, with results stored in the Admin Lakehouse. GitHub Actions triggers the pipeline daily, handling Fabric Capacity pause/resume automatically.

| Topic | Training |
|-------|----------|
| Pipeline orchestration & GX validation | [PRJ006 Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=4194b3d17e494e98909931a8961210af) / [Sprint 6](https://www.skool.com/fabricdojo/classroom/41bb7437?md=bea2db31262f468fb6eef77821a3491c) |

## Go-Live

Final testing, production deployment, and capacity automation.

| Topic | Training |
|-------|----------|
| Production readiness | [PRJ007 Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=1dbee0f13a184dcc8ef4ce9de62905bb) / [Sprint 7](https://www.skool.com/fabricdojo/classroom/41bb7437?md=4c5280f224414c339cd9bde220402175) |

## Naming Convention

All Fabric items follow the pattern: `[type]-[project]-[purpose]`

| Prefix | Item Type |
|--------|-----------|
| `nb-` | Notebook |
| `pp-` | Data Pipeline |
| `lh_` | Lakehouse |
| `vl-` | Variable Library |
| `env_` | Spark Environment |

Project code: `int` (Intermediate). Environments: `dev`, `test`, `prod`.
