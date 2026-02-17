# fabric-solution-int

[![Run Fabric Pipeline](https://github.com/UnifiedEducation/fabric-solution-int/actions/workflows/run-fabric-pipeline-cli.yml/badge.svg)](https://github.com/UnifiedEducation/fabric-solution-int/actions/workflows/run-fabric-pipeline-cli.yml)

The Intermediate real-world Fabric Data Platform built at [Fabric Dojo](https://skool.com/fabricdojo).

_'Intermediate'_ - meaning it's a reasonable solution for those transitioning from Power BI-centric architectures and teams new to the world of Data Platforms. If you already have extensive Data Platform building experience, the [Advanced-level project](https://github.com/UnifiedEducation/fabric-solution-adv) will be best for you.

This solution ingests YouTube API data through a Bronze/Silver/Gold lakehouse architecture, with PySpark notebooks, Great Expectations validation, and GitHub Actions orchestration.

## Repository Structure

```
solution/
  datastores/          Lakehouses (Bronze, Silver, Gold, Admin)
  processing/
    notebooks/         PySpark notebooks (ingest, load, clean, model, validate)
    pipelines/         Data pipeline orchestration
    env_spark_gx/      Spark environment with Great Expectations
    vl-int-variables/  Variable Library (DEV/TEST/PROD configs)
  consumption/         Consumption layer (reports, semantic models)
docs/                  Solution documentation, architecture diagram
.github/workflows/     GitHub Actions for scheduled pipeline runs
```

## Quick Start

1. Clone the repo and copy `.env.example` to `.env`, filling in your Azure credentials
2. Connect your DEV workspaces to the repo via Fabric Git Integration
3. See [PRJ002 - Version Control & Deployment](https://www.skool.com/fabricdojo/classroom/41bb7437?md=2208a7c8b4634672a9eea982175384e2) for the full walkthrough

## Video Walkthroughs

This repo is the companion code for the Intermediate Real-World Project series on [Fabric Dojo](https://skool.com/fabricdojo) (premium). Each module includes a design lesson and a sprint build-along:

| Module | Topic                           | Repo Area                           | Links                                                                                                                                                                                                   |
| ------ | ------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PRJ001 | Capacity & Workspace Design     | `docs/development/`                 | [Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=ed30c42142b64304b015e75ed075e4a0) / [Sprint 1](https://www.skool.com/fabricdojo/classroom/41bb7437?md=b27c3351ec26416a9d9cc76eaaecee3f) |
| PRJ002 | Version Control & Deployment    | `.github/`, `vl-int-variables/`     | [Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=2208a7c8b4634672a9eea982175384e2) / [Sprint 2](https://www.skool.com/fabricdojo/classroom/41bb7437?md=4fb344e4ed75417b9fa52f5dcc09a0a2) |
| PRJ003 | Getting Data into Fabric        | `notebooks/nb-int-0-ingest-*`       | [Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=22e7ee1b1b8446b88f59b9cb78fb35db) / [Sprint 3](https://www.skool.com/fabricdojo/classroom/41bb7437?md=d3218e80654e469daf8c4f903f9fb60b) |
| PRJ004 | Lakehouse Development           | `datastores/`                       | [Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=6e0c608ff85c48e7a3234d12e8186bfa) / [Sprint 4](https://www.skool.com/fabricdojo/classroom/41bb7437?md=923e221d9cf549a2bbd5314c6ce66478) |
| PRJ005 | Data Transformation & Movement  | `notebooks/nb-int-1,2,3-*`          | [Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=8e327eb0345e43b097be0a785eff312c) / [Sprint 5](https://www.skool.com/fabricdojo/classroom/41bb7437?md=0c0f7072cab9458496e5bc63ed4469ac) |
| PRJ006 | Orchestration & Data Validation | `pipelines/`, `nb-int-4-validate-*` | [Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=4194b3d17e494e98909931a8961210af) / [Sprint 6](https://www.skool.com/fabricdojo/classroom/41bb7437?md=bea2db31262f468fb6eef77821a3491c) |
| PRJ007 | Prepare for GO-LIVE             | Full solution                       | [Design](https://www.skool.com/fabricdojo/classroom/41bb7437?md=1dbee0f13a184dcc8ef4ce9de62905bb) / [Sprint 7](https://www.skool.com/fabricdojo/classroom/41bb7437?md=4c5280f224414c339cd9bde220402175) |
