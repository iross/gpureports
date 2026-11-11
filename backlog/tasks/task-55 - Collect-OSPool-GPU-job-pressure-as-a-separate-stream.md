---
id: TASK-55
title: Collect OSPool GPU job pressure as a separate stream
status: Done
assignee:
  - '@claude'
created_date: '2026-11-10 21:34'
updated_date: '2026-11-11 18:01'
labels:
  - collector
  - ospool
dependencies:
  - TASK-54
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Job pressure is only collected for the CHTC access points (ap2001, ap2002). We also want queue pressure and project attribution for GPU jobs submitted on OSPool access points, kept in their own monthly files so CHTC analyses aren't mixed with OSPool data. OSPool jobs carry ProjectName rather than ChtcProjects, so project attribution needs a column both streams can populate.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 get_job_pressure.py can be run for either the CHTC pool or the OSPool, each with its own collector and hard-coded whitelist of access points
- [x] #2 OSPool runs write ospool_job_pressure_YYYY-MM.parquet files that the existing CHTC readers (load_job_pressure, attribute_claimed_jobs) do not pick up
- [x] #3 read_data can load the OSPool stream for a time window
- [x] #4 Both streams record ProjectName for each job; existing job_pressure files without ProjectName still load and still accept new polls
- [x] #5 Anonymous query access to each whitelisted OSPool AP is confirmed, or the credential needed is documented
- [x] #6 OPERATIONS.md documents the OSPool stream, its CronJob command, and any credential requirement
- [x] #7 Tests cover pool selection, file naming, stream separation, and polling against a pre-ProjectName file
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Probe cm-1.ospool.osg-htc.org for OSPool AP schedds and test anonymous GPU-job queries; pick the whitelist
2. get_job_pressure.py: per-pool config (collector, schedd whitelist, file prefix) selected by --pool; collect ProjectName
3. read_data.py: ProjectName in JOB_PRESSURE_SCHEMA; prefix-aware glob/loader
4. Back-fill missing schema columns when polling against an older file
5. Tests: pool selection, naming, stream separation, pre-ProjectName file
6. OPERATIONS.md + smoke test; lint, format, type check
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Approach: one script, one hard-coded whitelist per pool. get_job_pressure.py gains a POOLS table (collector, schedd whitelist, file prefix) selected by --pool chtc|ospool (default chtc); --schedd still overrides the whitelist.

Access check (2026-11-11, from a smolvm guest): cm-1.ospool.osg-htc.org advertises 32 schedds. Whitelisted the OSPool-operated APs ap23.uc and ap40-ap43.uw .osg-htc.org; all answer anonymous queries, so no credential is needed. ap2001/ap2002 also advertise there and were left out to avoid double-counting with the CHTC stream. About 600 OSPool GPU jobs per poll vs about 40k for CHTC.

Storage: OSPool writes ospool_job_pressure_YYYY-MM.parquet. The prefix is chosen so the CHTC glob job_pressure_*.parquet never matches it; the prefixes live in read_data (CHTC_/OSPOOL_JOB_PRESSURE_PREFIX). load_job_pressure takes a prefix argument; attribute_claimed_jobs stays CHTC-only.

ProjectName: added to JOB_PRESSURE_SCHEMA and collected in both streams (non-empty on every live job in both). Older files lack the column: record_jobs reads with missing_columns=insert so the next poll adds it; load_job_pressure selects * and fills missing columns because DuckDB rejects a column no file has; migrate_job_pressure fills it with null. Intervals opened before the upgrade keep a null ProjectName.

Files: get_job_pressure.py, read_data.py, migrate_job_pressure.py, tests/test_job_pressure.py, OPERATIONS.md, justfile (smolvm-collectors now polls both pools).

Verification: 31 job-pressure tests pass (the new old-file tests fail when either back-fill fix is reverted); full suite 167 passed; prek clean; just smolvm-collectors OK against the live pools.

Not done here: the k8s CronJob for --pool ospool (manifests live outside this repo). ty reports a pre-existing invalid-argument-type on every .cast(JOB_PRESSURE_SCHEMA) because the dict is inferred too narrowly; left for a separate change.
<!-- SECTION:NOTES:END -->
