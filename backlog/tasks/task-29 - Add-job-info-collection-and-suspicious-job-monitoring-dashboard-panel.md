---
id: task-29
title: Add job info collection and suspicious job monitoring dashboard panel
status: Done
assignee:
  - claude
created_date: '2026-03-20 17:46'
updated_date: '2026-03-23 13:53'
labels:
  - dashboard
  - data-collection
  - monitoring
dependencies: []
priority: medium
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Currently we collect GlobalJobId in gpu_state but have no visibility into what those jobs are actually doing. We want to collect job details from the HTCondor schedd at collection time and surface them in the dashboard so admins can spot users running idle/suspicious jobs (e.g. sleep jobs) on open capacity nodes.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 get_gpu_state.py enriches a monthly job_info_YYYY-MM.db at each run, using the htcondor Python bindings to query condor_q for any GlobalJobIds not yet seen
- [x] #2 job_info table stores: GlobalJobId, Cmd, Args, Owner, RequestGPUs, QDate, first_seen
- [x] #3 Only claimed slots are queried; unclaimed slots are skipped
- [x] #4 Schedd queries are batched per schedd hostname (parsed from GlobalJobId) to minimize round trips
- [x] #5 A YAML config file defines suspicious job criteria: cmd patterns (e.g. sleep, bare bash/sh/python) and minimum runtime threshold
- [x] #6 Dashboard has a new panel showing all recent jobs on open capacity claimed slots, joined with job_info
- [x] #7 Jobs matching suspicious criteria (long-running AND idle cmd) are visually highlighted in the panel
- [x] #8 Monthly job_info DBs follow the same naming convention and cross-month query pattern as gpu_state DBs
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add job info collection to get_gpu_state.py: query condor_q for GlobalJobIds on claimed slots, batch per schedd hostname, store to job_info_YYYY-MM.db
2. Create suspicious_jobs.yaml config with cmd patterns and min_runtime_hours
3. Add get_open_capacity_jobs_data() to dashboard/data.py: query gpu_state for claimed open-capacity slots, join with job_info DBs (cross-month), classify suspicious jobs
4. Add /api/jobs endpoint to dashboard/server.py
5. Add Jobs tab to index.html and app.js with table + suspicious row highlighting
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented all 8 ACs:
- get_gpu_state.py: added collect_job_info() that queries condor_q per-schedd for claimed slots' GlobalJobIds and stores to job_info_YYYY-MM.db (GlobalJobId, Cmd, Args, Owner, RequestGPUs, QDate, first_seen)
- suspicious_jobs.yaml: new config with cmd_patterns (sleep, bare bash/sh/python) and min_runtime_hours=1
- dashboard/data.py: added get_open_capacity_jobs_data(), _get_job_info_databases() (cross-month), _load_suspicious_criteria(), _is_suspicious(), _fetch_job_info()
- dashboard/server.py: new /api/jobs endpoint with 5-min cache
- index.html: new 'Open Capacity Jobs' tab with table (machine, gpu, owner, cmd, args, runtime, jobid)
- app.js: renderJobs() fetches /api/jobs, highlights suspicious rows in red with flag icon
- style.css: jobs-container, jobs-table, .job-suspicious styles
Only claimed non-backfill non-prioritized slots are shown; cross-month job_info DB lookup handles month transitions
<!-- SECTION:NOTES:END -->
