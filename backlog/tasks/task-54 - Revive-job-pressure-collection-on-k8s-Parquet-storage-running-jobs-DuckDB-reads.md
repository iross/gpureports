---
id: TASK-54
title: >-
  Revive job pressure collection on k8s: Parquet storage, running jobs, DuckDB
  reads
status: In Progress
assignee:
  - '@claude'
created_date: '2026-09-29 15:03'
updated_date: '2026-09-30 17:10'
labels:
  - reporting
  - collector
  - duckdb
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Job pressure data (per-job owner, GPU request, queue state and the ChtcProjects group) is only collected by a cron on a baremetal host into SQLite, is not part of the k8s production setup, and covers only two of the schedds and only idle jobs. It is the sole source of job-level group membership (needed for per-group reports of open-capacity usage) and of queue pressure. Bring it into the k8s data flow alongside gpu_state, in Parquet, with enough coverage to attribute running jobs to groups.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 get_job_pressure.py runs as a k8s CronJob writing to the shared data volume, and OPERATIONS.md describes what actually runs where
- [x] #2 Job pressure is stored as monthly Parquet files written atomically so concurrent readers never see partial data; the SQLite writer is removed
- [x] #3 Collection covers idle and running GPU jobs on ap2001 and ap2002 (the agreed schedd scope) and records ChtcProjects for each
- [ ] #4 Existing SQLite job_pressure history, including the unmigrated old-schema May file, is converted to Parquet with no loss of idle-interval semantics
- [x] #5 read_data provides a DuckDB-backed loader that returns job pressure for a time window and joins it to gpu_state on GlobalJobId; the sqlite3 code in host_report.py is replaced by it
- [x] #6 Measured against gpu_state for a full month, at least 91% of distinct open-capacity claimed jobs have a project attribution, and the actual rate is reported
- [x] #7 Tests cover interval open/extend/close, month boundaries, atomic writes, and the GlobalJobId join
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Research findings:

Current state
- get_job_pressure.py still exists, imports htcondor2 (same as collector.py), and is COPYed into the k8s Dockerfile image, but the k8s deployment does not run it. It currently runs from a cron on a baremetal host (the justfile's sync-dbs recipe scps job_pressure/gpu_state DBs from deepdivesubmit2000.chtc.wisc.edu:/home/iaross/gpureports). OPERATIONS.md wrongly describes it as a k8s CronJob 'intended every 5 min'; the script itself documents a 30-minute cron (_STALE_SECONDS = 5400 = 3x30min). The k8s manifests live outside this repo; the cluster API was not reachable during research, so nothing about the cluster was verified.
- Collector cutover (commit acaa8d1, 2026-05-15) moved only gpu_state to Parquet. job_pressure stayed SQLite (interval schema: one row per idle period, first_seen/last_seen INTEGER). job_info collection died with the deletion of get_gpu_state.py (TASK-39/TASK-49.2) and is not part of this task.
- Local copies (synced 2026-07-03, so staleness is a sync artifact, not evidence collection stopped): job_pressure_2026-05.db is 2.2GB, OLD per-snapshot schema, 10.2M rows, timestamps only 2026-05-05..05-14 (never migrated by migrate_job_pressure.py; data gap for the rest of May in this copy); 2026-06 is 358,494 interval rows / 307,616 distinct jobs (~62MB SQLite; would be a few MB as Parquet); 2026-07 covers Jul 1-3 only.
- Only consumer today is scripts/host_report.py (raw sqlite3, hand-rolled 15-min bucket expansion in Python). Nothing in the canonical polars pipeline, dashboard, or email reports reads it.

Why it matters (data value)
- ChtcProjects is the only place a job's group is recorded (job-level groups like Biochemistry_Raman exist only here, not in gpu_state.PrioritizedProjects). gpu_state has RemoteOwner and GlobalJobId per claimed slot, so group attribution is a join on GlobalJobId. Measured on June 2026 with DuckDB: 91% of distinct open-capacity claimed jobs (34,650 of 37,894) match job_pressure, in 0.08s. The 9% miss are jobs never observed idle (start before the next poll) and jobs on schedds not polled.
- Coverage gaps in get_job_pressure.py: (1) TARGET_APS hardcoded to ap2001/ap2002, but gpu_state (2026-08) also has running jobs from grid-submitter.icecube.wisc.edu (~224k rows), osggrid01.hep.wisc.edu, scarcity-ap-1.glbrc.org, login01/login04.hep.wisc.edu, wright-ap4000, oconnor-ap4000 -- invisible to job_pressure; (2) CONSTRAINT = 'RequestGPUs >= 1 && JobStatus == 1' records idle jobs only, so a job that starts running within one poll interval has no ChtcProjects record. Extending to running jobs (JobStatus == 2) fixes the attribution miss. NOT verified: that running-job ads expose ChtcProjects and RequestGPUs the same way idle ones do, and how many ads that returns per poll; must be checked against a live schedd (htcondor2 is not installed in the local venv, so this needs the collector image or the baremetal host).

Storage/DuckDB decision (made with requester): Parquet, queried with DuckDB.
- Prior evidence (backlog/decisions/task-30-duckdb-parquet-evaluation.md): DuckDB ATTACHing SQLite was 2.3x slower than SQLite+polars on gpu_state, but DuckDB on Parquet was ~17x faster; so DuckDB is the query engine, not a storage format. A DuckDB database file is not suitable as the store: one read-write process and no concurrent readers, which conflicts with the collector + read-only dashboard/emailer sharing the RWO PVC (backlog/decisions/task-48-dashboard-pvc-concurrency.md). SQLite over Ceph RBD with a concurrent reader is also the risk that Parquet-with-atomic-rename avoids.
- Writer pattern to reuse: collector.py _write_parquet_atomic() (read-concat, write dot-prefixed .tmp, os.replace) -- readers glob job_pressure_*.parquet and never see the temp file. The interval model needs updating last_seen on existing rows, so each poll reads the month's file, updates/appends, and atomically rewrites it; ~360k rows/month makes that cheap. Schema for running jobs is a design decision (see plan step 2).
- DRAFT-2 (evaluate DuckDB SQLite-attach for job_pressure) is superseded by this decision and can be dropped once this lands.

Plan:
1. Spike on a live schedd (collector image or baremetal host): confirm JobStatus == 2 ads carry ChtcProjects/Owner/RequestGPUs/GlobalJobId, count ads per poll for all schedds, and confirm schedd discovery via locateAll returns the non-CHTC APs and that the query works against them (auth/permissions). Record results in Implementation Notes.
2. Decide the Parquet schema: keep the idle-interval table (first_seen/last_seen per idle period) and add running-job records (e.g. one row per job with first/last seen running and project), or a single per-job table with status intervals. Must keep everything host_report.py's contention analysis needs (idle intervals) and add project attribution by GlobalJobId for running jobs.
3. Port get_job_pressure.py to write job_pressure_YYYY-MM.parquet atomically; remove the SQLite writer; drop the hardcoded TARGET_APS in favor of all schedds (or a configurable list); keep the stale-gap logic parameterized (not hardcoded 5400).
4. One-time migration of existing SQLite files (migrate_job_pressure.py handles old->interval schema; extend/replace it to emit Parquet, including the 10M-row 2026-05 old-schema file) and a backfill decision for the baremetal-host history.
5. Read side: add job-pressure loading to read_data.py using DuckDB (scan parquet, interval-overlap and GlobalJobId join) returning a polars frame; replace the sqlite3 code in scripts/host_report.py (or whatever replaces it under TASK-53) so there is one reader.
6. Deployment: add get_job_pressure.py CronJob to the k8s manifests (outside this repo -- coordinate with wherever they live), shared gpu-stats-data-pvc, match the collector's schedule; retire the baremetal cron once k8s has been writing for a full month boundary. Fix OPERATIONS.md (currently claims k8s runs it), README, Dockerfile if module paths change (TASK-52 is relocating files -- keep the entrypoint filename stable).
7. Tests: interval open/extend/close and month-boundary behavior on the Parquet writer with a fake schedd result; atomic-write safety (no partial file visible to readers); reader interval-overlap correctness; GlobalJobId join with missing/duplicate rows.
8. Smoke test: run the writer against a live/simulated poll twice, verify intervals extend, then run the DuckDB join against gpu_state and report the match rate (target: at least the 91% measured for June, expected higher with running jobs and all schedds).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Done on branch task-54-job-pressure-parquet (PR #30):

- Spike (live, via linux/amd64 container with htcondor2, anonymous auth): running-job ads (JobStatus == 2) carry ChtcProjects, Owner, RequestGPUs and GlobalJobId exactly like idle ads (ap2001: 104/104 running with a project; ap2002: 189/189). ~38k idle + ~295 running GPU jobs per poll across ap2001/ap2002. Anonymous queries are DENIED by the HEP/physics schedds (SECMAN:2010) and several submit hosts (roy/mir/submit1.wid/glbrc/morgridge) are unreachable; polling all advertised schedds takes ~70s because of those timeouts, versus ~10s for ap2001+ap2002 only. grid-submitter.icecube.wisc.edu ads carried no ChtcProjects.
- get_job_pressure.py: rewritten to write job_pressure_YYYY-MM.parquet (zstd, always cast to read_data.JOB_PRESSURE_SCHEMA -- an all-null column otherwise becomes Parquet type Null, which DuckDB cannot read). Schema = old columns + JobState ('idle'|'running'); one row per contiguous (GlobalJobId, JobState) interval. Read-modify-atomic-replace via dot-prefixed .tmp + os.replace, under flock on .job_pressure_YYYY-MM.parquet.lock. Stale window is 3 x --poll-interval (default 1800) instead of the hardcoded 5400. Schedds: DEFAULT_SCHEDDS = ap2001 + ap2002; --schedd (repeatable) overrides. Month file chosen by UTC month. SQLite writer removed. htcondor2 import is now lazy so the module is importable in tests.
- read_data.py: JOB_PRESSURE_SCHEMA, load_job_pressure() (DuckDB, interval-overlap window + optional JobState filter) and attribute_claimed_jobs() (one DuckDB query joining distinct Claimed gpu_state GlobalJobIds to the latest job_pressure record across all months). Naive datetimes are interpreted as UTC (k8s collectors run in UTC); note gpu_state history written by the baremetal collector used local naive time.
- migrate_job_pressure.py: now converts SQLite -> Parquet read-only (source untouched). Old per-snapshot schema is merged into intervals (auto-detected interval, 2x threshold) with --local-utc-offset (default 18000 s, CDT) applied because those timestamps were naive local time; interval-schema files are copied AS RECORDED. Initial version re-merged interval files using a detected gap, which turned out to corrupt them (June: detected 28800 s 'interval', 358,494 -> 316,548 rows); re-merge now requires an explicit --gap. Local conversion results: 2026-05 old schema 10,209,023 rows -> 88,670 intervals (0.3 MB); 2026-06 358,494 -> 358,494; 2026-07 32,201 -> 32,201.
- scripts/host_report.py: sqlite3 reader replaced by load_job_pressure(..., ('idle',)); smoke-run on Chemistry_Huang / June data produced the job pressure table.
- OPERATIONS.md updated (data flow, file layout, converter, schedd auth caveats, that the k8s CronJob is not yet deployed).
- Measured on June 2026 (converted Parquet, idle-only history): attribute_claimed_jobs matched 92.3% of all distinct claimed jobs (157,039 job ids, 0.06 s); the earlier open-capacity-only measurement was 91.4% (34,650 of 37,894). This is before running-job coverage, so expect it to improve once the new collector has run for a month.
- Live end-to-end run (two polls 15 s apart, default schedd selection): 38,556 interval rows, schema identical to JOB_PRESSURE_SCHEMA, running rows present for ap2001/ap2002, last_seen extended on ~99.9% of rows, load_job_pressure read the file back.
- Tests: tests/test_job_pressure.py -- interval open/extend/stale-boundary/state-transition, atomic write + all-null column readable by DuckDB, window inclusivity and multi-month reads, month boundary, GlobalJobId attribution edge cases, converter behavior (offset, no re-merge). Full suite passes.

Not done / needs the requester:
- AC #1: the k8s CronJob manifest lives outside this repo; needs to be added there (run get_job_pressure.py /data --poll-interval <period>, concurrencyPolicy Forbid recommended even though the flock guards overlap) and the baremetal cron retired after a month boundary.
- Schedd scope: per requester decision only ap2001.chtc.wisc.edu and ap2002.chtc.wisc.edu are polled (DEFAULT_SCHEDDS). Jobs from other submit hosts (e.g. HEP, icecube, glbrc, wright/oconnor) therefore stay unattributed; widening would need credentials for the schedds that deny anonymous queries.
- AC #4: the tool is ready and verified on local copies, but the production history on the baremetal host (and months after the Jul 3 sync) still has to be converted and copied to the PVC.
<!-- SECTION:NOTES:END -->
