---
id: TASK-53
title: Add a groupreport command for per-project GPU usage reports over a time range
status: To Do
assignee: []
created_date: '2026-09-29 14:52'
updated_date: '2026-09-29 14:55'
labels:
  - reporting
  - cli
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Ad-hoc group reports (for a PI, department or project such as Chemistry_Huang or Biochemistry_Raman) require knowing the right --host and --hours-back flags of scripts/host_report.py, which only reports on hardware-owning projects over a lookback window. Add a simple interface that takes a group name and a time range and produces that group's GPU usage report, including groups that own no hardware and run only on open capacity.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Running `groupreport <group> <start> [end]` produces a report over the given time range without the user specifying hosts or hours-back
- [ ] #2 The group name is resolved against both PrioritizedProjects (hardware groups) and ChtcProjects (job-level groups such as Biochemistry_Raman) using whole-name matching, and the report states which it matched; an unknown name exits non-zero suggesting close names
- [ ] #3 For hardware groups: machines and GPU types, available/used GPU-hours, utilization, and per-user GPU-hours on priority slots (backfill optional)
- [ ] #4 For job-level groups: per-user and total GPU-hours on open-capacity and backfill slots, by GPU model, plus queue pressure where available; the report states how membership was attributed and its known limits
- [ ] #5 Pressure/contention/fairshare sections appear when job_pressure data covers the window; otherwise the report says they are unavailable
- [ ] #6 Explicit start and end times are honored, including windows spanning multiple monthly parquet files; --hours-back remains as an alternative
- [ ] #7 Host exclusions from masked_hosts.yaml and --exclude-users are applied, and the same dedup as the email/dashboard reports is used so totals agree with them
- [ ] #8 Only one implementation of the group report remains (scripts/host_report.py replaced or reduced to a thin caller); justfile and README document the command
- [ ] #9 Tests cover name matching for both group kinds, window boundaries, per-user hour totals and missing job_pressure data
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Research findings (what already exists):

TWO DIFFERENT "GROUP" NOTIONS -- the design hinges on which one the command reports on:
- PrioritizedProjects (gpu_state parquet, per slot): hardware-level. Names the projects that own/have priority on a machine (e.g. Chemistry_Huang). Empty on open-capacity slots by definition. Can hold several space-separated names.
- ChtcProjects (job_pressure SQLite only, per job): job/user-level project from the submitter's ClassAd. This is where names like Biochemistry_Raman live -- it appears in job_pressure_2026-05/06 (owners jschwartz36, sstrugar) and NOT in any gpu_state PrioritizedProjects. Values can be comma-separated lists (e.g. 'ECE_Ramanathan,Biostats_DaifengWang'). Such groups own no hardware, so their usage is entirely open-capacity/backfill.

Answer to "do open-capacity slots say who is running and what group": who yes, group no.
- gpu_state on claimed open-capacity slots (primary, non-backfill, empty PrioritizedProjects) always has RemoteOwner ('user@chtc.wisc.edu'; 0 nulls in 2026-08, 194 distinct users) and GlobalJobId (125 nulls of ~1.14M rows). collector.py's projection has no per-job project/group attribute (Name, AssignedGPUs, State, PrioritizedProjects, GPUsAverageUsage, Machine, RemoteOwner, GlobalJobId, PreventJobsReason, Disk, ...).
- Group can only be recovered by joining to job_pressure. Two routes: (a) join on GlobalJobId -- measured on June 2026 (full-month coverage in both sources) 91% of distinct open-cap claimed jobs match (34,650 of 37,894); misses are jobs never observed idle (started before the next poll) or on schedds get_job_pressure.py does not poll (only ap2001/ap2002 of 9 schedds seen in gpu_state); (b) map RemoteOwner -> ChtcProjects via Owner (Owner is the bare username, RemoteOwner has @domain; strip it). 173 of 252 open-cap users (93.9% of claimed observations, 2026-05..07) have a known project this way, but 5 of 199 owners have more than one ChtcProjects value and users can belong to several groups. Route (a) is preferred; route (b) is the fallback for misses.
- job_info_YYYY-MM.db had per-job data but collection stopped at the parquet cutover (TASK-39) -- not a source.
- job_pressure is currently collected only by a baremetal cron into SQLite, not on k8s (see TASK-54 for reviving it in Parquet with running-job and all-schedd coverage). This task depends on that data being available; until then job-level groups can only be reported for windows covered by the synced SQLite copies (2026-06 fully, 2026-05 partially and in old schema).

Existing report code:
- scripts/host_report.py is the closest thing and works end-to-end (`uv run python scripts/host_report.py --project Chemistry_Huang --hours-back 168 --output-dir /tmp/hr`, ~5s, Markdown + 4 PNGs). But it is hardware-centric: --project only resolves machines via PrioritizedProjects substring match and uses ChtcProjects only to filter queued-job pressure. `--project Biochemistry_Raman` would find no machines and exit with an error. Gaps: only --hours-back (no start/end); no positional project; legacy pandas path with 15-min buckets and get_time_filtered_data/filter_df_enhanced instead of canonical prepare_frames(); does not load masked_hosts.yaml (classify_slots.HOST_EXCLUSIONS starts empty and it never sets it); sys.path.insert hack; hardcoded BUCKET_HOURS.
- Canonical pieces to build on: read_data.scan_time_filtered -> classify_slots.prepare_frames (dedup via slot_dedup_rank, exclusions, PreparedFrames.dedup/raw_bf); calculate_backfill_usage_by_user() and calculate_device_user_breakdown() (per-user polars aggregations); _researcher_scope(). Nothing in the canonical pipeline filters by project.
- Nothing else does project-level reporting: archive/experiments/q4_2025_analysis.py, analysis/analyze_task7_troubleshoot.py, weekly_summary.py, open_cap_user_jobs.py (open-cap usage by user, no group), analyze_pool_health.py.
- PrioritizedProjects data facts: multi-project values ('BMI_Gitter SmallMolecule_Hoffman', 'MaterialScience_Morgan UWMadison_Skunkworks_2023') so match whole tokens not substrings ('Morgan' must not match MaterialScience_Morgan); placeholder values 'None','CHTC','OSPool','COSMOS' are not groups.

Plan:
1. Confirm scope with the requester: groupreport should accept EITHER kind of group name. Resolve the name against both vocabularies (PrioritizedProjects tokens in gpu_state; ChtcProjects tokens, comma-split, in job_pressure) and report which it matched; error with close-name suggestions if neither, and handle a name present in both.
2. Hardware groups: machines via whole-token match on primary-slot PrioritizedProjects; report priority usage per user, utilization, optional backfill on those machines.
3. Job-level groups: member set = distinct job_pressure Owners with that ChtcProjects token (strip @domain to match RemoteOwner); report their GPU-hours per slot class (priority-on-someone-else's-hardware is impossible unless they also belong to a hardware group; open capacity; backfill by machine ownership class), per user, per GPU model, over time, plus queue wait/pressure from job_pressure. State the attribution limits in the report (users first seen idle only; multi-group users counted in each group, not split).
4. Decide whether to also improve attribution at the source (collector: capture the job's project) -- separate task if pursued; this task must work with job_pressure-derived membership.
5. Implement on prepare_frames() with explicit start/end (reuse usage_stats' flexible datetime parsing), no hardcoded bucket size, masked_hosts.yaml + --exclude-users applied; CLI `groupreport <group> <start> [end]` + justfile recipe; --hours-back as alternative.
6. Replace or thin out scripts/host_report.py so only one implementation remains; update justfile isye-report. Place module per TASK-52's src/gpureports/ layout.
7. Tests on synthetic parquet+sqlite fixtures: token matching for both vocabularies, multi-group users, unknown name, window boundaries spanning two monthly files, per-user hour totals, missing job_pressure for the window. Smoke-run with a hardware group (Chemistry_Huang), a multi-project machine, and Biochemistry_Raman over 2026-05/06 (where job_pressure data exists).
8. Update README.md usage.
<!-- SECTION:PLAN:END -->
