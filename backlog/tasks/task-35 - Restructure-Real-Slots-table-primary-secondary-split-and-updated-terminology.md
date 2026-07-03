---
id: TASK-35
title: 'Restructure Real Slots table: primary/secondary split and updated terminology'
status: Done
assignee: []
created_date: '2026-06-25 13:45'
updated_date: '2026-06-25 15:10'
labels: []
dependencies: []
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The Real Slots table presents backfill usage as a disconnected bottom section, hiding the relationship between prioritized machines and their secondary (backfill) workloads. Additionally, 'Researcher Owned' and 'CHTC Owned' labels don't communicate the hardware-vs-capacity distinction to readers unfamiliar with CHTC infrastructure. This restructuring makes both the utilization story and the ownership model more transparent.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] TOTAL row includes both primary and secondary usage
- [ ] Prioritized rows split: Primary subtotal (current priority slots) + Secondary subtotal (current backfill slots on those machines)
- [ ] Separate BACKFILL TOTAL section removed; its data appears under Prioritized → Secondary
- [ ] "Researcher Owned" renamed to "Researcher-Owned Hardware" everywhere in the report
- [ ] "CHTC Owned" renamed to "Researcher-Reserved Capacity" everywhere in the report
- [ ] "Backfill" renamed to "Secondary (Backfill)" everywhere in the report
- [ ] Open Capacity section structure unchanged
- [ ] Report generates without errors
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Restructured the main Real Slots table in stats_reporting.py to nest Secondary (Backfill) rows under Prioritized, with Primary and Secondary sub-subtotals. TOTAL row now includes both primary and secondary usage. Removed the separate BACKFILL TOTAL section. Updated get_display_name in gpu_utils.py and gpu_utils_polars.py: Priority-ResearcherOwned → Researcher-Owned Hardware, Priority-CHTCOwned → Researcher-Reserved Capacity, Backfill-* → same display names (context provided by row grouping). Updated all user-visible label strings in stats_reporting.py text output and host_report.py overview table and section headers. Modified files: stats_reporting.py, gpu_utils.py, gpu_utils_polars.py, scripts/host_report.py
<!-- SECTION:NOTES:END -->

## Mockup

### Before

```
| Class                              | Allocated % | Allocated (avg.) | Drained (avg.) | Available (avg.) |
|------------------------------------|-------------|------------------|----------------|------------------|
| TOTAL                              |   xx%       |       xx         |      xx        |       xx         |
|   Prioritized (TOTAL)              |   xx%       |       xx         |      xx        |       xx         |
|     Prioritized (Researcher Owned) |   xx%       |       xx         |      xx        |       xx         |
|     Prioritized (CHTC Owned)       |   xx%       |       xx         |      xx        |       xx         |
|   Open Capacity (TOTAL)            |   xx%       |       xx         |      xx        |       xx         |
|     Open Capacity (Flagship)       |   xx%       |       xx         |      xx        |       xx         |
|     Open Capacity (Standard)       |   xx%       |       xx         |      xx        |       xx         |
| ── separator ──────────────────────────────────────────────────────────────────────────────────── |
| BACKFILL TOTAL                     |   xx%       |       xx         |      xx        |       xx         |
|   Backfill (Researcher Owned)      |   xx%       |       xx         |      xx        |       xx         |
|   Backfill (CHTC Owned)            |   xx%       |       xx         |      xx        |       xx         |
```

Issues: TOTAL excludes backfill; backfill appears unrelated to prioritized machines; labels obscure ownership model.

### After

```
| Class                                          | Allocated % | Allocated (avg.) | Drained (avg.) | Available (avg.) |
|------------------------------------------------|-------------|------------------|----------------|------------------|
| TOTAL (primary + secondary)                    |   xx%       |       xx         |      xx        |       xx         |
|   Prioritized (TOTAL)                          |   xx%       |       xx         |      xx        |       xx         |
|     Primary                                    |   xx%       |       xx         |      xx        |       xx         |
|       Researcher-Owned Hardware                |   xx%       |       xx         |      xx        |       xx         |
|       Researcher-Reserved Capacity             |   xx%       |       xx         |      xx        |       xx         |
|     Secondary (Backfill)                       |   xx%       |       xx         |      xx        |       xx         |
|       Researcher-Owned Hardware                |   xx%       |       xx         |      xx        |       xx         |
|       Researcher-Reserved Capacity             |   xx%       |       xx         |      xx        |       xx         |
|   Open Capacity (TOTAL)                        |   xx%       |       xx         |      xx        |       xx         |
|     Open Capacity (Flagship)                   |   xx%       |       xx         |      xx        |       xx         |
|     Open Capacity (Standard)                   |   xx%       |       xx         |      xx        |       xx         |
```
