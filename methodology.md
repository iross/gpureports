# Methodology

**Data Collection:** The source data is acquired by using the python bindings to query for Startds with at least one GPU. `State`, `Name`, and `PrioritizedProjects` are used to classify both the current state of each GPU and whether it's in a Prioritized, Open Capacity, or Backfill slot. This data is grabbed every 15 minutes and stored in a sqlite database.


**Allocation Categories:**

- **Open Capacity:** Slots on machines with empty PrioritizedProjects, excluding hosted capacity machines.
- **Prioritized Service:** Slots on machines with non-empty PrioritizedProjects.
- **Backfill (Hosted Capacity):** Backfill slots specifically on hosted capacity machines 
- **Backfill (Researcher Owned):** Backfill slots on researcher owned machines hosted within CHTC
- **GlideIn:** Backfill slots on open capacity machines (empty PrioritizedProjects, not in hosted capacity list).

**Machine Classification Logic:**

*Machine classification is based on:*
1. **External List Integration:** The `hosted_capacity` file contains a definitive list of hosted capacity machines.
2. **PrioritizedProjects Field:** Used to identify researcher owned vs open capacity machines.

*Classification Rules:*
- **Hosted Capacity Machines:** Any machine listed in the `hosted_capacity` file, regardless of PrioritizedProjects value.
- **Researcher Owned Machines:** Machines with non-empty PrioritizedProjects field that are NOT in the hosted capacity list.
- **Open Capacity Machines:** Machines with empty PrioritizedProjects field that are NOT in the hosted capacity list.

*Backfill Slot Classification:*
- Backfill slots are classified based on the machine they run on:
  - Slots on hosted capacity machines → "Backfill (Hosted Capacity)"
  - Slots on researcher owned machines → "Backfill (Researcher Owned)" 
  - Slots on open capacity machines → "GlideIn" (reclassified from backfill)

**Metrics:**

- **Allocated %:** Percentage of available GPUs that are in 'Claimed' state during the time period
- **Allocated (avg.):** Average number of GPUs in 'Claimed' state across all 15-minute snapshots in the time period
- **Available (avg.):** Average total number of GPUs available for allocation across all snapshots in the time period. If a GPU is being utilized in a Prioritized slot, it is removed from Available in the Backfill slot for the entire time slice.

**Calculation Method:** Averages are calculated by sampling GPU states every 15 minutes and computing the mean across all intervals in the specified lookback period.

**Machine Classifications:**

*Hosted Capacity Machines (as of 2025-08-08):*
- blengerichgpu4000.chtc.wisc.edu
- ssilwalgpu4000.chtc.wisc.edu
- amuraligpu4000.chtc.wisc.edu
- btellman-jsullivangpu4000.chtc.wisc.edu
- mkhodakgpu4000.chtc.wisc.edu
- txie-dsigpu4000.chtc.wisc.edu
- cxiaogpu4000.chtc.wisc.edu
