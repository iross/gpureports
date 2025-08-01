# Methodology

**Data Collection:** The source data is acquired by using the python bindings to query for Startds with at least one GPU. `State`, `Name`, and `PrioritizedProjects` are used to classify both the current state of each GPU and whether it's in a Prioritized, Open Capacity, or Backfill slot. This data is grabbed every 15 minutes and stored in a sqlite database.


**Allocation Categories:**

- **Open Capacity:** Slots where PrioritizedProjects is empty and the slot name does not contain 'backfill'. These represent general-access GPU resources.
- **Prioritized Service:** Slots where PrioritizedProjects is not empty and the slot name does not contain 'backfill'. 
- **Backfill:** Slots where the slot name contains 'backfill'. These utilize idle priority resources for lower-priority jobs.

**Metrics:**

- **Allocated %:** Percentage of available GPUs that are in 'Claimed' state during the time period
- **Allocated (avg.):** Average number of GPUs in 'Claimed' state across all 15-minute snapshots in the time period
- **Available (avg.):** Average total number of GPUs available for allocation across all snapshots in the time period. If a GPU is being utilized in a Prioritized slot, it is removed from Available in the Backfill slot for the entire time slice.

**Calculation Method:** Averages are calculated by sampling GPU states every 15 minutes and computing the mean across all intervals in the specified lookback period. 