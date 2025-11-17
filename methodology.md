# Methodology

**Data Collection:** The source data is acquired by using the python bindings to query for Startds with at least one GPU. `State`, `Name`, and `PrioritizedProjects` are used to classify both the current state of each GPU and whether it's in a Prioritized, Open Capacity, or Backfill slot. This data is grabbed every 15 minutes and stored in a sqlite database.


**Allocation Categories:**

- **Open Capacity:** Slots on machines with empty PrioritizedProjects, excluding CHTC owned machines.
- **Prioritized (Researcher Owned):** Priority slots on researcher owned machines with non-empty PrioritizedProjects.
- **Prioritized (CHTC Owned):** Priority slots on CHTC owned machines with non-empty PrioritizedProjects (previously called "Hosted Capacity")
- **Backfill (CHTC Owned):** Backfill slots specifically on CHTC owned machines
- **Backfill (Researcher Owned):** Backfill slots on researcher owned machines hosted within CHTC
- **Backfill (Open Capacity):** Backfill slots on open capacity machines (empty PrioritizedProjects, not in CHTC owned list).

**Machine Classification Logic:**

*Machine classification is based on:*
1. **External List Integration:** The `chtc_owned` file contains a definitive list of CHTC owned machines.
2. **PrioritizedProjects Field:** Used to identify researcher owned vs open capacity machines.

*Classification Rules:*
- **CHTC Owned Machines:** Any machine listed in the `chtc_owned` file, regardless of PrioritizedProjects value. These are machines that were purchased with Campus support as part of startup packages.
- **Researcher Owned Machines:** Machines with non-empty PrioritizedProjects field that are NOT in the CHTC owned list.
- **Open Capacity Machines:** Machines with empty PrioritizedProjects field that are NOT in the CHTC owned list.

*Backfill Slot Classification:*
- Backfill slots are classified based on the machine they run on:
  - Slots on CHTC owned machines → "Backfill (CHTC Owned)"
  - Slots on researcher owned machines → "Backfill (Researcher Owned)"
  - Slots on open capacity machines → "Backfill (Open Capacity)" (reclassified from backfill)

**Metrics:**

- **Allocated %:** Percentage of available GPUs that are in 'Claimed' state during the time period
- **Allocated (avg.):** Average number of GPUs in 'Claimed' state across all 15-minute snapshots in the time period
- **Available (avg.):** Average total number of GPUs available for allocation across all snapshots in the time period. If a GPU is being utilized in a Prioritized slot, it is removed from Available in the Backfill slot for the entire time slice.

**Calculation Method:** Averages are calculated by sampling GPU states every 15 minutes and computing the mean across all intervals in the specified lookback period.

**GPU Model Filtering:** For consistency and cleaner analysis, certain older or uncommon GPU models are filtered out of allocation calculations and user usage tables by default. This filtering ensures that the allocation statistics and usage breakdowns focus on the most relevant and commonly used GPU types in the cluster.

*Filtered GPU Models:*
- **GTX 1080** series
- **P100**
- **Quadro** series
- **A30**
- **A40**

*Note:* Tables marked with "(filtered)" exclude these GPU models from calculations. The raw allocation summary tables include all GPU types for completeness.

**GPU Performance Tier Classifications:**

Open Capacity GPUs are grouped into performance tiers to provide visibility into high-demand vs standard GPU utilization:

*Flagship Tier:*
- **NVIDIA H100 80GB HBM3** - Latest Hopper architecture, highest performance
- **NVIDIA H200** - Latest generation with 141GB HBM3e memory
- **NVIDIA A100-SXM4-80GB** - High-memory variant of A100

*Standard Tier:*
- **NVIDIA L40S** (48GB)
- **NVIDIA L40** (48GB)
- **NVIDIA GeForce RTX 2080 Ti** (11GB)
- **NVIDIA A100-SXM4-40GB** (40GB)
- All other GPU models not in Flagship tier

*Note:* This classification groups the most capable, high-memory GPUs (H100, H200, A100 80GB) together as "Flagship" to track utilization of premium resources separately from standard GPUs.

**Memory Category Classifications:**

GPUs are grouped into memory categories based on their VRAM capacity for easier analysis. The classifications below reflect the GPU models included in filtered tables (marked with "(filtered)"):

*<48GB Memory Category:*
- **NVIDIA GeForce RTX 2080 Ti** (11GB)
- **NVIDIA A100-SXM4-40GB** (40GB)

*48GB Memory Category:*
- **NVIDIA L40** (48GB)
- **NVIDIA L40S** (48GB)

*80GB Memory Category:*
- **NVIDIA A100-SXM4-80GB** (80GB)
- **NVIDIA H100 80GB HBM3** (80GB)

*>80GB Memory Category:*
- **NVIDIA H200** (141GB HBM3e)

*Note: Additional GPU models exist in the cluster but are excluded from filtered tables for cleaner analysis. See "GPU Model Filtering" section above for details.*

**Machine Classifications:**

*CHTC Owned Machines (as of 2025-08-08):*
- blengerichgpu4000.chtc.wisc.edu
- ssilwalgpu4000.chtc.wisc.edu
- amuraligpu4000.chtc.wisc.edu
- btellman-jsullivangpu4000.chtc.wisc.edu
- mkhodakgpu4000.chtc.wisc.edu
- txie-dsigpu4000.chtc.wisc.edu
- cxiaogpu4000.chtc.wisc.edu
