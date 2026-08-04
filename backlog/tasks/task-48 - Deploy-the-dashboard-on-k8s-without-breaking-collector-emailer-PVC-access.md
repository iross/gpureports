---
id: TASK-48
title: Deploy the dashboard on k8s without breaking collector/emailer PVC access
status: In Progress
assignee:
  - iaross@wisc.edu
created_date: '2026-07-31 20:52'
updated_date: '2026-08-04 19:31'
labels: []
dependencies:
  - TASK-27
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The stat collector and emailers currently run as Kubernetes CronJobs, reading/writing gpu_state Parquet files on a PVC (gpu-stats-data-pvc, storageClassName 3x-replica-hdd-raddus, accessModes: ReadWriteOnce, 15Gi). ReadWriteOnce means the volume can only be mounted read-write by pods on a single node at a time -- exact concurrent-mount behavior (whether multiple pods on the SAME node can share it) depends on the CSI driver backing that storage class. Adding the dashboard as a long-running Deployment introduces a workload that holds the PVC mounted indefinitely, unlike the short-lived CronJob pods. If the dashboard pod and a collector/emailer CronJob pod land on different nodes, the PVC could fail to attach or block, silently breaking data collection or email delivery. This fulfills task-27's still-open AC #4 (k8s Deployment + Service) and AC #7 (CI image build/push) with a concrete, safe approach.

Note on a viable intermediate step: mounting the PVC read-only (readOnly: true on both the pod's volume and volumeMount) in the dashboard pod is a real safety guarantee -- the dashboard container literally cannot write to the volume -- but it does NOT by itself relax the ReadWriteOnce access-mode contract. RWO still restricts the volume to a single node's attachment regardless of how individual pods mount it, so a read-only mount alone doesn't fix cross-node scheduling conflicts. It only fully solves the concurrency concern when combined with node affinity/nodeSelector pinning the dashboard Deployment to the same node as the collector/emailer CronJobs, since RWO does permit multiple pods on the SAME node to mount concurrently under most CSI drivers. Same-node affinity + a read-only dashboard mount is a reasonable, low-effort intermediate step to ship before investing in a ReadOnlyMany-capable storage class or decoupling the dashboard from the PVC entirely.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Confirm whether the 3x-replica-hdd-raddus storage class (or an available alternative) supports ReadWriteMany, or document that it is ReadWriteOnce-only and what that implies for same-node vs cross-node pod scheduling
- [x] #2 Choose and document an approach for safe concurrent access: node-affinity pinning the dashboard Deployment and the collector/emailer CronJobs to the same node, switching to an RWX-capable storage class/volume, or having the dashboard consume data through a separate mechanism (e.g. periodic sync to another volume, object storage, or a lightweight read API) instead of mounting the same PVC directly
- [ ] #3 Dashboard is deployed as a Kubernetes Deployment + Service using the chosen approach (fulfills task-27 AC #4)
- [ ] #4 CI builds and pushes the dashboard image only when dashboard source changes, not on every push to main (fulfills task-27 AC #7)
- [ ] #5 Manual verification: trigger a collector CronJob run and confirm it completes successfully while the dashboard Deployment is running and serving requests, with no PVC mount contention or failures
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Research whether the 3x-replica-hdd-raddus storage class (or any available alternative) supports ReadWriteMany\n2. Document the RWO/RWX findings and their implication for same-node vs cross-node scheduling\n3. Choose and document a concrete approach for safe concurrent access\n4. Leave manifest authoring and manual verification (AC #3-5) for a follow-up once the approach is confirmed
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Per user direction, scoped this pass to AC #1/#2 only (decision writeup); manifests, CI changes, and manual verification (AC #3-5) are follow-up work once the approach is confirmed.

Ran 'kubectl describe storageclass 3x-replica-hdd-raddus' (output supplied by user): provisioner is rook-ceph.rbd.csi.ceph.com (Ceph RBD block storage, ext4 fstype) -- confirmed ReadWriteOnce-only; RWX isn't available for RBD in standard Filesystem volumeMode (only in raw Block mode with app-managed concurrency, which doesn't fit this workload).

Documented findings and the chosen approach in backlog/decisions/task-48-dashboard-pvc-concurrency.md: same-node affinity pinning the dashboard Deployment to the collector/emailer CronJobs' node, plus a read-only PVC mount on the dashboard (safety net, not the concurrency fix itself -- the affinity is). Chose this over an RWX storage class or decoupling the dashboard from the PVC because both are larger, uncertain-effort changes not yet justified; flagged as follow-ups if same-node pinning becomes operationally painful.
<!-- SECTION:NOTES:END -->
