# Decision: Dashboard/PVC concurrency approach for k8s deployment

**Date:** 2026-08-04
**Status:** Accepted (for AC #1/#2 only — manifests and rollout are a follow-up)
**Task:** TASK-48

## Context

The collector and emailer CronJobs read/write gpu_state Parquet files on `gpu-stats-data-pvc`
(storage class `3x-replica-hdd-raddus`, 15Gi). Adding the dashboard as a long-running
Deployment means a workload holds the PVC mounted indefinitely, instead of the short-lived
CronJob pods. The question: can the dashboard mount the same PVC concurrently with the
collector/emailer without risking mount failures or blocking data collection, and if not,
what's a safe way to give it access?

## Storage class findings (AC #1)

`kubectl describe storageclass 3x-replica-hdd-raddus` output:

```
Provisioner:  rook-ceph.rbd.csi.ceph.com
Parameters:   ... csi.storage.k8s.io/fstype=ext4, pool=path-replica-hdd-pool ...
```

This is a **Rook-Ceph RBD (block storage)** class, provisioning ext4-formatted volumes for
standard `Filesystem`-mode PVCs. RBD volumes of this kind are **ReadWriteOnce only** — this
is not a soft limitation of the class's config, it's inherent to how RBD attaches a block
device to a single node. `ReadWriteMany` for RBD is only possible in raw `Block` volumeMode,
where the application manages concurrent writers itself; that doesn't fit this workload
(the collector, emailer, and dashboard all expect a normal filesystem with Parquet files on
it, not a raw block device).

Getting genuine `ReadWriteMany` would require a CephFS-backed storage class instead (a
different provisioner, `rook-ceph.cephfs.csi.ceph.com`) — whether one exists in this cluster
is unconfirmed and out of scope for this decision; see Follow-ups.

**Conclusion: `3x-replica-hdd-raddus` is ReadWriteOnce-only.** Under RWO, the PVC can only be
attached to one node at a time. Whether multiple pods on that *same* node can mount it
concurrently is a property of the CSI driver, not the access mode — Ceph-CSI RBD does permit
multiple pods on the same node to mount an already-attached RWO volume, so same-node
co-location is viable; cross-node is not.

## Chosen approach (AC #2)

**Same-node affinity + read-only dashboard mount**, as the low-effort, low-risk near-term
approach:

- The dashboard Deployment gets a `nodeAffinity`/`nodeSelector` pinning it to the same node
  the collector/emailer CronJobs run on (or a node pool of one, if the cluster doesn't
  otherwise constrain CronJob scheduling — see Follow-ups).
- The dashboard's PVC mount is `readOnly: true` on both the volume and volumeMount. This is a
  real guarantee — the container literally cannot write to the volume — but it does **not**
  by itself solve the access-mode conflict; it only removes the risk of the dashboard
  corrupting collector output if a race were to occur. The concurrency safety comes from the
  node pinning, not the read-only flag.

This was chosen over the alternatives in AC #2 because:

- **RWX-capable storage class**: would fully remove the co-location constraint, but requires
  either finding an existing CephFS class or provisioning one — larger, uncertain-effort
  infra work with no confirmed availability yet (see Follow-ups). Worth revisiting once/if the
  same-node constraint becomes a real operational problem (e.g. cluster autoscaling wants to
  move the dashboard, or the dashboard needs multiple replicas for HA).
- **Decoupling the dashboard from the PVC** (periodic sync to another volume, object storage,
  or a lightweight read API): more robust long-term, but more code and moving parts than is
  justified to unblock this task. Same rationale as above — a good next step if same-node
  pinning turns out to be operationally painful.

## Follow-ups (not covered by this decision)

- Confirm whether a CephFS (or other RWX-capable) storage class exists in this cluster as an
  option for later.
- Author the actual Deployment/Service manifests and node-affinity rules (AC #3).
- CI path-filtered image build/push (AC #4).
- Manual verification: trigger a collector CronJob run with the dashboard Deployment live and
  confirm no mount contention (AC #5) — requires cluster access this session doesn't have.
