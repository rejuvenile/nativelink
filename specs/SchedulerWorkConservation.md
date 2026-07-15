# SchedulerWorkConservation — verdict

**Question (`#sched`):** does the NativeLink dispatch scheduler conserve work — *"No work
should be queued while processors of any kind are idle (assuming sufficient other resources
exist on the worker: ram, network, etc.)"* — and does the candidate total-core gate restore it?

**TLC verdict:** the current production gate **VIOLATES** work-conservation. The candidate
total-core gate **RESTORES** it *without* oversubscribing cores or overflowing RAM. Proven
by TLC over the finite models below (not a proof for all fleet sizes — dimensions stated).

Spec: `specs/SchedulerWorkConservation.tla`. Full TLC output tee'd to `/tmp/sched-tla-2038737.log`.

---

## 1. The property, formalized

```
IdleProcessor(w) == RunningCount(w) < PCoreCount + ECoreCount   \* a P- or E-core is free
QueuedFits(w)    == \E a : queued(a) /\ (UsedRam(w) + ActionRam(a) <= RamCap)
QuiescentDispatch == \A w,a : ~DispatchEnabled(w,a)              \* dispatch fixpoint

WorkConserving ==
    QuiescentDispatch => (\A w : QueuedFits(w) => ~IdleProcessor(w))
```

- **"processors of ANY kind"** — `IdleProcessor` counts a free slot among `PCoreCount +
  ECoreCount`. At the violating state `RunningCount(w) = PCoreCount` means precisely the P-cores
  are full and *every E-core is idle*; that is a violation exactly as much as an idle P-core.
- **"assuming sufficient other resources"** — the violation is CONDITIONAL on `QueuedFits`: a
  queued action must actually fit the idle worker's remaining RAM. An idle processor with only
  too-big queued work is **not** a violation (see the RAM-bound run, §2).
- **quiescence gate** — the property is asserted only at the dispatch fixpoint
  (`~ENABLED Dispatch` for all w,a). This is load-bearing: in *any* dispatch system there is a
  transient between submit and dispatch where work is queued and a processor is idle; that
  transient is the scheduler about-to-dispatch, not a violation. Gating on quiescence is the
  user's own equivalent form ("no reachable QUIESCENT state ..."). Under the bugged gate the
  fixpoint *is* the live incident; under the fix the only fixpoints are work-conserving.

Resource-safety invariants the fix must not break (checked simultaneously):
```
NoProcessorOversubscription == \A w : RunningCount(w) <= PCoreCount + ECoreCount
NoRamOverflow               == \A w : UsedRam(w) <= RamCap
```

---

## 2. TLC results (all five configs)

| Config | Shape (P/E, workers, actions) | Gate | Result | Distinct states |
|---|---|---|---|---|
| `...BuggedCurrentGate.cfg` | 2/2, 2w, 6a, RAM slack | `running < p_core` | **WorkConserving VIOLATED** | 6 |
| `...FixedTotalCoreGate.cfg` | 2/2, 2w, 6a, RAM slack | `running < p+e_core` | no error (all 3 invariants hold) | 13 |
| `...FixedTotalCoreGateRamBound.cfg` | 2/2, 2w, 6a, **RAM binds** | `running < p+e_core` | no error (holds) | 3 |
| `...BuggedRealShape.cfg` | **4/6**, 2w, 10a, RAM slack | `running < p_core` | **WorkConserving VIOLATED** | 4298 |
| `...FixedRealShape.cfg` | **4/6**, 2w, 10a, RAM slack | `running < p+e_core` | no error (holds) | 59049 |

### CurrentGate counterexample (small shape) — the live incident in miniature
TLC, `SchedulerWorkConservationBuggedCurrentGate.cfg`, depth 5:
```
State 5:  location = (a1:>w1, a2:>w1, a3:>w2, a4:>w2, a5:>"Queue", a6:>"Queue")
```
Both workers at `running = 2 = p_core_count`; the gate `running < p_core_count` refuses all
further dispatch (state is quiescent) yet a5,a6 are queued and fit RAM, while 2 E-cores per
worker sit idle. `WorkConserving` false.

### CurrentGate counterexample (real 4P/6E shape)
TLC, `SchedulerWorkConservationBuggedRealShape.cfg`, depth 9:
```
State 9:  w1 = {a1,a2,a3,a4}  (running=4),  w2 = {a5,a6,a7,a8}  (running=4),
          a9,a10 = "Queue"
```
Every worker pinned at `running = 4 = p_core_count`, all **6 E-cores idle per worker (12 fleet-
wide)**, RAM not the bottleneck, 2 actions queued — the exact live observation (queue deep,
workers at 4, E-cores idle).

### TotalCore — holds, and holds *conditionally* when RAM binds
- `...FixedTotalCoreGate.cfg` / `...FixedRealShape.cfg`: total-core capacity ≥ the offered
  actions, so all work is placed onto the (P+E) processors; every fixpoint has an empty queue.
  `WorkConserving`, `NoProcessorOversubscription` (≤4 resp. ≤10 per worker) and `NoRamOverflow`
  all hold.
- `...FixedTotalCoreGateRamBound.cfg` (RamCap=3, ActionRamEach=2 → 1 action/worker by RAM): at
  the fixpoint each worker runs 1 action (3 idle processors) but **no queued action fits**
  (2+2 > 3), so `QueuedFits` is false and `WorkConserving` holds — the fix correctly leaves
  processors idle when RAM is the true constraint, and `NoRamOverflow` (used=2 ≤ 3) confirms it
  does **not** overflow RAM to conserve work.

**Cross-product conclusion:** work-conserving AND resource-safe hold *simultaneously* under the
total-core gate — the fix does not buy conservation by oversubscribing cores or overflowing RAM.

---

## 3. Ground truth (verified against current code, this session)

- Gate `worker_has_p_headroom` — `nativelink-scheduler/src/api_worker_scheduler.rs:1038-1044`;
  sole per-worker concurrency limiter on top of resource fit, called at dispatch (`:2239`).
- Prod config: `p_idle_threshold_pct = 0` (`nativelink-config/src/schedulers.rs:447`),
  `p_headroom_override_factor = 2` (`default_p_headroom_override_factor` → 2). Threshold 0 makes
  the override clause `p_load < 0` never fire, collapsing the gate to `running < p_core_count`
  (= model `GateMode = "PCoreOnly"`).
- Worker fields `p_core_count` (`worker.rs:210`), `e_core_count` (`:219`), `p_core_load_pct`
  (`:175`). `e_core_count` feeds only the cache-vs-load blend score — it is **never** dispatch
  capacity, which is exactly why the gate leaves 6 processors idle per M4 worker.
- Even if an operator set `p_idle_threshold_pct > 0`, the override admits only up to
  `p_core_count * override_factor = 4*2 = 8 < 10` processors — still under-uses the fleet. The
  total-core bound is the structural fix; the override is a partial, config-gated palliative.

---

## 4. The single caveat the model does NOT capture

**"Work-conserving" here treats an E-core as a fully usable processor** (matching the empirical
finding that macOS spills USER_INITIATED-QoS actions onto E-cores). It therefore ignores that
**E-cores are ~slower than P-cores**: placing an action on an E-core is *work-conserving* but not
necessarily *latency-optimal* — a job that could wait ~milliseconds for a P-core to free might
finish sooner there than on an E-core started immediately. The model also omits: **dispatch
latency / churn** (actions never complete — the monotone dispatch-to-fixpoint is the adversarial
fullest-queue snapshot), **locality / cache-affinity preference** (the real ranker prefers
cache-warm workers), and **network as a second resource dimension** (only RAM is modeled;
uniform per-action RAM — heterogeneous RAM is a trivial `ActionRam(a)` generalization).

Consequence for the fix decision: the total-core gate is the sound fix **for the work-conservation
(throughput / no-idle-processor) property**. It should be paired with an E-core-aware *scheduling
preference* (dispatch to E-cores only after P-cores are full, and/or a small hold for an imminent
P-core) so conservation does not regress p95 latency. That preference is a policy layer on top of
the gate, out of scope for this safety proof.

---

## 5. Scope / do-not-overstate

TLC proves the property over the FINITE models above: 2 workers, P/E ∈ {2/2, 4/6}, ≤10 actions,
uniform RAM, RAM-slack and RAM-binding workloads. A green run is evidence the gate **logic**
conserves work and stays resource-safe on these instances — it is **not** a proof for the live
10×(4+6) fleet at arbitrary queue depth. The real-shape (4/6) run raises confidence that the P/E
ratio in production is the one that exhibits the violation and that the total-core gate closes it.

**Top-line:** the total-core gate `running < p_core_count + e_core_count` (with resource-fit
retained) is the sound fix for the work-conservation property. Current prod gate: VIOLATED at
both modeled shapes. Total-core gate: HOLDS, and holds *conditionally-correctly* (leaves
processors idle only when RAM genuinely does not fit) without oversubscribing cores or
overflowing RAM.
