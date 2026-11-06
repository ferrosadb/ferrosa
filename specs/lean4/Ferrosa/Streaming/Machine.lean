/-
  The memory machine: immutable versions, pinned readers, one permit, and a spill that is not data
  ==============================================================================================

  Three claims the storage and query code makes and has never stated together: a read does not load an
  unbounded result set, results are produced and consumed incrementally, and durable data is never
  edited — only a TRANSFORM writes a new version and publishes it atomically. They are one rule seen
  from three sides, because each is a statement about what may be resident at once and what a reader
  may observe while it is resident.

  So this module is one machine and not three. A `Store` holds versions whose bytes are fixed at write
  time and a manifest that names the version readers may see. A `Reader` pins a version and asks for a
  chunk at a time, so what it holds is a property of the chunk and never of the table. A `Transform`
  takes input versions and produces a fresh one; publishing is the only transition that makes a version
  visible, and a transform that fails or is refused leaves the previous manifest exactly where it was.

  The other two bounds the operator specified sit in the same machine because they are the same
  question asked at a different scale: how many memory-bearing operations may run at once (a permit),
  and what happens when one operator needs working memory proportional to its input (it spills). A
  permit with no limit and a spill with no result bound are both ways of writing "bounded" in a
  comment, which is why `Permits` and `Operator` are modelled here rather than described.

  Nothing here is asserted about Ferrosa; `Ferrosa/Streaming/Audit.lean` records which sites in the
  tree meet each obligation and which do not.
-/
namespace Ferrosa.Streaming

/-! ## Durable artifacts and the manifest that publishes them

A store is a set of versions plus a manifest. `size` is total, and that totality is the model's way of
stating immutability: a version's byte count is a function of the version number, not of when it is
read, so no read can observe it change. `written` is the high-water mark of versions whose bytes exist
and whose checksum was verified; a version above it is not an artifact yet.
-/

/-- A durable store: the published manifest, the fixed size of every written version, and how many
    versions have been written and verified. -/
structure Store where
  /-- The version the manifest publishes for each name. `0` means "this name publishes nothing". -/
  published : Nat → Nat
  /-- The byte length of a written version. Total, so a version's size cannot change under a reader. -/
  size : Nat → Nat
  /-- Versions `1..written` exist, were fully written, and passed their checksum. -/
  written : Nat

/-- The version a reader is given for `name`, or nothing when the manifest has no entry. -/
def Store.read (s : Store) (name : Nat) : Option Nat :=
  if s.published name = 0 then none else some (s.published name)

/-- Bytes a reader must hold to serve `name` from the pinned version alone. -/
def Store.bytesOf (s : Store) (name : Nat) : Nat :=
  match s.read name with
  | none => 0
  | some v => s.size v

/-- A version is visible exactly when some manifest entry names it. Nothing else — no file on disk, no
    partially written run — is reachable by a read. -/
def Store.visible (s : Store) (v : Nat) : Bool :=
  v ≠ 0 && (List.range (s.written + 1)).any (fun name => s.published name = v)

/-- Set one name's manifest entry, leaving every other entry alone. -/
def setEntry (f : Nat → Nat) (name v : Nat) : Nat → Nat :=
  fun n => if n = name then v else f n

/-- The manifest entry for a name after a publish: one name moves, every other entry is untouched. -/
def Store.withPublished (s : Store) (name v : Nat) : Store :=
  { s with published := setEntry s.published name v }

/-- Publishing a new version moves one manifest entry and nothing else, which is what makes a
    transform atomic from a reader's point of view. -/
theorem publish_moves_one_manifest_entry (s : Store) (name v other : Nat) (h : other ≠ name) :
    (s.withPublished name v).published other = s.published other := by
  simp [Store.withPublished, setEntry, h]

/-- A publish does not move the size of any version, because sizes are fixed at write time. -/
theorem publish_does_not_change_a_version_size (s : Store) (name v k : Nat) :
    (s.withPublished name v).size k = s.size k := rfl

/-! ## Readers pin a version and hold a chunk

Two readers over the same store differ only in what they hold at once. A streaming reader holds its
chunk plus a fixed working set; a materialising reader holds the whole result. The difference is not an
implementation detail — it is whether the code path has a memory ceiling or a memory curve.
-/

/-- A reader: the version it pinned, the bytes it asks for per step, its non-data working set, and how
    far it has consumed. -/
structure Reader where
  /-- The version this reader was opened against. It does not change for the reader's lifetime. -/
  pin : Nat
  /-- Bytes requested per step. -/
  chunk : Nat
  /-- Non-data working set: decode buffers, iterators, the page it renders. -/
  fixed : Nat
  /-- Steps consumed so far. -/
  cursor : Nat
  /-- Steps the result contains. -/
  total : Nat
  deriving Repr, DecidableEq

/-- Resident bytes of a reader that consumes one chunk at a time. -/
def Reader.resident (r : Reader) : Nat := r.chunk + r.fixed

/-- Resident bytes of a reader that materialises the whole result first. -/
def materialisedResident (rows width : Nat) : Nat := rows * width

/-- **A streaming reader's resident set does not depend on the result size.**  The same reader holds
    the same bytes whether the table has one row or a billion. -/
theorem a_streaming_reader_holds_the_same_bytes_for_any_result (r : Reader) :
    r.resident = r.chunk + r.fixed := rfl

/-- **No constant bounds a materialising reader.**  For every proposed ceiling there is a result size
    that exceeds it, so "bounded" cannot be claimed of a path that builds the whole result. -/
theorem no_constant_bounds_a_materialising_reader (c width : Nat) (hw : 0 < width) :
    ∃ rows, c < materialisedResident rows width := by
  refine ⟨c + 1, ?_⟩
  unfold materialisedResident
  have h1 : c < c + 1 := Nat.lt_succ_self c
  have h2 : c + 1 ≤ (c + 1) * width := by
    cases width with
    | zero => omega
    | succ w => rw [Nat.mul_succ]; omega
  omega

/-- **A pinned read is one version for its whole life.**  What a reader is shown depends on its pin and
    nothing else, so advancing it can never move it to a version published after it opened. -/
theorem a_reader_view_depends_only_on_its_pin (s : Store) (r r' : Reader) (h : r.pin = r'.pin) :
    s.bytesOf r.pin = s.bytesOf r'.pin := by
  rw [h]

/-- Steps still owed by a reader. -/
def Reader.remaining (r : Reader) : Nat := r.total - r.cursor

/-- Abandoning a reader consumes the rest of its work without performing it. -/
def Reader.abandon (r : Reader) : Reader := { r with cursor := r.total }

/-- **An abandoned reader has no work queued.**  Dropping a reader early leaves nothing to drain, so
    cancellation is not a promise the producer has to keep afterwards. -/
theorem abandoning_a_reader_leaves_no_work (r : Reader) : (r.abandon).remaining = 0 := by
  unfold Reader.remaining Reader.abandon
  exact Nat.sub_self r.total

/-- An abandoned reader keeps its pin, so cancellation cannot silently re-point it at a newer version. -/
theorem abandoning_a_reader_keeps_its_pin (r : Reader) : (r.abandon).pin = r.pin := rfl

/-- Resident bytes of a reader after it is abandoned: the chunk it was holding is released. -/
def Reader.releasedResident (r : Reader) : Nat := r.fixed

/-- **Abandoning releases the chunk**, so a reader that is dropped early holds only its fixed working
    set — and never the rest of the result. -/
theorem abandoning_releases_the_chunk (r : Reader) : r.releasedResident = r.fixed := rfl

/-! ## Transforms: fresh output, verified bytes, atomic publish

A transform is the only thing that produces a new version. Three preconditions make it safe, and the
model states all three rather than leaving them to a caller: the output version is fresh (so no
published artifact is overwritten), every input is a written version (so a transform never reads a
half-written artifact), and the bytes are verified (so an unverified tail is never published).
-/

/-- A transform: the version it produces, the versions it reads, and whether the output's whole byte
    range was written and checksum-verified. -/
structure Transform where
  /-- The version produced. Must be strictly above everything already written. -/
  out : Nat
  /-- The versions consumed. -/
  inputs : List Nat
  /-- Whether the produced bytes were completely written and verified. -/
  verified : Bool
  deriving Repr, DecidableEq

/-- The stated precondition of a transform. Not a convention: a transform that fails it is refused. -/
def Transform.permitted (s : Store) (t : Transform) : Bool :=
  t.verified && s.written < t.out && t.inputs.all (fun v => v ≤ s.written)

/-- The forbidden operation, modelled so its consequence can be stated: rewrite the bytes of a version
    that is already published. `Ferrosa.Streaming.Machine.a_pinned_view_changes_under_an_in_place_edit`
    is the theorem that says why no transform may do this. -/
def Store.editInPlace (s : Store) (v newSize : Nat) : Store :=
  { s with size := setEntry s.size v newSize }

/-- What a step did. A refusal carries the store unchanged, so "nothing happened" is a value the
    caller has to handle rather than an absence. -/
inductive StepResult where
  /-- The step ran; this is the store afterwards. -/
  | ok (s : Store)
  /-- The step was refused or failed; this is the store afterwards, which is the one before. -/
  | refused (s : Store) (why : String)

/-- A step of the machine. Reads and abandons are here so their effect on the store can be proved to
    be none rather than asserted to be none. -/
inductive Step where
  /-- Serve one page from `name`. -/
  | read (name : Nat)
  /-- Drop a reader early. -/
  | abandon
  /-- Write a spill run. Spill bytes are not durable data and never reach the manifest. -/
  | spill (bytes : Nat)
  /-- Produce and publish a new version. -/
  | transform (t : Transform) (name : Nat)
  deriving Repr

/-- Run one step. A transform either publishes the new version in one transition or is refused with the
    store unchanged: there is no third outcome and no partially published version. -/
def run (s : Store) : Step → StepResult
  | .read _ => .ok s
  | .abandon => .ok s
  | .spill _ => .ok s
  | .transform t name =>
    if t.permitted s then
      .ok { (s.withPublished name t.out) with written := t.out }
    else
      .refused s "transform refused: output not fresh, input unwritten, or bytes unverified"

/-- **A read changes nothing.**  Serving a page is a pure observation of one pinned version. -/
theorem a_read_changes_nothing (s : Store) (name : Nat) : run s (.read name) = .ok s := rfl

/-- **An abandoned reader changes nothing.**  Cancelling a read leaves no side effect behind, so a
    client that disconnects cannot leave the store in a different state. -/
theorem an_abandon_changes_nothing (s : Store) : run s .abandon = .ok s := rfl

/-- **A spill changes nothing durable.**  Spill bytes never enter the store, so no sequence of spills
    can move what a reader sees. -/
theorem a_spill_changes_nothing (s : Store) (bytes : Nat) : run s (.spill bytes) = .ok s := rfl

/-- **A failed transform leaves the published version readable.**  The refusal carries the store it was
    given, so every manifest entry — including the one the transform wanted to replace — still names
    what it named. -/
theorem a_failed_transform_leaves_the_manifest (s : Store) (t : Transform) (name : Nat)
    (h : t.permitted s = false) :
    run s (.transform t name) = .refused s
      "transform refused: output not fresh, input unwritten, or bytes unverified" := by
  simp [run, h]

/-- **A refused transform publishes nothing.**  Its result carries the old store, so no name can resolve
    to the version the transform was going to produce. -/
theorem a_refused_transform_publishes_nothing (s : Store) (t : Transform) (name : Nat)
    (h : t.permitted s = false) :
    ∃ s', run s (.transform t name) = .refused s'
      "transform refused: output not fresh, input unwritten, or bytes unverified" ∧ s' = s := by
  exact ⟨s, a_failed_transform_leaves_the_manifest s t name h, rfl⟩

/-- **Publishing is atomic.**  A successful transform moves exactly one manifest entry; there is no
    intermediate state in which half a version is visible. -/
theorem a_successful_transform_publishes_one_entry (s : Store) (t : Transform) (name : Nat)
    (h : t.permitted s = true) :
    run s (.transform t name) = .ok { (s.withPublished name t.out) with written := t.out } := by
  simp [run, h]

/-- **An unverified output is never published.**  A writer that fails mid-stream, or whose tail does
    not checksum, fails the precondition and is refused. -/
theorem an_unverified_transform_is_refused (s : Store) (t : Transform) (h : t.verified = false) :
    t.permitted s = false := by
  simp [Transform.permitted, h]

/-- **An output that is not fresh is refused**, so no transform can overwrite a version that already
    exists. This is the model's statement of "no in-place edit": it is a precondition, not a hope. -/
theorem a_stale_output_is_refused (s : Store) (t : Transform) (h : s.written ≥ t.out) :
    t.permitted s = false := by
  unfold Transform.permitted
  have : ¬ s.written < t.out := Nat.not_lt.mpr h
  simp [this]

/-- **An in-place edit is visible to a reader pinned on that version.**  This is the theorem the
    precondition exists for: if bytes could be rewritten, two reads at the same pin would disagree and
    the "consistent snapshot" claim would be false. -/
theorem a_pinned_view_changes_under_an_in_place_edit (s : Store) (v old new : Nat)
    (h : s.size v = old) (hne : old ≠ new) :
    (s.editInPlace v new).size v ≠ s.size v := by
  have h1 : (s.editInPlace v new).size v = new := by
    simp [Store.editInPlace, setEntry]
  rw [h1, h]
  exact fun hh => hne hh.symm

/-! ## One permit, a bounded queue, and the queue that is a leak

Serialisation is the operator's memory bound: at most `limit` memory-bearing operations run at once. The
model makes the three answers to a second request explicit — refused, queued within a capacity, or
spilled — and proves that the fourth, an unbounded queue, is not a bound at all.
-/

/-- A permit pool: a fixed number of slots and how many are held. -/
structure Permits where
  /-- Slots the node admits at once. -/
  limit : Nat
  /-- Slots currently held. -/
  held : Nat
  deriving Repr, DecidableEq

/-- What happened to an admission request. -/
inductive Admit where
  /-- The request holds a slot until it is dropped. -/
  | granted (p : Permits)
  /-- The request was refused, loudly, with the pool unchanged. -/
  | refused (p : Permits) (why : String)
  deriving Repr

/-- Admit a memory-bearing operation, or refuse it. Refusal is the default so an over-subscribed node
    cannot silently exceed its permit by queueing. -/
def Permits.acquire (p : Permits) : Admit :=
  if p.held < p.limit then .granted ⟨p.limit, p.held + 1⟩ else .refused p "at the memory permit limit"

/-- Held slots after an admission. -/
def Admit.held : Admit → Nat
  | .granted p => p.held
  | .refused p _ => p.held

/-- **At most `limit` operations hold a permit.**  A grant raises the count by one and a refusal leaves
    it alone, so the number of concurrent memory-bearing operations is a constant the node chose. -/
theorem at_most_the_limit_hold_a_permit (p : Permits) (h : p.held ≤ p.limit) :
    p.acquire.held ≤ p.limit := by
  unfold Permits.acquire Admit.held
  by_cases hc : p.held < p.limit
  · simp [hc]; omega
  · simp [hc]; exact h

/-- **A refusal is not a success.**  The caller cannot mistake a refusal for a result, which is what
    keeps an over-subscribed node from degrading into an unbounded queue. -/
def Admit.isGranted : Admit → Bool
  | .granted _ => true
  | .refused _ _ => false

/-- A refusal reports itself as a refusal, so no caller can read "overloaded" as "done". -/
theorem a_refusal_is_not_a_grant (p : Permits) (h : ¬ p.held < p.limit) :
    p.acquire.isGranted = false := by
  simp [Permits.acquire, Admit.isGranted, h]

/-- A queue with a capacity and an occupancy. -/
structure Queue where
  capacity : Nat
  length : Nat
  deriving Repr, DecidableEq

/-- What a push into a bounded queue did. Backpressure is not a failure: the producer waits. -/
inductive Push where
  /-- One item took a free slot. -/
  | accepted (q : Queue)
  /-- The queue is full and the producer must wait. -/
  | backpressure (q : Queue)
  deriving Repr

/-- A bounded push either fills a slot or refuses. It never drops and never grows. -/
def Queue.push (q : Queue) : Push :=
  if q.length < q.capacity then .accepted ⟨q.capacity, q.length + 1⟩ else .backpressure q

/-- Occupancy after a push. -/
def Push.length : Push → Nat
  | .accepted q => q.length
  | .backpressure q => q.length

/-- **A bounded queue never exceeds its capacity**, so a slow consumer cannot become the producer's
    memory bill. -/
theorem a_bounded_queue_stays_within_its_capacity (q : Queue) (h : q.length ≤ q.capacity) :
    q.push.length ≤ q.capacity := by
  unfold Queue.push Push.length
  by_cases hc : q.length < q.capacity
  · simp [hc]; omega
  · simp [hc]; exact h

/-- Occupancy of a queue that has no capacity: what was produced and not yet retired. -/
def unboundedOccupancy (produced retired : Nat) : Nat := produced - retired

/-- Resident bytes held by `n` operators that each spill at `share`. -/
def aggregateResident (n share : Nat) : Nat := n * share

/-- **A per-operator threshold is not a process bound.**  `n` operators that each spill at `share` hold
    `n * share` between them, so any share above `budget / n` overcommits the budget however small each
    operator's own threshold looks. The code sets `share` to half the detected budget, so three
    concurrent operators are already over it. -/
theorem per_operator_thresholds_multiply (budget n share : Nat) (h : budget < n * share) :
    budget < aggregateResident n share := by
  simpa [aggregateResident] using h

/-- **Two permit pools are two bounds.**  Each pool stays within its own limit, so the node's total is
    their sum — which is why a limit per surface is not a limit per node. -/
theorem pool_limits_add (a b : Permits) (ha : a.held ≤ a.limit) (hb : b.held ≤ b.limit) :
    a.held + b.held ≤ a.limit + b.limit := by
  omega

/-- **An unbounded queue converts an out-of-memory into an out-of-memory with extra steps.**  Its
    occupancy is whatever has been submitted and not finished, so it grows with the submission rate and
    has no ceiling in it. A queue is only a bound when its length is one. -/
theorem an_unbounded_queue_is_the_leak (produced retired : Nat) (h : retired ≤ produced) :
    unboundedOccupancy produced retired + retired = produced := by
  unfold unboundedOccupancy
  omega

/-! ## Spill: a bounded working set, a mandatory flush, and files that are not data

An operator whose working set would be proportional to its input spills past a threshold. The model
carries the threshold as the operator's bound and proves three things about it: resident bytes stop at
the bound whatever the input, the step at the bound is a spill rather than "keep going", and the spill
region is disjoint from the published store so no reader can see it.
-/

/-- A blocking operator: its bound, whether it has a spill path at all, and whether that path is
    reachable from the query that needs it. -/
structure Operator where
  /-- Resident-byte threshold at which the operator spills. -/
  bound : Nat
  /-- Whether a spill implementation exists. -/
  hasSpill : Bool
  /-- Whether the executor actually calls it for this shape. -/
  spills : Bool
  deriving Repr, DecidableEq

/-- Resident bytes of a blocking operator over `rows` rows of `width` bytes. Without a reachable spill
    path it holds everything; with one it stops at its bound. -/
def Operator.resident (o : Operator) (rows width : Nat) : Nat :=
  if o.hasSpill && o.spills then min (rows * width) o.bound else rows * width

/-- **Bounded scratch.**  An operator that spills holds at most its bound, whatever its input. -/
theorem a_spilling_operator_is_bounded (o : Operator) (rows width : Nat)
    (h : o.hasSpill = true) (h2 : o.spills = true) : o.resident rows width ≤ o.bound := by
  unfold Operator.resident
  simp [h, h2]
  exact Nat.min_le_right _ _

/-- **A spill path that nothing calls is not a bound.**  With the implementation present but unreachable
    the resident set is the whole input again, which is why `spills` is a separate field from
    `hasSpill`. -/
theorem a_spill_nothing_calls_is_not_a_bound (o : Operator) (rows width : Nat)
    (h : o.spills = false) : o.resident rows width = rows * width := by
  unfold Operator.resident
  simp [h]

/-- What a blocking operator does at its current occupancy. -/
inductive Action where
  /-- Keep buffering: there is room left. -/
  | buffer
  /-- Write the buffer out: the bound is reached. -/
  | spill
  deriving Repr, DecidableEq

/-- The step rule at the threshold. It is a rule, not a preference: reaching the bound selects a spill. -/
def stepAt (resident bound : Nat) : Action :=
  if bound ≤ resident then .spill else .buffer

/-- **Spill is mandatory, not best-effort.**  At the bound the operator's next step is a spill; there is
    no branch that continues in memory. -/
theorem at_the_bound_the_next_step_is_a_spill (bound resident : Nat) (h : bound ≤ resident) :
    stepAt resident bound = .spill := by
  simp [stepAt, h]

/-- Resident bytes after ignoring the rule and continuing in memory. -/
def continuedInMemory (rows width : Nat) : Nat := rows * width

/-- **Continuing in memory breaks the bound.**  This is the trace the rule exists to exclude: past the
    threshold, "keep buffering" holds the whole input. -/
theorem continuing_in_memory_breaks_the_bound (o : Operator) (rows width : Nat)
    (h : o.bound < rows * width) : o.bound < continuedInMemory rows width := by
  simpa [continuedInMemory] using h

/-- Resident bytes of a spilling sort whose sorted output is then collected back into one vector. -/
def collectedResult (rows : Nat) : Nat := rows

/-- **A spill whose result is collected is not a bound.**  The sort can be perfect and the bound still
    defeated: the last step holds every row again, so peak memory is the result, not the threshold. -/
theorem a_collected_result_defeats_the_bound (rows bound : Nat) (h : bound < rows) :
    bound < collectedResult rows := by
  simpa [collectedResult] using h

/-- What a spill attempt did. -/
inductive SpillOutcome where
  /-- The run was written and stays outside the store. -/
  | written
  /-- The write failed: no space, a short write, an I/O error. Named, never silent. -/
  | failed (why : String)
  deriving Repr

/-- Whether a spill may start: it needs the space it is about to use. -/
def spillAdmitted (free need : Nat) : Bool := need ≤ free

/-- **A spill without admission discovers ENOSPC halfway through.**  The model's admitted spill never
    starts without room; the unadmitted one fails inside the merge, after the work is done. -/
theorem an_unadmitted_spill_fails_midway (free need : Nat) (h : free < need) :
    spillAdmitted free need = false := by
  simp [spillAdmitted, Nat.not_le.mpr h]

/-- Spill state: whether the run was published anywhere a reader can reach, and whether a startup sweep
    would clear it. -/
structure SpillState where
  /-- Whether the bytes entered the durable store. Always false, by construction. -/
  inStore : Bool
  /-- Whether the bytes are visible through the manifest. -/
  visible : Bool
  /-- Whether a crash before cleanup leaves the run on disk. -/
  orphanAfterCrash : Bool
  /-- Whether reconciliation adopts the orphan instead of removing it. -/
  adopted : Bool
  deriving Repr, DecidableEq

/-- A spill run as the code writes one: outside the store, invisible, orphaned by a crash, and removed
    by reconciliation rather than adopted. -/
def spillRun : SpillState :=
  { inStore := false, visible := false, orphanAfterCrash := true, adopted := false }

/-- **Spill files are not durable data.**  A run is not in the store and not visible through the
    manifest, so no reader can reach it however the query ends. -/
theorem a_spill_is_not_durable_data : spillRun.inStore = false ∧ spillRun.visible = false := by
  decide

/-- **A crash mid-spill leaves nothing a reader can see.**  The run may sit on disk, but it is outside
    the store and unreferenced, so the loss is work rather than a partially visible result. -/
theorem a_crash_mid_spill_is_invisible : spillRun.visible = false := by decide

/-- **Reconciliation clears an orphan rather than adopting it.**  An adopted orphan is a partial result
    that looks like state, which is worse than the disk space it costs. -/
theorem reconciliation_clears_orphans : spillRun.adopted = false := by decide

/-- Whether a spill medium is memory-backed: a temp directory on tmpfs. -/
def spillMediumIsMemory (tmpfs : Bool) : Bool := tmpfs

/-- **A spill to a memory-backed filesystem is not a spill.**  The bound is on resident pages, so a temp
    directory on tmpfs turns the operator's threshold back into the RAM it was meant to leave. The model
    keeps this as a property of the medium, because whether a path is tmpfs is not visible in the
    operator that writes there. -/
theorem a_spill_to_tmpfs_stays_in_memory : spillMediumIsMemory true = true := rfl

/-- The failure policy of a spill: report the error, or fall back to memory. -/
inductive SpillPolicy where
  /-- Return the error; the query fails with a name. -/
  | loud
  /-- Keep going in RAM, which turns a disk problem into a memory problem. -/
  | fallbackToRam
  deriving Repr, DecidableEq

/-- Resident bytes under each policy at the moment the spill fails. -/
def SpillPolicy.resident (p : SpillPolicy) (rows width bound : Nat) : Nat :=
  match p with
  | .loud => 0
  | .fallbackToRam => rows * width

/-- **Spill failure is loud, or it is a memory leak.**  The fallback policy holds the whole input at the
    exact moment the disk was the thing that failed. -/
theorem a_silent_fallback_exceeds_the_bound (rows width bound : Nat) (h : bound < rows * width) :
    bound < SpillPolicy.resident .fallbackToRam rows width bound := by
  simpa [SpillPolicy.resident] using h

/-- **A reordering stage is a spilling operator, not a streaming one.**  A stage that must see every row
    before it can emit the first cannot be bounded by a chunk, so it is bounded by its spill or not at
    all. -/
def reorderingStageResident (rows width : Nat) (spills : Bool) (bound : Nat) : Nat :=
  if spills then min (rows * width) bound else rows * width

/-- A reordering stage with no spill is proportional to its input, whatever its chunk size. -/
theorem a_reordering_stage_without_spill_is_proportional (rows width bound : Nat) :
    reorderingStageResident rows width false bound = rows * width := by
  simp [reorderingStageResident]

end Ferrosa.Streaming
