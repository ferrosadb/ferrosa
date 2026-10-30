/-
  Running the checker on a history a real run produced
  ====================================================

  The printed verdicts are the evidence that the parser and the checker agree with real Elle output,
  because `main` runs here rather than inside a test. `main` loads one Elle EDN file, prints what it
  parsed — events, committed transactions, failures — and runs the search form of the checker over
  the first four and the first five committed transactions, where the search tries every order of
  that prefix. A `true` is then a certificate that the prefix is serializable, since a serial order
  was exhibited, and a `false` is a witness that no order of the prefix replays to what the clients
  saw. `completionOrder` names the real-time order a run actually exhibits, and replaying the whole
  history against it is what would settle the full history, but nothing in `main` does that yet: what
  runs is the prefix search. The fixture path points into a sibling worktree,
  `../../.wt-pr457-linux/deploy/fly-accord-elle`, so that checkout has to exist for the run to work.
-/
import Ferrosa.Accord.Edn

namespace Ferrosa.Accord

def describe (h : History) : String :=
  s!"events={h.evs.length} committed={(h.committed).length} failed={(h.evs.filter (fun e => e.kind == .fail)).length}"

/-- The first `n` completed transactions — a coherent prefix sub-history, used
    to exercise the *search* form of the checker rather than the replay form. -/
def firstNCommitted (n : Nat) (h : History) : History :=
  ⟨(h.evs.filter (fun e => e.kind == .ok)).take n⟩

/-- Completion order: the order in which the clients saw their transactions
    finish, which is the real-time linear extension a run actually exhibits. -/
def completionOrder (h : History) : List TxnId := (h.committed).map TxnRec.id

def historyDir : System.FilePath :=
  "../../.wt-pr457-linux/deploy/fly-accord-elle"

def checkFile (path : System.FilePath) : IO Unit := do
  let h ← loadHistory path
  IO.println s!"{path}"
  IO.println s!"  parsed: {describe h}"
  let small := firstNCommitted 4 h
  IO.println s!"  serializable(first 4 committed, search over 4! orders): {serializableB small}"
  let small5 := firstNCommitted 5 h
  IO.println s!"  serializable(first 5 committed, search over 5! orders): {serializableB small5}"

def main : IO Unit := do
  checkFile (historyDir / "elle-cert-e860ec88-20260724T181744Z.edn")

end Ferrosa.Accord

def main : IO Unit := Ferrosa.Accord.main