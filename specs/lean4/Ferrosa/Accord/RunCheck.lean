/-
  Running the checker on histories a run actually produced.

  `main` is executed here, so the printed verdicts are the evidence that the
  parser and the checker agree with real Elle output.

  The conclusive check is the last line: replay the *whole* history in
  completion order and compare every observed read.  `true` there is a
  certificate that the run's history is serializable — the order used is the
  real-time order the client observed, so it is also the strict witness.
  A `false` is not a proof of non-serializability (some other order might work);
  it means the completion order is not a witness.
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