import Ferrosa.Docs.Tex

/-!
# A sentence for theorems whose module gives none

Most theorems in this corpus carry a doc comment, and that is the sentence the book prints. About
one theorem in five does not. For those the renderer needs *something* in English, and the only
honest source is the statement itself: this file says what the formula says, using the formula's own
terms, with the connectives in words.

    ∀ s, Reachable(batch, s) → acked(s) ≤ synced(s)
    "For all s: if Reachable(batch, s), then acked(s) ≤ synced(s)."

That is a reading, not an interpretation. It cannot say *why* the theorem matters — only a docstring
can — but it does say what it claims, in the order the statement says it.
-/

namespace Ferrosa.Docs.Gloss

open Lean

/-- The math wrapper removed: `\mathrm{Step}(\mathit{s})` becomes `Step(s)`. The result is meant for
    a `\texttt` span in prose, so the escapes the LaTeX printer already applied stay. -/
def plainify (s : String) : String :=
  -- The wrappers are rewritten, never removed: `\mathrm{X}` becomes `\texttt{X}` and keeps its
  -- closing brace. Stripping the opening brace leaves an unmatched one, which TeX happily pairs with
  -- something later in the paragraph and then reports as an undefined command a page away.
  s.replace "\\mathrm{" "\\texttt{"
   |>.replace "\\mathsf{" "\\texttt{"
   |>.replace "\\mathit{" "\\texttt{"
   |>.replace "\\prime" "'"
   |>.replace "\\langle" "⟨"
   |>.replace "\\rangle" "⟩"
   |>.replace "\\ge" "≥"
   |>.replace "\\Rightarrow" "⇒"
   |>.replace "\\tau" "τ"
   |>.replace "\\gamma" "γ"
   |>.replace "\\alpha" "α"
   |>.replace "\\beta" "β"
   |>.replace "\\delta" "δ"
   |>.replace "\\sigma" "σ"
   |>.replace "\\mu" "μ"
   |>.replace "\\ne" "≠"
   |>.replace "\\le" "≤"
   |>.replace "\\cdot" "·"
   |>.replace "\\times" "×"
   |>.replace "\\to" "→"
   |>.replace "\\leftrightarrow" "↔"
   |>.replace "\\forall" "∀"
   |>.replace "\\exists" "∃"
   |>.replace "\\top" "⊤"
   |>.replace "\\bot" "⊥"
   |>.replace "\\lambda" "λ"
   |>.replace "\\subset" "⊂"
   |>.replace "\\subseteq" "⊆"
   |>.replace "\\wedge" "∧"
   |>.replace "\\vee" "∨"
   |>.replace "\\neg" "¬"
   |>.replace "\\in" "∈"
   |>.replace "\\," " "
   |>.replace "\\ " " "


/-- A term, set as code inside a sentence. -/
def code (env : Environment) (e : Expr) : String :=
  plainify (Ferrosa.Docs.Tex.tex env e)

/-- Split at top-level conjunctions, as the display printer does. -/
partial def conjuncts (e : Expr) : Array Expr :=
  match e.getAppFn with
  | .const n _ =>
    if n.toString.endsWith "And" && e.getAppArgs.size >= 2 then
      let a := e.getAppArgs
      conjuncts a[a.size - 2]! ++ conjuncts a[a.size - 1]!
    else #[e]
  | _ => #[e]

/-- Walk the binder telescope, splitting it into quantified variables and hypotheses. Same
    substitution trick as the display printer: every binder is replaced by a constant named for it,
    so the body prints `s` rather than a de Bruijn index. -/
partial def peel (env : Environment) (e : Expr) (subst : Array Expr) (vars : Array String)
    (hyps : Array String) : Array String × Array String × Expr :=
  match e with
  | .forallE n t b bi =>
    let nm := if n.toString == "_" then "x" else n.getString!
    let t := t.instantiate subst
    let c := Expr.const (.str .anonymous nm) []
    let b := b.instantiate (#[c] ++ subst)
    if bi == .instImplicit then peel env b (#[c] ++ subst) vars hyps
    else if Ferrosa.Docs.Tex.isPropTy env t then peel env b (#[c] ++ subst) vars (hyps.push (code env t))
    else peel env b (#[c] ++ subst) (vars.push ("\\(" ++ Ferrosa.Docs.Tex.escape nm ++ "\\)")) hyps
  | _ => (vars, hyps, e)

/-- The statement in English: quantifiers, hypotheses, conclusion, in that order. -/
def desc (env : Environment) (ty : Expr) : String :=
  let (vars, hyps, body) := peel env ty #[] #[] #[]
  let quant :=
    if vars.isEmpty then ""
    else "For all " ++ String.intercalate " and " vars.toList ++ (if hyps.isEmpty then ": " else ", ")
  let ante :=
    if hyps.isEmpty then ""
    else "if " ++ String.intercalate " and " hyps.toList ++ ", then "
  let cons := String.intercalate ", and " ((conjuncts body).toList.map (fun c => code env c))
  quant ++ ante ++ cons ++ "."

end Ferrosa.Docs.Gloss
