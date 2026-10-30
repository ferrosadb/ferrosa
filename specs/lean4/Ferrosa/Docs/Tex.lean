import Lean

/-!
# LaTeX for Lean terms

The book prints theorems as mathematics. That means the term has to be *translated*, not quoted:
`<.primary, .secondary, false, true>` becomes `⟨primary, secondary, false, true⟩`, `∧` stays `∧`,
`Writable` becomes `\mathrm{Writable}`, and an implicit type argument is dropped because a reader
does not want to see it.

Two decisions worth stating, because they cost something:

* **Implicit and instance arguments are dropped.** A Lean statement carries its type parameters and
  its instances; a mathematical one does not. The printer walks the head constant's binders in step
  with the arguments and keeps only the explicit ones.
* **Hypotheses become implications.** A binder whose type is a proposition is a hypothesis, so it is
  printed as `P → Q` rather than `∀ (h : P), Q`. Whether a type is a proposition is decided without
  reduction, by walking to the head of the type: `Prop` is `Sort 0`, and a constant is a proposition
  when its own type reduces to one in this sense. That is cheap and covers every relation in the
  corpus (`Writable : Pair → Node → Prop`), which is all it has to do.

Everything here is pure: no `MetaM`, no elaborator state. The input is an `Expr` from a compiled
environment and the output is a string of LaTeX.
-/

namespace Ferrosa.Docs.Tex

open Lean

/-! ## Names -/

/-- Escape the characters LaTeX reads as syntax. -/
def escape (s : String) : String :=
  String.join <| s.toList.map fun c =>
    match c with
    | '_' => "\\_"
    | '\'' => "\\prime"
    | _ => c.toString

/-- Greek letters as LaTeX commands. A bare `τ` in a name reaches the math font, which in this
    document does not carry Greek, and prints as nothing at all. -/
def greek (c : Char) : Option String :=
  match c with
  | 'α' => some "\\alpha"   | 'β' => some "\\beta"    | 'γ' => some "\\gamma"
  | 'δ' => some "\\delta"   | 'ε' => some "\\epsilon" | 'ζ' => some "\\zeta"
  | 'η' => some "\\eta"     | 'θ' => some "\\theta"   | 'ι' => some "\\iota"
  | 'κ' => some "\\kappa"   | 'λ' => some "\\lambda"  | 'μ' => some "\\mu"
  | 'ν' => some "\\nu"      | 'ξ' => some "\\xi"      | 'π' => some "\\pi"
  | 'ρ' => some "\\rho"     | 'σ' => some "\\sigma"   | 'τ' => some "\\tau"
  | 'υ' => some "\\upsilon" | 'φ' => some "\\phi"     | 'χ' => some "\\chi"
  | 'ψ' => some "\\psi"     | 'ω' => some "\\omega"   | 'Γ' => some "\\Gamma"
  | 'Δ' => some "\\Delta"   | 'Θ' => some "\\Theta"   | 'Λ' => some "\\Lambda"
  | 'Π' => some "\\Pi"      | 'Σ' => some "\\Sigma"   | 'Φ' => some "\\Phi"
  | 'Ψ' => some "\\Psi"     | 'Ω' => some "\\Omega"
  | _ => none

/-- Replace every Greek letter in a name by its command. -/
def greekize (s : String) : String :=
  String.join (s.toList.map (fun c => (greek c).getD c.toString))

/-- The last component of a name, set as a formula: `Readable` is a \texttt{\textbackslash mathrm}
    word, `primary` a \texttt{\textbackslash mathsf} one. That split is the corpus's own: capitalised
    names are predicates and types, lowercase ones are constructors and alternatives. -/
def texName (n : Name) : String :=
  let s := greekize n.getString!
  let qualified := n.components.length > 1
  match s.toList.head? with
  | some c =>
    if c.isLower then
      -- an unqualified lowercase name is a bound variable, not a constructor: italic, not sans
      (if qualified then "\\mathsf{" else "\\mathit{") ++ escape s ++ "}"
    else "\\mathrm{" ++ escape s ++ "}"
  | none => "\\_"

/-! ## Is a type a proposition?

  `Prop` is `Sort 0`, so this is syntactic. For a constant, look at its type and try again, bounded,
  because the corpus has no definition that hides a proposition behind more than a few applications.
-/
/-- Head constants that are propositions by construction. The environment walk below finds these
    anyway when it succeeds; naming them here means a hypothesis is printed as an implication even
    when the walk is defeated by an instance argument it cannot see through. -/
def propHeads : Array String :=
  #["Eq", "Ne", "And", "Or", "Not", "Iff", "Exists", "Subtype", "LT.lt", "LE.le",
    "Membership.mem", "Iff", "Or", "Dvd.dvd"]

/-- Constants that look like projections and are not: `OfNat.ofNat`, `HAdd.hAdd`, `Neg.neg`. They
    are class methods, and printing `1.ofNat` for the numeral 1 is worse than printing nothing. -/
def classMethod (n : Name) : Bool :=
  let s := n.toString
  ["OfNat", "OfScientific", "OfDecimal", "Neg", "HAdd", "HSub", "HMul", "HDiv", "HMod", "HPow",
   "HAnd", "HOr", "Append", "LT", "LE", "GT", "GE", "Membership", "Dvd", "Inter", "Union",
   "Subset", "Decidable", "Coe", "CoeT", "OfCoe", "Nat.add", "Nat.mul", "Nat.sub",
   "Nat.mod", "Nat.div", "Nat.pow", "Bool.and", "Bool.or", "Bool.not"].any (fun p =>
    s == p || s.startsWith (p ++ "."))

partial def isPropTy (env : Environment) (e : Expr) (depth : Nat := 8) : Bool :=
  if depth == 0 then false else
  match e with
  | .forallE _ _ b _ => isPropTy env b (depth - 1)
  | .sort lvl => lvl == .zero
  | .app f _ =>
    match f with
    | .const n _ => propHeads.contains n.toString || isPropTy env f (depth - 1)
    | _ => isPropTy env f (depth - 1)
  | .const n _ =>
    propHeads.contains n.toString ||
      (match env.find? n with
       | some ci => isPropTy env ci.type (depth - 1)
       | none => false)
  | _ => false

/-! ## Symbols -/

/-- The infix spelling of a constant, when it has one. Matching is on the suffix so that
    `LE.le`, `Nat.le` and a locally opened `le` all land on the same glyph. -/
def infix? (n : Name) : Option String :=
  let s := n.toString
  if s.endsWith "Eq" && !s.endsWith "Ne" then some "="
  else if s.endsWith "Ne" then some "\\ne"
  else if s.endsWith "Iff" then some "\\leftrightarrow"
  else if s.endsWith "And" then some "\\wedge"
  else if s.endsWith "Or" then some "\\vee"
  else if s.endsWith "LT.lt" || s == "LT.lt" then some "<"
  else if s.endsWith "LE.le" || s == "LE.le" then some "\\le"
  else if s.endsWith "GT.gt" || s == "GT.gt" then some ">"
  else if s.endsWith "GE.ge" || s == "GE.ge" then some "\\ge"
  else if s.endsWith "Membership.mem" then some "\\in"
  else if s.endsWith "HAdd.hAdd" then some "+"
  else if s.endsWith "HSub.hSub" then some "-"
  else if s.endsWith "HAdd.hAdd" || s.endsWith "Nat.add" then some "+"
  else if s.endsWith "HSub.hSub" || s.endsWith "Nat.sub" then some "-"
  else if s.endsWith "HMul.hMul" || s.endsWith "Nat.mul" then some "\\cdot"
  else if s.endsWith "Append.append" then some "\\mathbin{{+}{+}}"
  else if s.endsWith "HAnd.hAnd" then some "\\wedge"
  else if s.endsWith "HOr.hOr" then some "\\vee"
  else if s.endsWith "HDiv.hDiv" then some "/"
  else none

/-! ## Applications -/

/-- The arguments of a constant applied to a prefix of its binders: the explicit ones only. When the
    head's type runs out of binders before the arguments do — which happens with polymorphic
    instances — the remaining arguments are kept, since dropping one would change the meaning. -/
partial def explicitArgs (env : Environment) (head : Name) (args : Array Expr) : Array Expr :=
  match env.find? head with
  | none => args
  | some ci =>
    let rec go (ty : Expr) (i : Nat) (acc : Array Expr) : Array Expr :=
      if i >= args.size then acc
      else match ty with
        | .forallE _ _ body bi =>
          if bi == .implicit || bi == .instImplicit then go body (i + 1) acc
          else go body (i + 1) (acc.push args[i]!)
        | _ => acc ++ args[i:]
    go ci.type 0 #[]

mutual
partial def tex (env : Environment) (e : Expr) : String :=
  match e with
  | .sort lvl => if lvl == .zero then "\\mathrm{Prop}" else "\\mathrm{Type}"
  | .lit (.natVal n) => toString n
  | .lit (.strVal s) => "\\text{\\texttt{" ++ s ++ "}}"
  | .const n _ =>
    match n with
    | .num _ _ => texName n
    | _ => if n == ``True then "\\top" else if n == ``False then "\\bot" else texName n
  | .forallE n ty b bi =>
    if bi == .implicit || bi == .instImplicit then tex env b
    else if isPropTy env ty then s!"{tex env ty} \\to {tex env b}"
    else s!"\\forall {greekize (escape n.getString!)} : {tex env ty},\\ {tex env b}"
  | .lam n ty b bi =>
    if bi == .implicit || bi == .instImplicit then tex env b
    else
      let nm := if n.toString == "_" then "x" else n.getString!
      let c := Expr.const (.str .anonymous nm) []
      s!"\\lambda {greekize (escape nm)} : {tex env ty},\\ {tex env (b.instantiate1 c)}"
  | .letE n _ v b _ => s!"{tex env b}"
  | .bvar i => s!"x\\_{i}"
  | .fvar id => s!"v_{id.name.getString!}"
  | .app .. => texApp env e
  | .mvar _ => "\\square"
  | .proj _ _ _ => "\\pi"
  | .mdata _ b => tex env b

/-- Applications, in the three shapes that matter: a known infix, a structure literal, and a plain
    function application. -/
partial def texApp (env : Environment) (e : Expr) : String :=
  let head := e.getAppFn
  let args := e.getAppArgs
  match head with
  | .const n _ =>
    if n.toString.endsWith "Not" && args.size >= 1 then
      s!"\\neg {tex env args[args.size - 1]!}"
    else if n.toString.endsWith "Exists" && args.size >= 2 then
      match args[args.size - 1]! with
      | .lam nm _ b _ =>
        let nm := if nm.toString == "_" then "x" else nm.getString!
        let c := Expr.const (.str .anonymous nm) []
        s!"\\exists {greekize (escape nm)},\\ {tex env (b.instantiate1 c)}"
      | other => s!"\\exists {tex env other}"
    else if let some op := infix? n then
      if args.size >= 2 then
        s!"{tex env args[args.size - 2]!} {op} {tex env args[args.size - 1]!}"
      else texName n
    else if n.toString.endsWith ".mk" && args.size > 0 then
      -- a structure literal: positional, with the fields printed as the structure declares them
      let keep := explicitArgs env n args
      let vals := keep.map (fun a => tex env a)
      "\\langle " ++ String.intercalate ", " vals.toList ++ " \\rangle"
    else if n.toString.endsWith "OfNat.ofNat" && args.size >= 2 then tex env args[1]!
    else if n.toString.endsWith "OfScientific.ofScientific" && args.size >= 2 then
      match args[1]! with
      | .lit (.natVal m) => toString m
      | _ => tex env args[1]!
    else if env.find? n |>.any (fun _ => false) then texName n
    else if (explicitArgs env n args).size == 1 && n.components.length >= 2
            && !classMethod n
            && (n.getString!.toList.head?).any (fun c => c.isLower) then
      -- a projection: `s.acked` reads as mathematics where `acked(s)` reads as code
      tex env (explicitArgs env n args)[0]! ++ ".\\mathsf{" ++ escape n.getString! ++ "}"
    else
      match n with
      | .str _ "OfNat" => ""
      | _ =>
        if n.toString.endsWith "OfNat.ofNat" && args.size >= 2 then tex env args[1]!
        else
          let keep := explicitArgs env n args
          if keep.isEmpty then texName n
          else texName n ++ "(" ++ String.intercalate ",\\," (keep.toList.map (tex env)) ++ ")"
  | _ => s!"\\left({tex env head}\\right)(" ++ String.intercalate ",\\," (args.toList.map (tex env)) ++ ")"
end

/-! ## Statements -/

/-- Split a proposition at its top-level conjunctions, so the document can break the line between
    them. This is what keeps a five-line statement inside the frame. -/
partial def conjuncts (e : Expr) : Array Expr :=
  match e.getAppFn with
  | .const n _ =>
    if n.toString.endsWith "And" && e.getAppArgs.size >= 2 then
      let a := e.getAppArgs
      conjuncts a[a.size - 2]! ++ conjuncts a[a.size - 1]!
    else #[e]
  | _ => #[e]

/-- A theorem's type as an `aligned` block: the quantified variables, then one line per conjunct of
    the conclusion. `\quad` and a leading `\wedge` on continuation lines, which is how the claim
    reads aloud. -/
partial def texStatement (env : Environment) (ty : Expr) : String :=
  -- Bound variables have to become something the printer can name. Each binder is replaced by a
  -- constant carrying its own name, so the body prints `Step(s, i, s')` rather than `Step(x_2,
  -- x_1, x_0)`. Substitution runs down the telescope, so a later binder's *type* can mention an
  -- earlier binder.
  let rec peel (e : Expr) (subst : Array Expr) (vars : Array String) (hyps : Array String) :
      Array String × Array String × Expr :=
    match e with
    | .forallE n t b bi =>
      let nm := n.getString!
      let nm := if nm == "_" || nm.isEmpty then "x" else nm
      let t := t.instantiate subst
      let c := Expr.const (.str .anonymous nm) []
      let b := b.instantiate (#[c] ++ subst)
      -- Implicit *variables* still get quantified: a reader does not care that Lean inferred the
      -- type, but the variable appears in the statement and has to be bound. Only typeclass
      -- arguments are dropped, because they are proof-irrelevant machinery.
      if bi == .instImplicit then peel b (#[c] ++ subst) vars hyps
      else if isPropTy env t then peel b (#[c] ++ subst) vars (hyps.push (tex env t))
      else peel b (#[c] ++ subst) (vars.push (greekize (escape nm))) hyps
    | _ => (vars, hyps, e)
  let (vars, hyps, body) := peel ty #[] #[] #[]
  let quant :=
    if vars.isEmpty then "" else "\\forall " ++ String.intercalate "\\ " vars.toList ++ ",\\ "
  let lines : List String := (conjuncts body).toList.map (fun c => tex env c)
  let withHyps :=
    if hyps.isEmpty then lines
    else
      -- hypotheses all attach to the first conjunct
      match lines with
      | first :: rest => (String.intercalate " \\to " hyps.toList ++ " \\to " ++ first) :: rest
      | [] => lines
  match withHyps with
  | [] => quant
  | first :: rest =>
    let tail := rest.map (fun l => "& \\wedge " ++ l)
    "\\begin{aligned}[t] & " ++ quant ++ first ++
      (if tail.isEmpty then "" else " \\\\ " ++ String.intercalate " \\\\ " tail) ++
      "\\end{aligned}"

end Ferrosa.Docs.Tex
