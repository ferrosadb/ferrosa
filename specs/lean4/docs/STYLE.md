# The voice of the book

The PDF is generated from the `/-` header and `/--` docstrings in `Ferrosa/`. Nothing in the
document is written twice: if a sentence is in the book, it is in the source, next to the thing it
describes, and it moves when the thing moves.

That only works if the source prose is written for the page.

## Who is speaking

A product manager who understands the system, writing for a customer who is deciding what to rely
on. Not a paper, not a commit message, not a code comment.

- **Say what the user can rely on.** "A write acknowledged at `quorum` survives the loss of one
  node" — not "this module models the acknowledgement path".
- **Lead with the consequence.** The first sentence of a header answers *why should I care*. The
  mechanics come second.
- **Name the failure.** Where the code does not meet the guarantee, say so plainly and say what the
  consequence is for a user.
- **No throat-clearing.** Never open with "This file", "This module", or "This section". The chapter
  already says which module it is.

## Register

| Do not write | Write |
|---|---|
| "This file connects the front end to the two layers already specified" | "A statement and a keyspace strategy are enough to decide how many replicas must acknowledge it." |
| "already specified", "as above", "below", "earlier", "the rest of the book" | name the module in backticks: `` `Topology/ConsistencyLevels.lean` `` — it becomes a link |
| "we", "I", "the author" | nobody. The specification speaks. |
| "obviously", "clearly", "simply", "just" | delete |
| "leverages", "utilises", "facilitates" | "uses", or a verb |
| Lean jargon as prose: "this theorem shows the ∀-quantified implication" | "every write that is acknowledged is also durable" |
| a bare `Prop` name in a sentence | in backticks: `` `Step` ``, `` `acked ≤ synced` `` |

Lean identifiers stay in backticks, always — that is what makes them set in mono and, where they name
a module, link. Names are not translated. A reader who wants the Lean finds it under every statement.

## What each part of the source contributes

**Module header (`/- ... -/` at the top of the file).** Two to five sentences. The first answers what
this module decides or guarantees. Then the one or two things that surprise a reader — a term that
had to be renamed, a property that holds only under a condition, a known gap. Ends before the first
`/-!` section. Do not restate the section headings; they follow immediately.

**Section heading (`/-! ## ... -/`).** A sentence, not a label. "The plan's properties" says nothing;
"What a plan must satisfy, and where the implementation's plan falls short" says what the section
will show. Sentence case, no trailing full stop.

**Theorem docstring (`/-- ... -/`).** One or two sentences stating what the theorem claims, in the
vocabulary the header just established. Where the theorem is red — it proves the *absence* of a
property — say what breaks and what a user sees. Do not describe the proof; do not name tactics;
do not say "we prove".

## The four things the build checks

`lake exe docs --check` fails on:

1. a theorem or lemma with no docstring;
2. a `/-!` section whose body is a heading and nothing else;
3. a statement whose mathematics and whose Lean are the same words — the translation has to add
   something or it should not be printed;
4. a red claim with no witness sentence.

Write so those four never fire. They are the difference between a specification and a pile of Lean.
