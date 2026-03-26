# Cancel Safety Conventions

**Applies to**: all async code in the Ferrosa workspace (ferrosa-net, ferrosa-cql,
ferrosa-cluster, ferrosa-graph, ferrosa).

**Reference**: [Oxide RFD 400 — Cancel Safety Patterns and Anti-Patterns](https://rfd.shared.oxide.computer/rfd/400)

---

## Background

A Tokio future can be dropped at any `.await` point. Code that is **not cancel-safe**
leaves shared state partially updated when this happens — a leaked pending-response
slot, a held lock, or a half-sent message. These bugs are silent: the future simply
never resumes, and no error is returned to the caller.

---

## Principles

1. **Actors own state.** Mutable state that must survive cancellation lives inside a
   dedicated actor task (a spawned loop that owns the data and processes requests
   through a bounded channel). The caller's future never holds state directly, so
   dropping the future cannot corrupt anything.

2. **Tokens signal cancellation.** Long-running tasks (subscription polls,
   server loops, graph reconciliation) watch a `CancellationToken`. Cancelling the
   token is an instantaneous, idempotent side-channel signal — no `await` is needed,
   and the token fires even if the caller that created it has already been dropped.

3. **Reserve before sending.** When a future must enqueue work and then wait for a
   response, use `Sender::reserve().await` to acquire a send permit *before* any
   state is committed. If the future is dropped while waiting for the permit, nothing
   has been written yet. Once the permit is in hand, `permit.send(value)` is
   synchronous and cannot be cancelled.

---

## Decision Tree

When writing new async code, choose the right primitive:

```
Does the future mutate shared state across an .await?
├── YES → Actor: move state into a spawned loop; communicate via mpsc channel.
│
└── NO — does it need to be stopped from the outside?
    ├── YES, and it runs forever (server loop, poll loop) → CancellationToken.
    │
    └── YES, and it is a one-shot task → tokio::spawn returns a JoinHandle;
        use CancellationToken for graceful shutdown, not JoinHandle::abort().
```

---

## Approved Patterns

### Actor Request/Response

Use for anything that reads or writes shared mutable state across an await.

```rust
// In the actor task (owns state):
while let Some(req) = rx.recv().await {
    let result = process(&mut state, req.payload);
    let _ = req.reply.send(result);
}

// In the caller:
let (reply_tx, reply_rx) = oneshot::channel();
let permit = tx.reserve().await?;   // cancel-safe: nothing committed yet
permit.send(Request { payload, reply: reply_tx });
let result = reply_rx.await?;       // cancel-safe: actor still runs
```

### Cooperative Cancellation

Use for long-running tasks that must stop cleanly.

```rust
let cancel = CancellationToken::new();

// In the task:
loop {
    tokio::select! {
        _ = cancel.cancelled() => break,
        _ = ticker.tick() => { /* do work */ }
    }
}

// To stop:
cancel.cancel(); // instantaneous, no await needed
```

### Cancel-Safe Channel Send

Use `reserve`+`send` whenever a future enqueues work and then awaits a result.

```rust
let permit = tokio::select! {
    p = push_tx.reserve() => p?,
    _ = cancel.cancelled() => return,
};
permit.send(value); // synchronous, cannot be cancelled
```

### Timeout in Actor

When a caller needs a deadline, the timeout belongs inside the actor or is attached
to the oneshot receive, not wrapped around the entire `send` call chain.

```rust
// Caller acquires permit then races the reply with a timeout:
let permit = tx.reserve().await?;
permit.send(Request { payload, reply: reply_tx });
tokio::time::timeout(duration, reply_rx).await??
```

---

## Anti-Patterns

| Anti-pattern | Why it is wrong | Use instead |
|---|---|---|
| `tokio::sync::Mutex` held across `.await` | The lock is held while the future is suspended; a cancellation drop leaves it locked forever | Actor pattern — move the guarded state into a task |
| `JoinHandle::abort()` for cancellation | Aborts the task at an arbitrary `.await` with no cleanup | `CancellationToken` — lets the task run its own teardown |
| `mpsc::Sender::send().await` inside a `select!` branch | If another branch fires, the send is dropped mid-flight; the receiver never gets the value | `reserve()` before entering `select!`, then `permit.send()` inside the branch |
| `tokio::time::timeout()` wrapping a cancel-unsafe future | The future is dropped at the deadline while its side effects are still in progress | Put the timeout inside the actor, or ensure the wrapped future is cancel-safe first |
| Recreating futures in a `select!` loop | A fresh future on each loop iteration silently discards progress made in the previous iteration | Poll the future once outside the loop, or hold it in a `pin_mut!` variable |

---

## Documenting Cancel Safety

All public `async` functions and methods in the Ferrosa workspace **must** include a
`# Cancel Safety` section in their doc comment. This is enforced at code review.

### Format

```rust
/// Short description of what the function does.
///
/// # Cancel Safety
///
/// This method is [cancel-safe / **not** cancel-safe]. [One or two sentences
/// explaining the reason — what guarantee is provided, or what goes wrong if
/// the future is dropped early.]
pub async fn my_method(&self, ...) -> Result<...> {
```

### Examples

```rust
/// # Cancel Safety
///
/// This method is **not** cancel-safe. It inserts a pending-response slot before
/// sending the frame; dropping the future after insertion leaks the slot.
/// Only call from actor loops that guarantee the future runs to completion.

/// # Cancel Safety
///
/// This method is cancel-safe. Enqueue uses `reserve`+`send`; the response
/// arrives on a oneshot channel owned by the lane actor, which is unaffected
/// by the caller dropping its future.
```
