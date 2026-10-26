//! Synchronous schema lifecycle fencing for physical apply and restore.
//!
//! Readers may overlap. Mutations are exclusive and reentrant on the same
//! thread so a restore can fence storage retirement plus nested registry edits.
//! Never retain a guard across an await.
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::ThreadId;

#[derive(Default)]
struct State {
    readers: usize,
    writer: Option<(ThreadId, usize)>,
}

#[derive(Default)]
pub(crate) struct Lifecycle {
    state: Mutex<State>,
    changed: Condvar,
}

/// Owned lease preventing schema replacement during synchronous physical apply.
pub struct SchemaReadLease(Arc<Lifecycle>);

/// Exclusive schema/storage reconciliation guard. Bound to its acquiring thread.
pub struct SchemaWriteLease {
    lifecycle: Arc<Lifecycle>,
    _thread: PhantomData<Rc<()>>,
}

impl Lifecycle {
    pub(crate) fn read(self: &Arc<Self>) -> SchemaReadLease {
        let mut state = self.state.lock().unwrap();
        while state.writer.is_some() {
            state = self.changed.wait(state).unwrap();
        }
        state.readers += 1;
        SchemaReadLease(self.clone())
    }

    pub(crate) fn write(self: &Arc<Self>) -> SchemaWriteLease {
        let current = std::thread::current().id();
        let mut state = self.state.lock().unwrap();
        loop {
            match state.writer {
                Some((owner, depth)) if owner == current => {
                    state.writer = Some((owner, depth + 1));
                    break;
                }
                None if state.readers == 0 => {
                    state.writer = Some((current, 1));
                    break;
                }
                _ => state = self.changed.wait(state).unwrap(),
            }
        }
        SchemaWriteLease {
            lifecycle: self.clone(),
            _thread: PhantomData,
        }
    }
}

impl Drop for SchemaReadLease {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.readers -= 1;
        self.0.changed.notify_all();
    }
}

impl Drop for SchemaWriteLease {
    fn drop(&mut self) {
        let mut state = self.lifecycle.state.lock().unwrap();
        let (_, depth) = state.writer.as_mut().expect("write lease owns the writer");
        *depth -= 1;
        if *depth == 0 {
            state.writer = None;
        }
        self.lifecycle.changed.notify_all();
    }
}
