use std::future::Future;
use std::sync::Arc;

/// Explicit spawn target for subsystem-owned async work.
///
/// `TaskPool::current` is a named fallback for code paths that do not yet
/// receive a dedicated runtime. Production hot paths should prefer
/// `TaskPool::runtime` so work lands on a tunable subsystem runtime.
#[derive(Clone)]
pub struct TaskPool {
    name: &'static str,
    runtime: Option<Arc<tokio::runtime::Runtime>>,
}

impl TaskPool {
    pub fn current(name: &'static str) -> Self {
        Self {
            name,
            runtime: None,
        }
    }

    pub fn runtime(name: &'static str, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self {
            name,
            runtime: Some(runtime),
        }
    }

    pub fn from_optional_runtime(
        name: &'static str,
        runtime: Option<Arc<tokio::runtime::Runtime>>,
    ) -> Self {
        match runtime {
            Some(runtime) => Self::runtime(name, runtime),
            None => Self::current(name),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn runtime_ref(&self) -> Option<&tokio::runtime::Runtime> {
        self.runtime.as_deref()
    }

    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if let Some(runtime) = &self.runtime {
            runtime.spawn(future)
        } else {
            tokio::spawn(future)
        }
    }

    pub fn spawn_blocking<F, R>(&self, f: F) -> tokio::task::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if let Some(runtime) = &self.runtime {
            runtime.spawn_blocking(f)
        } else {
            tokio::task::spawn_blocking(f)
        }
    }
}
