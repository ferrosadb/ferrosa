#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyLevel {
    Quorum,
}

impl ConsistencyLevel {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(_s: &str) -> Option<Self> {
        None
    }
}
