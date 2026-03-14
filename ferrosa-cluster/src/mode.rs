#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentMode {
    Standalone,
    Pair,
    Cluster,
}
