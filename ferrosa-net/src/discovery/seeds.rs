use super::Discovery;
use std::net::SocketAddr;

/// Static seed list parsed from CLI args or FERROSA_SEED env var.
pub struct SeedDiscovery {
    seeds: Vec<SocketAddr>,
}

impl SeedDiscovery {
    pub fn new(seeds: Vec<SocketAddr>) -> Self {
        Self { seeds }
    }

    pub fn parse(input: &str) -> Vec<SocketAddr> {
        input
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.parse().ok())
            .collect()
    }

    pub fn from_config(config: &crate::config::NetConfig) -> Self {
        Self::new(config.seeds.clone())
    }
}

impl Discovery for SeedDiscovery {
    fn peers(&self) -> Vec<SocketAddr> {
        self.seeds.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_seeds_from_comma_separated() {
        let seeds = SeedDiscovery::parse("10.0.1.5:7000,10.0.1.6:7000");
        assert_eq!(seeds.len(), 2);
        assert_eq!(seeds[0], "10.0.1.5:7000".parse().unwrap());
    }

    #[test]
    fn parse_seeds_empty_string() {
        let seeds = SeedDiscovery::parse("");
        assert!(seeds.is_empty());
    }

    #[test]
    fn parse_seeds_trims_whitespace() {
        let seeds = SeedDiscovery::parse(" 10.0.1.5:7000 , 10.0.1.6:7000 ");
        assert_eq!(seeds.len(), 2);
    }

    #[test]
    fn parse_seeds_skips_invalid() {
        let seeds = SeedDiscovery::parse("10.0.1.5:7000,not-an-addr,10.0.1.6:7000");
        assert_eq!(seeds.len(), 2);
    }
}
