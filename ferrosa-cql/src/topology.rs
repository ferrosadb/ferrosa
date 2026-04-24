//! Connection-aware topology advertisement policy for mixed deployments.
//!
//! Ferrosa exposes peer topology through `system.local` / `system.peers_v2`.
//! Generic Cassandra drivers expect those tables to advertise one canonical
//! address family per connection. In mixed host/container deployments, host
//! clients often need the host-published CQL address while in-cluster clients
//! need the internode/container address family instead.

use std::net::{IpAddr, SocketAddr};

use ferrosa_schema::system::TopologyView;

/// Parsed policy for deciding which topology view a client should see.
#[derive(Debug, Clone, Default)]
pub struct ClientTopologyPolicy {
    internal_client_cidrs: Vec<Cidr>,
}

impl ClientTopologyPolicy {
    /// Parse a comma-separated CIDR list.
    pub fn from_csv(raw: &str) -> Result<Self, String> {
        let mut internal_client_cidrs = Vec::new();
        for entry in raw
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
        {
            internal_client_cidrs.push(entry.parse()?);
        }
        Ok(Self {
            internal_client_cidrs,
        })
    }

    /// Returns true when no internal-client networks are configured.
    pub fn is_empty(&self) -> bool {
        self.internal_client_cidrs.is_empty()
    }

    /// Select the topology view for a parsed client IP.
    pub fn topology_view_for_ip(&self, ip: IpAddr) -> TopologyView {
        if self
            .internal_client_cidrs
            .iter()
            .any(|cidr| cidr.contains(ip))
        {
            TopologyView::Internal
        } else {
            TopologyView::Public
        }
    }

    /// Select the topology view for a client address string.
    ///
    /// `client_address` is stored as `"ip:port"` by the CQL connection layer.
    /// Parse failures fall back to the public view.
    pub fn topology_view_for_client_address(&self, client_address: &str) -> TopologyView {
        self.topology_view_for_client_with_locals(client_address, &[])
    }

    /// Select the topology view for a client address string with awareness of
    /// the local node's internal addresses.
    ///
    /// Podman host-to-container NAT on macOS can make a host client appear to
    /// the server as the node's own container IP. Treating that as internal
    /// breaks mixed host/container deployments because the host then receives
    /// `system.local` / `system.peers` endpoints that are only valid on the
    /// container network. When the parsed client IP matches one of the local
    /// addresses, force the public view.
    pub fn topology_view_for_client_with_locals(
        &self,
        client_address: &str,
        local_addresses: &[IpAddr],
    ) -> TopologyView {
        if let Ok(addr) = client_address.parse::<SocketAddr>() {
            if local_addresses.contains(&addr.ip()) {
                return TopologyView::Public;
            }
            return self.topology_view_for_ip(addr.ip());
        }
        if let Ok(ip) = client_address.parse::<IpAddr>() {
            if local_addresses.contains(&ip) {
                return TopologyView::Public;
            }
            return self.topology_view_for_ip(ip);
        }
        TopologyView::Public
    }
}

/// Minimal CIDR representation without an external dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cidr {
    network: IpAddr,
    prefix_len: u8,
}

impl std::str::FromStr for Cidr {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let (ip_raw, prefix_raw) = raw
            .split_once('/')
            .ok_or_else(|| format!("invalid CIDR '{raw}'"))?;
        let network: IpAddr = ip_raw
            .parse()
            .map_err(|_| format!("invalid CIDR network '{raw}'"))?;
        let prefix_len: u8 = prefix_raw
            .parse()
            .map_err(|_| format!("invalid CIDR prefix '{raw}'"))?;
        let max_prefix = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix_len > max_prefix {
            return Err(format!("CIDR prefix out of range '{raw}'"));
        }
        Ok(Self {
            network,
            prefix_len,
        })
    }
}

impl Cidr {
    fn contains(self, ip: IpAddr) -> bool {
        match (self.network, ip) {
            (IpAddr::V4(network), IpAddr::V4(ip)) => {
                let mask = ipv4_mask(self.prefix_len);
                (u32::from(network) & mask) == (u32::from(ip) & mask)
            }
            (IpAddr::V6(network), IpAddr::V6(ip)) => {
                let mask = ipv6_mask(self.prefix_len);
                (u128::from(network) & mask) == (u128::from(ip) & mask)
            }
            _ => false,
        }
    }
}

fn ipv4_mask(prefix_len: u8) -> u32 {
    if prefix_len == 0 {
        0
    } else {
        u32::MAX << (32 - prefix_len)
    }
}

fn ipv6_mask(prefix_len: u8) -> u128 {
    if prefix_len == 0 {
        0
    } else {
        u128::MAX << (128 - prefix_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn client_in_internal_cidr_gets_internal_view() {
        let policy = ClientTopologyPolicy::from_csv("10.89.0.0/16,fd00::/8").unwrap();

        assert_eq!(
            policy.topology_view_for_ip("10.89.1.44".parse().unwrap()),
            TopologyView::Internal
        );
        assert_eq!(
            policy.topology_view_for_ip("fd00::42".parse().unwrap()),
            TopologyView::Internal
        );
    }

    #[test]
    fn client_outside_internal_cidrs_gets_public_view() {
        let policy = ClientTopologyPolicy::from_csv("10.89.0.0/16").unwrap();

        assert_eq!(
            policy.topology_view_for_client_address("127.0.0.1:54321"),
            TopologyView::Public
        );
    }

    #[test]
    fn client_that_looks_like_local_container_ip_gets_public_view() {
        let policy = ClientTopologyPolicy::from_csv("10.89.0.0/16").unwrap();

        assert_eq!(
            policy.topology_view_for_client_with_locals(
                "10.89.1.6:35628",
                &["10.89.1.6".parse().unwrap()]
            ),
            TopologyView::Public
        );
    }

    #[test]
    fn invalid_cidr_is_rejected() {
        let err = ClientTopologyPolicy::from_csv("10.89.0.0/99").unwrap_err();
        assert!(err.contains("out of range"));
    }

    #[test]
    fn empty_csv_yields_empty_policy() {
        let policy = ClientTopologyPolicy::from_csv("").unwrap();
        assert!(policy.is_empty());
        assert_eq!(
            policy.topology_view_for_ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            TopologyView::Public
        );
    }

    #[test]
    fn mask_helpers_handle_full_prefixes() {
        assert_eq!(ipv4_mask(32), u32::MAX);
        assert_eq!(ipv6_mask(128), u128::MAX);
        assert_eq!(ipv4_mask(0), 0);
        assert_eq!(ipv6_mask(0), 0);
    }
}
