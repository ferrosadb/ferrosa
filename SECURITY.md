# Security Policy

## Supported Versions

Ferrosa is currently in developer preview. Only the latest tagged release receives
security updates.

| Version          | Supported          |
| ---------------- | ------------------ |
| latest tag       | :white_check_mark: |
| anything older   | :x:                |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Report vulnerabilities privately via one of these channels:

1. **GitHub Security Advisories** (preferred): https://github.com/ferrosadb/ferrosa/security/advisories/new
2. **Email**: security@ferrosadb.com

When reporting, please include:

- A description of the vulnerability and the impact
- Steps to reproduce, ideally with a minimal proof of concept
- The version, commit hash, or release tag where you observed the issue
- Any relevant configuration (replication factor, consistency level, auth setup, etc.)
- Whether the issue requires authenticated access, network access, or specific data shapes

## Response Process

We aim to:

- Acknowledge the report within **3 business days**
- Provide an initial assessment within **7 business days**
- Coordinate disclosure timing with the reporter once a fix is available

If you do not receive an acknowledgement within 3 business days, please follow up via
email.

## Disclosure Policy

We follow a **coordinated disclosure** model:

1. The reporter and Ferrosa maintainers agree on a disclosure timeline
2. A fix is prepared and tested privately
3. A patched release is published
4. A public security advisory is issued via GitHub Security Advisories with credit to
   the reporter (unless they prefer to remain anonymous)

We do not currently operate a paid bug bounty program. We are happy to publicly credit
researchers who report valid vulnerabilities.

## Scope

In scope:

- The Ferrosa engine and its public CQL, Bolt, HTTP, and admin protocols
- Authentication and authorization logic in `ferrosa-schema`
- Storage durability and recovery paths
- Cluster membership, Raft metadata, and internode communication
- Released binaries and the install script

Out of scope:

- Issues in third-party dependencies (please report upstream; we'll track and update)
- Denial of service via unbounded resource consumption that would require an
  authenticated client and existing infrastructure controls (rate limiting, quotas)
- Vulnerabilities in deployment-specific infrastructure (S3 bucket policies, network
  configuration, OS hardening)
- Self-hosted deployments running modified or unsupported builds
