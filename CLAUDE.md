# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Ferrosa is a wrapper repository containing Apache Cassandra as a git submodule (`cassandra/` → `git@github.com:apache/cassandra.git`). The actual source code lives inside the `cassandra/` directory.

- **Cassandra version**: 5.1
- **Language**: Java
- **Supported JDKs**: 11, 17, 21 (11 is default)
- **Build tool**: Apache Ant 1.10+

## Common Commands

All commands run from `cassandra/`:

```bash
cd cassandra

# Build
ant build                    # Compile classes
ant jar                      # Create JAR

# Tests
ant test                     # Run unit tests
ant testsome -Dtest.name=org.apache.cassandra.service.StorageServiceServerTest  # Single test class
ant testsome -Dtest.name=MyTest -Dtest.methods=testFoo,testBar  # Specific methods
ant test-jvm-dtest           # JVM-based distributed tests
ant long-test                # Long-running tests

# Code quality
ant check                    # Verify source code (pre-commit)
.build/check-code.sh         # Full code checks (checkstyle, RAT, OWASP)

# Clean
ant clean                    # Remove build artifacts
ant realclean                # Remove entire build directory
```

## Architecture

Source is under `cassandra/src/java/org/apache/cassandra/`:

| Package | Purpose |
|---------|---------|
| `cql3/` | CQL query language implementation |
| `db/` | Storage engine core |
| `config/` | Configuration management |
| `dht/` | Distributed hash table / partitioning |
| `gms/` | Gossip protocol (cluster membership) |
| `net/` | Networking / internode messaging |
| `service/` | Core services (StorageService, etc.) |
| `repair/` | Anti-entropy repair |
| `auth/` | Authentication & authorization |
| `schema/` | Schema management |

Tests mirror this structure under `cassandra/test/`:
- `unit/` — JUnit unit tests
- `distributed/` — JVM-based distributed tests (mocked clusters)
- `burn/` — Stress tests
- `long/` — Long-running tests
- `microbench/` — JMH benchmarks
- `harry/` — Property-based testing

Cassandra also has an `accord` submodule under `cassandra/modules/accord/` for distributed transaction consensus.

## Checkstyle Rules

Key enforced conventions (`.build/checkstyle.xml`):

- **Clock**: Use `org.apache.cassandra.utils.Clock.Global`, not `System.currentTimeMillis()` / `System.nanoTime()` / `Instant.now()`
- **Executors**: Use `org.apache.cassandra.concurrent.ExecutorFactory.Global`, not `java.util.concurrent.Executors` directly
- Suppressible with inline comments: `// checkstyle: permit this import`, `// checkstyle: permit this instantiation`, `// checkstyle: permit this invocation`

## Branch Naming Convention

Feature branches: `your-name/CASSANDRA-#####/base-branch`

## Commit Message Format

```
<One sentence description>

<Optional longer description>

patch by <Authors>; reviewed by <Reviewers> for CASSANDRA-#####
```
