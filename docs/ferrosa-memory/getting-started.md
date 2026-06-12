# Ferrosa Memory Getting Started

Ferrosa Memory is a developer-preview memory layer for agents. It runs as an MCP-compatible service backed by Ferrosa Database, with a workbench on port `18765` and a graph visualization surface on port `18766`.

This guide shows how to run a local development stack, connect an agent harness, and use the core memory operations.

## Fast setup

For most users, start with the hosted setup scripts instead of cloning repositories manually.

Ferrosa Database only:

```bash
curl -fsSL https://ferrosadb.com/install.sh | bash
```

Ferrosa Memory plus agent onboarding:

```bash
curl -fsSL https://ferrosadb.com/setup-memory.sh | bash
```

`setup-memory.sh` downloads the onboarding prompt, optionally clones or updates the public repositories, offers to pull the local Nomic embedding model, and then hands the user to the selected LLM harness with:

```text
onboard me using ONBOARDING.md
```

Manual source setup is still useful for contributors and is shown below.

## What you will run

The minimal local setup can be just:

- one native Ferrosa Database process using local filesystem storage;
- one native Ferrosa Memory MCP/workbench process;
- optional local Nomic embeddings for semantic/vector search.

The fuller development stack adds:

- three Ferrosa Database nodes;
- MinIO for S3-compatible object storage;
- the same MCP/workbench and visualization surfaces used by local operator testing.

S3/MinIO is optional for the smallest local single-user setup. If you skip embeddings, Ferrosa Memory still works, but semantic/vector search quality is degraded; lexical, phonetic, direct lookup, and graph traversal remain available.

Default local ports:

| Service | URL / port |
|---|---|
| MCP/workbench | `http://127.0.0.1:18765/` |
| MCP JSON-RPC | `http://127.0.0.1:18765/mcp` |
| Viz | `http://127.0.0.1:18766/viz` |
| CQL | `127.0.0.1:19042-19044` |
| Graph HTTP | `http://127.0.0.1:17474-17476` |
| Bolt | `127.0.0.1:17687-17689` |
| MinIO, full stack only | `http://127.0.0.1:19000`, console `19001` |

## Prerequisites

Install:

- Git and curl
- Rust toolchain with Cargo for source/native builds
- Docker or Podman with Compose support only for the full development stack
- Ollama only if you want local Nomic embeddings
- Python 3 for optional smoke scripts

Check what you plan to use:

```bash
rustc --version
cargo --version
git --version
curl --version
python3 --version
ollama --version              # optional semantic search layer
docker compose version        # full Docker stack only
podman compose version        # full Podman stack only
```

## Optional Nomic embedding layer

For semantic/vector retrieval, install the local embedding model:

```bash
ollama pull nomic-embed-text-v2-moe
ollama list | grep nomic-embed-text-v2-moe
```

If you skip this step, warn users and test with lexical/phonetic queries first:

```text
Nomic embeddings disabled; semantic/vector search is degraded.
```

## 0.13 retrieval defaults

The 0.13 preview defaults keep normal agent turns compact while leaving evals enough depth to measure recall:

```toml
[retrieval]
default_limit = 10

[embeddings]
provider = "ollama"
model = "nomic-embed-text-v2-moe"
dimensions = 768

[eval]
retrieval_k = 25
```

Live retrieval uses `default_limit = 10` when an agent omits `limit` or `k`. Agents and users can lower this with the `config` MCP tool if a session is spending too many tokens on memory. Benchmarks should widen candidate generation in the eval runner rather than increasing the live default.

The best-known BRIGHT-Pro support-doc-closed MCP slice profile for 0.13 is:

```text
candidate_limit=50
fusion_profile=all
query_decomposition=llm
query_task=bright_pro
query_variant_limit=5
query_embed_variants=true
chunk_expansion=none
rerank=false
```

On the 200-document biology support-closed slice this measured alpha_nDCG `0.816`, NDCG `0.799`, aspect_recall `0.940`, and recall `0.796`. These are preview slice numbers; full-corpus paper comparisons require the full corpus to be ingested through the same MCP path.

Optional live judge/reranking is configured separately:

```toml
[judge]
enabled = false
provider = "ollama"
base_url = "http://127.0.0.1:11434"
model = "qwen2.5-coder:7b"
timeout_seconds = 60
```

Keep it disabled unless a local or remote model endpoint is available. If the judge fails or abstains, Ferrosa Memory records an abstention rather than a positive or negative judgment. Agent and user feedback can still use compact `+1` and `-1` item feedback to tune future rankings by workspace, query shape, and retrieval channel.

## Manual source setup

```bash
mkdir -p ~/src/ferrosa-suite
cd ~/src/ferrosa-suite

git clone https://github.com/ferrosadb/ferrosa.git ferrosa
git clone https://github.com/ferrosadb/ferrosa-memory.git ferrosa-memory
```

If you already have the repositories:

```bash
git -C ~/src/ferrosa-suite/ferrosa fetch --all --prune
git -C ~/src/ferrosa-suite/ferrosa-memory fetch --all --prune
```

## Native minimal mode

Build and run native binaries when you want the smallest local setup with filesystem storage and no S3/MinIO dependency:

```bash
cd ~/src/ferrosa-suite/ferrosa
cargo build --release

cd ~/src/ferrosa-suite/ferrosa-memory
cargo build --release
```

Use repository config examples or `setup-memory.sh`/`ONBOARDING.md` to choose ports, data directories, credentials, embedding settings, skills, hooks, and harness prompts.

## Full Compose development stack

The Compose stack is for local cluster/operator testing. It expects a local Ferrosa node image named `ferrosa-memory-node:latest`, a local MCP binary staged in `target-podman-linux/`, and generated runtime config under `.runtime/`.

The MCP service runs with `network_mode: host`, so the documented loopback HTTP smoke tests are valid for this compose deployment. Do not switch the MCP service to bridge networking without also changing the runtime config and smoke tests to TLS-aware `https://` checks.

Before starting Compose, ensure `~/data/ferrosa-memory/` is writable and that your Docker/Podman engine can mount paths under `$HOME`. The compose file bind-mounts `~/data/ferrosa-memory/{minio,node1,node2,node3}` for persistent data.

```bash
cd ~/src/ferrosa-suite/ferrosa
docker build -t ferrosa-memory-node:latest .
# or: podman build -t ferrosa-memory-node:latest .

cd ~/src/ferrosa-suite/ferrosa-memory
scripts/init-runtime.sh
make build-podman-binary
docker compose up -d
# or: podman compose up -d

docker compose ps
# or: podman compose ps
```

The database nodes should become healthy before the MCP service is considered ready.

## Health checks

```bash
curl -fsS http://127.0.0.1:18765/healthz/live && echo
curl -fsS http://127.0.0.1:18765/healthz/ready && echo
curl -fsS http://127.0.0.1:18766/viz | head -c 64 && echo
```

Expected:

```text
ok
ready
<!DOCTYPE html>
```

If your local configuration enables TLS for the MCP/workbench endpoint, use `https://127.0.0.1:18765` and the configured local CA/certificate options instead.

## Connect an MCP client

Ferrosa Memory exposes MCP over HTTP at:

```text
http://127.0.0.1:18765/mcp
```

Generic MCP server configuration shape:

```json
{
  "name": "ferrosa-memory",
  "transport": "http",
  "url": "http://127.0.0.1:18765/mcp",
  "headers": {
    "Authorization": "Basic <base64 username:password>"
  }
}
```

For a single-user local development stack, the example compose config may use local default credentials. For shared machines or networks, configure unique credentials and keep the service loopback-only or behind TLS and an authenticated proxy.

## Smoke-test with curl

List tools:

```bash
curl -sS -u ferrosa_user:ferrosa_user \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' \
  http://127.0.0.1:18765/mcp
```

Get stats:

```bash
curl -sS -u ferrosa_user:ferrosa_user \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_stats","arguments":{}}}' \
  http://127.0.0.1:18765/mcp
```

## Core memory examples

The exact tool names exposed to your agent may be namespaced by the client. The JSON below shows the intended tool and argument shapes.

### Ingest a memory entity

```json
{
  "tool": "smart_ingest",
  "arguments": {
    "entity_name": "Project migration rule",
    "entity_type": "decision",
    "content": "Schema migrations should be additive by default so live memory clusters can be upgraded without deleting data."
  }
}
```

### Retrieve related memory

```json
{
  "tool": "hybrid_search",
  "arguments": {
    "query": "what migration rule did we decide on for live memory clusters",
    "limit": 5
  }
}
```

### Record a temporal fact

```json
{
  "tool": "write_temporal_fact",
  "arguments": {
    "entity_id": "<entity UUID from smart_ingest>",
    "fact_text": "Current rule: prefer additive migrations for live memory clusters."
  }
}
```

### Follow the current temporal chain

```json
{
  "tool": "get_temporal_chain",
  "arguments": {
    "entity_id": "<entity UUID>"
  }
}
```

### Link two entities

```json
{
  "tool": "create_edge",
  "arguments": {
    "src_entity_id": "<source UUID>",
    "dst_entity_id": "<destination UUID>",
    "edge_type": "related_to",
    "weight": 0.8
  }
}
```

### Persist raw context segments

```json
{
  "tool": "ingest_context_segments",
  "arguments": {
    "conversation_id": "demo-conversation",
    "messages": [
      {"role": "user", "turn_index": 1, "content": "Remember that our migration policy is additive."},
      {"role": "assistant", "turn_index": 2, "content": "Stored as a project migration decision."}
    ]
  }
}
```

### Search raw context segments

```json
{
  "tool": "search_context_segments",
  "arguments": {
    "query": "migration policy additive",
    "limit": 5,
    "expand": {"prev": 1, "next": 1, "max_tokens": 2000}
  }
}
```

## Workbench and visualization

Open:

```text
http://127.0.0.1:18765/
http://127.0.0.1:18766/viz
```

Use the workbench to inspect:

- CQL tables and counts;
- memory summaries;
- graph, CQL, SPARQL, and Datalog query surfaces;
- the Judge Config page for optional local/remote model provider settings;
- aliases, rules, approvals, and explanations when enabled.

Use the viz page to inspect graph neighborhoods and entity links. Wait for graph queries to finish before taking screenshots or drawing conclusions from an empty view.

## Agent hooks and feedback

The onboarding flow can install Codex, Claude, and Hermes hooks. Those hooks capture session turns, working directory metadata, and compact retrieval feedback so memories learned in a repository can be preferred when future agents work from the same directory.

From a source checkout:

```bash
cd ~/src/ferrosa-suite/ferrosa-memory
./setup.sh --harness auto
```

or run the hook installer directly:

```bash
python3 scripts/install-agent-hooks.py --harness auto --verify
```

Agents should call `feedback` after retrieval when they can judge returned items. Use scores in result order: `1` for useful, `-1` for irrelevant or wrong, `0` for neutral, and `"-"` when a judge abstains or fails.

## Evaluation helpers

Ferrosa Memory includes helper scripts for BRIGHT-Pro and long-memory eval development:

```bash
# deterministic harness smoke test
scripts/run-official-evals.sh --self-test

# tune MCP fusion/decomposition profiles
FMEM_EVAL_QUERY_DECOMPOSITION=llm \
FMEM_EVAL_QUERY_EMBED_VARIANTS=true \
scripts/run-fusion-ablations.sh

# start a resumable full-corpus BRIGHT-Pro MCP ingest
scripts/start-bright-pro-full-load.sh
```

The full-corpus loader writes `heartbeat.json`, `progress.json`, and `load.log` under `diagnostics/eval-runs/...` so long runs can be monitored or resumed without attaching to a terminal.

## Safe stop and restart

Stop without deleting data:

```bash
cd ~/src/ferrosa-suite/ferrosa-memory
docker compose stop
# or
podman compose stop
```

Start again:

```bash
cd ~/src/ferrosa-suite/ferrosa-memory
docker compose start
# or
podman compose start
```

Recreate containers without deleting volumes after rebuilding an image:

```bash
cd ~/src/ferrosa-suite/ferrosa-memory
docker compose up -d --no-deps --force-recreate node1
```

Roll nodes one at a time and wait for each node to become healthy before moving to the next. Do not run `down -v` unless you intentionally want to delete all persisted memory data.

## Troubleshooting

| Symptom | What to check |
|---|---|
| `live` fails | MCP container is not running or the port is not mapped. |
| `ready` fails | CQL nodes are not healthy, auth is wrong, or contact points are unreachable. |
| Agent cannot list tools | MCP client config is wrong or the harness needs a restart/reload. |
| `get_stats` times out | Check node replay/OOM logs and CQL read timeouts. |
| Viz loads but graph is empty | Wait for the query to finish; verify memory rows exist; inspect filters. |
| MCP in a container cannot reach CQL | If config uses `localhost:19042`, run MCP with host networking or use container-routable contact points. |

Logs:

```bash
cd ~/src/ferrosa-suite/ferrosa-memory
docker compose logs --tail=100 node1 node2 node3 ferrosa-memory-mcp
# or
podman compose logs --tail=100 node1 node2 node3 ferrosa-memory-mcp
```

## Next steps

- Add Ferrosa Memory as an MCP server to your preferred agent harness.
- Run the ingest/search/temporal examples above.
- Use the workbench to verify the memory rows and graph links.
- Use the long-horizon evaluation scenarios in `ferrosa-memory` to measure retrieval quality, temporal accuracy, and evidence-packet usefulness before claiming long-horizon reasoning performance.
