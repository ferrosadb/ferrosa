from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SETUP_SCRIPT = REPO_ROOT / "docs" / "setup-memory.sh"
GETTING_STARTED = REPO_ROOT / "docs" / "ferrosa-memory" / "getting-started.md"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_setup_memory_defaults_use_canonical_ferrosadb_org_and_existing_onboarding_path():
    script = read(SETUP_SCRIPT)
    assert "https://github.com/ferrosadb/ferrosa-memory.git" in script
    assert "https://github.com/ferrosadb/ferrosa.git" in script
    assert (
        "https://raw.githubusercontent.com/ferrosadb/ferrosa-memory/main/ONBOARDING.md"
        in script
    )
    assert "github.com/bkearns/ferrosa" not in script
    assert "raw.githubusercontent.com/bkearns/ferrosa-memory" not in script


def test_getting_started_manual_clone_and_compose_steps_match_public_runtime_contract():
    guide = read(GETTING_STARTED)
    assert "git clone https://github.com/ferrosadb/ferrosa.git ferrosa" in guide
    assert (
        "git clone https://github.com/ferrosadb/ferrosa-memory.git ferrosa-memory"
        in guide
    )
    assert "scripts/init-runtime.sh" in guide
    assert "make build-podman-binary" in guide
    assert "~/data/ferrosa-memory/" in guide
    assert "curl -fsS http://127.0.0.1:18765/healthz/live" in guide


def test_getting_started_does_not_mix_bridge_tls_guidance_with_host_network_smoke_test():
    guide = read(GETTING_STARTED)
    compose_section = guide.split("## Full Compose development stack", 1)[1]
    assert "host-network" in compose_section or "network_mode: host" in compose_section
    assert "loopback" in compose_section
    assert "http://127.0.0.1:18765" in compose_section
