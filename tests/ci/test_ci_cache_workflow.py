from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


def test_node_image_build_uses_and_verifies_action_managed_sccache():
    workflow = WORKFLOW.read_text(encoding="utf-8")
    node_job = workflow.split("  build-node-image:", 1)[1]
    node_job = node_job.split("\n  driver-smoke:", 1)[0]

    assert 'RUSTC_WRAPPER="$SCCACHE_PATH" cargo build --release' in node_job
    assert "- name: Verify sccache handled compiler requests" in node_job
    assert '"compile_requests"' in node_job
