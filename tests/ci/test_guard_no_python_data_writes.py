"""Tests for scripts/guard-no-python-data-writes.py.

Each case builds a throwaway git repo, so the guard runs against real
`git ls-files`, not a mock. The last case runs it on this repository.
"""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GUARD = REPO / "scripts" / "guard-no-python-data-writes.py"

ARG = 'ap.add_argument("--data-dir", required=True)\n'


def run_guard(root: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(GUARD), str(root)], capture_output=True, text=True
    )


def repo_with(files: dict) -> tempfile.TemporaryDirectory:
    tmp = tempfile.TemporaryDirectory()
    root = Path(tmp.name)
    subprocess.run(["git", "-C", str(root), "init", "-q"], check=True)
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
        subprocess.run(["git", "-C", str(root), "add", name], check=True)
    return tmp


class GuardTest(unittest.TestCase):
    def expect(self, files: dict, want: int, msg: str) -> None:
        with repo_with(files) as d:
            got = run_guard(Path(d))
            self.assertEqual(got.returncode, want, f"{msg}\n{got.stdout}{got.stderr}")

    def test_open_for_write_with_a_data_dir_arg_is_refused(self):
        self.expect({"scripts/x.py": ARG + 'open(p, "w").write("x")\n'}, 1, "open w")

    def test_os_open_with_write_flags_is_refused(self):
        body = ARG + "fd = os.open(m, os.O_WRONLY | os.O_CREAT, 0o644)\n"
        self.expect({"scripts/x.py": body}, 1, "os.open O_WRONLY")

    def test_subprocess_rm_is_refused(self):
        self.expect(
            {"scripts/x.py": ARG + 'subprocess.run(["rm", "-rf", d])\n'}, 1, "rm"
        )

    def test_path_mutators_and_shutil_are_refused(self):
        for call in ("p.write_text('x')", "p.unlink()", "shutil.rmtree(d)", "os.rename(a, b)"):
            self.expect({"scripts/x.py": ARG + call + "\n"}, 1, call)

    def test_a_nested_script_is_checked(self):
        self.expect({"scripts/sub/x.py": ARG + 'open(p, "ab")\n'}, 1, "nested")

    def test_a_marked_line_is_allowed(self):
        body = ARG + 'open(p, "w")  # data-dir-write-ok: unsupported reference\n'
        self.expect({"scripts/x.py": body}, 0, "line marker")

    def test_a_marker_on_the_data_dir_arg_waives_the_file(self):
        body = 'ap.add_argument("--data-root")  # data-dir-write-ok: scratch dir\nopen(p, "w")\n'
        self.expect({"scripts/x.py": body}, 0, "file waiver")

    def test_a_read_only_script_with_a_data_dir_arg_is_allowed(self):
        body = ARG + 'open(p, "rb").read()\nopen(q)\nname.replace("a", "b")\n'
        self.expect({"scripts/x.py": body}, 0, "read only")

    def test_a_writer_without_a_data_dir_arg_is_allowed(self):
        self.expect({"scripts/x.py": 'open("out.txt", "w")\n'}, 0, "no data dir arg")

    def test_a_commented_out_write_is_allowed(self):
        self.expect({"scripts/x.py": ARG + '# open(p, "w")\n'}, 0, "comment")

    def test_a_python_file_outside_scripts_is_not_checked(self):
        self.expect({"tools/x.py": ARG + 'open(p, "w")\n'}, 0, "outside scripts/")

    def test_a_non_checkout_cannot_be_scanned(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(run_guard(Path(d)).returncode, 2)

    def test_this_repository_is_clean(self):
        got = run_guard(REPO)
        self.assertEqual(got.returncode, 0, got.stdout + got.stderr)


if __name__ == "__main__":
    unittest.main()
