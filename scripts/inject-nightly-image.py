#!/usr/bin/env python3
"""
Inject `image: ${FERROSA_NIGHTLY_IMAGE:-ferrosa-nightly:latest}` above the
existing `build:` block for every Ferrosa node service in Docker Compose files.
Keeps the `build` block as a local fallback when the env var is unset.
"""
import re, sys

NODE_PATTERNS = [
    r'^  node\d+:\s*$',
    r'^  dc\d+-node\d+:\s*$',
]

def is_node_service(line):
    return any(re.match(p, line) for p in NODE_PATTERNS)

def process(text):
    lines = text.splitlines(keepends=True)
    out = []
    i = 0
    while i < len(lines):
        line = lines[i]
        out.append(line)
        if is_node_service(line):
            # Next non-blank line should be the build block at 4-space indent
            j = i + 1
            while j < len(lines) and lines[j].strip() == '':
                out.append(lines[j])
                j += 1
            if j < len(lines) and lines[j].strip() == 'build:':
                indent = '    '
                out.append(f'{indent}image: ${{FERROSA_NIGHTLY_IMAGE:-ferrosa-nightly:latest}}\n')
                # copy remaining lines as-is
                out.extend(lines[j:])
                return ''.join(out)
        i += 1
    return ''.join(out)

if __name__ == '__main__':
    for path in sys.argv[1:]:
        with open(path) as f:
            text = f.read()
        new_text = process(text)
        with open(path, 'w') as f:
            f.write(new_text)
        print(f'updated {path}')
