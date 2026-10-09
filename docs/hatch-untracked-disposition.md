# P3#11 ignore note

Repo-side only. Hatch file moves and deletes are done via Muse. This change does not clean the hatch disk.

`.gitignore` ignores these untracked dumps:

- `native-probe/probe*_results.txt`
- `native-probe/probe*_stdout.log`
- `native-probe/probe*_*.log`
- `channel-restore/outbound-probe-draft-*.md`

`native-probe/gw2.py`, `native-probe/README.md`, and other tracked native-probe dependencies are not matched by those patterns. Probe scripts (`*.py`) stay eligible to commit.
