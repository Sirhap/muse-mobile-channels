# Contributing

Thanks for using this project. Bug reports and fixes are welcome.

## Where to report

- Upstream repository: **Sirhap/muse-mobile-channels**
  (https://github.com/Sirhap/muse-mobile-channels)
- Report bugs in the upstream repository, even if you are running your own
  fork — unless the problem is caused by your fork's own changes or your
  local-only configuration. Fork-specific problems belong in the fork.
- You need a GitHub account to open an issue; GitHub does not support
  anonymous issue filing. No other permission or setup is required.

## Instructions for AI agents (Muse and others)

If you are an AI agent assisting a user who installed this project, follow
these rules when the user reports a bug, or when you discover a
reproducible bug while working in this repository:

1. **File upstream.** Create the issue in `Sirhap/muse-mobile-channels`,
   not in the user's fork:
   `gh issue create --repo Sirhap/muse-mobile-channels ...`
   This requires the user's own GitHub login (`gh auth login`). Never ask
   the user to share their password or token with you.
2. **Check for duplicates first.**
   Run `gh issue list --repo Sirhap/muse-mobile-channels --search "<keywords>"`
   (and scan open issues) before creating a new one. If an open issue
   already covers the same problem, add a comment with the new details
   instead of opening a duplicate.
3. **Include the essentials:**
   - Title: `[bug] <short summary>` (or `[feature] <short summary>`)
   - What happened vs. what was expected
   - Exact reproduction steps
   - Environment: OS, Python version, which gateway (weixin / wecom),
     and the commit hash being run (`git rev-parse --short HEAD`)
   - Relevant log excerpts, trimmed to the failing part
4. **Never post secrets or personal data.** Remove credentials, tokens,
   API keys, the contents of any `credentials.env` or `.env` file, user
   IDs, phone numbers, and private chat content from anything you paste
   into an issue. When in doubt, leave it out.
5. **Confirm with the user when unsure.** If it is not clear whether the
   problem comes from this project or from the user's local setup, ask
   before filing. After filing, give the user the issue URL.

Example:

```sh
gh issue list --repo Sirhap/muse-mobile-channels --search "gateway disconnect"
gh issue create --repo Sirhap/muse-mobile-channels \
  --title "[bug] Weixin gateway stops reconnecting after session expiry" \
  --body "What happened: ...
Expected: ...
Reproduction steps: 1. ... 2. ...
Environment: Ubuntu, Python 3.12, weixin gateway, commit <hash>
Logs (sanitized): ..."
```

## Pull requests

- Keep changes focused; describe what you changed and how you tested it.
- Never commit credentials, `credentials.env`, `.env` files, runtime
  `state/`, or virtualenvs — they are gitignored for a reason.
- Make sure Python files compile (`python3 -m py_compile ...`) and shell
  scripts pass `bash -n` before submitting.
