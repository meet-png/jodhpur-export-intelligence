# Security Policy

This document describes the security posture of the Jodhpur Export Intelligence System (JEIS) and how to report a vulnerability.

## Reporting a vulnerability

If you discover a security issue, **please do not open a public GitHub issue**. Instead, email the maintainer:

> meetkabra149@gmail.com

Include:
- A clear description of the issue
- Steps to reproduce
- Affected commit SHA or release tag

You will receive an acknowledgment within 5 business days. Patches will be released as quickly as possible and credited to the reporter unless anonymity is requested.

## Threat model

JEIS is a single-tenant analytical pipeline that ingests *only public data* (UN Comtrade, Baker Hughes, IMD, Volza/Zauba free tiers). It stores no PII, no payment data, and no proprietary company information. The realistic attack surface is therefore narrow:

| Asset | Protected against | Mitigation |
|---|---|---|
| Supabase database password | Source-control leakage | `.env` is gitignored; `pre-commit` hook scans every commit for secret patterns; CI gate (planned) blocks pushes containing high-entropy strings |
| UN Comtrade API key | Source-control leakage | Same as above |
| Email alerting credentials (Gmail App Password) | Source-control leakage | Same as above |
| Production database integrity | Schema corruption / accidental data loss | All DDL is idempotent (`CREATE TABLE IF NOT EXISTS`); `init_db.py --drop-first` requires explicit flag and is never invoked by automation |
| Production database integrity | Unauthorized writes | Supabase Postgres credentials are scoped to a single role (`postgres`) used only by the pipeline. No app-server or end-user has direct DB access |
| Pipeline integrity | Malicious upstream data | Great Expectations validation suite runs before any data reaches the database. Validation failure halts the pipeline (PRD §FR-2) and preserves the previous-week dataset |
| Dependency supply chain | Compromised PyPI package | All Python dependencies pinned to exact versions in `requirements.txt`. Dependabot (planned) will alert on known CVEs |
| GitHub Actions runner | Secret exfiltration via malicious PR | Pipeline workflow runs only on `push` to `main` and `workflow_dispatch` from maintainers. PRs from forks do not trigger the secrets-bearing workflow |

## What is **not** in the threat model

- **End-user authentication** — JEIS has no UI and no end users beyond the maintainer
- **Multi-tenant isolation** — single-tenant deployment
- **Network-level DDoS protection** — Supabase and GitHub provide their own
- **PII handling** — there is no PII in the dataset

## Secret handling — operational rules

1. **Never** commit `.env`, `.env.local`, or any file matching `secrets/*`. The `.gitignore` excludes these; the `pre-commit` hook double-checks every commit.
2. **Never** paste the full `DATABASE_URL`, Comtrade key, or App Password into chat, screenshots, or issues. Redact with `XXXX` characters preserving length.
3. **Rotate credentials** if any of the following happens:
   - A laptop with `.env` is lost or stolen
   - A screenshot is shared that contains an unredacted secret
   - A `.env` file is accidentally committed (even if reverted — git history retains it)
4. **Supabase password rotation:** Project Settings → Database → "Reset database password" → update `.env` locally → update GitHub Actions secret `DATABASE_URL`.
5. **Comtrade key rotation:** comtradeplus.un.org → API Management → regenerate primary key.
6. **App Password rotation:** myaccount.google.com/apppasswords → revoke old, create new, update everywhere.

## Pre-commit secret scanning

The `.pre-commit-config.yaml` at the repo root configures [`gitleaks`](https://github.com/gitleaks/gitleaks) to scan every staged change for secret patterns (Postgres URIs, API keys, GitHub tokens, AWS keys, etc.) before allowing the commit. To install once:

```bash
pip install pre-commit
pre-commit install
```

After install, every `git commit` runs the scan automatically. If a secret pattern is detected the commit is rejected and you'll see exactly which line triggered.

## What the maintainer commits to

- Acknowledge security reports within 5 business days
- Patch confirmed vulnerabilities within 30 days, or sooner for critical issues
- Document mitigations and credit reporters in release notes
- Run an annual review of dependencies, RLS policies (if introduced), and threat-model assumptions
