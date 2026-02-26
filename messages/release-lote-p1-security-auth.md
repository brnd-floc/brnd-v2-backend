# Release Evidence — Lote P1 Security/Auth

Date: 2026-02-26
Repo: brnd-v2-backend
Branch: codex/p1-security-auth

## Scope
- src/main.ts
- src/security/config.ts
- src/security/guards/admin.guard.ts
- src/security/guards/authorization.guard.ts
- src/security/guards/index.ts
- src/security/guards/webhook-api-key.guard.ts
- src/security/guards/debug-endpoint.guard.ts
- src/core/auth/services/auth.service.ts
- src/security/config.spec.ts
- src/security/guards/webhook-api-key.guard.spec.ts
- src/core/auth/services/auth.service.spec.ts

## Commands Executed
```bash
git fetch origin --prune
git worktree add /Users/gsus/projects/brnd/.wt-be-p1-security -b codex/p1-security-auth origin/main
npm install
npm run test -- --runInBand
npm run build
```

## Check Results
- npm install: OK
- npm run test -- --runInBand: OK (4 suites, 18 tests)
- npm run build: OK

## Release Metadata
- PR: TBD
- Merge SHA: TBD
- Railway deploy URL/log: TBD
- Smoke tests:
  - GET /notification-service/health: TBD
  - GET /embeds/health: TBD
