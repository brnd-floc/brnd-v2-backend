# Release Evidence — Lote P3C1 Indexer Token Sync

Date: 2026-02-26
Repo: brnd-v2-backend
Branch: codex/p3c1-indexer-token-sync

## Scope
- src/core/blockchain/services/indexer-sync.service.ts
- src/utils/neynar/api.ts

## Commands Executed
```bash
git fetch origin --prune
git worktree add /Users/gsus/projects/brnd/.wt-be-p3c1-sync -b codex/p3c1-indexer-token-sync origin/main
npm install
npm run test -- --runInBand
npm run build
```

## Check Results
- npm install: OK
- npm run test -- --runInBand: OK (7 suites, 28 tests)
- npm run build: OK

## Release Metadata
- PR: TBD
- Merge SHA: TBD
- Railway deploy ID/log: TBD
- Smoke tests:
  - GET /notification-service/health: TBD
  - GET /embeds/health: TBD
