# Release Evidence — Lote P2 Blockchain/Notification Guards

Date: 2026-02-26
Repo: brnd-v2-backend
Branch: codex/p2-blockchain-guards

## Scope
- src/core/blockchain/blockchain.controller.ts
- src/core/blockchain/blockchain.module.ts
- src/core/notification/notification.controller.ts
- src/core/notification/notification.module.ts
- src/core/blockchain/blockchain.controller.guards.spec.ts
- src/core/notification/notification.controller.guards.spec.ts

## Commands Executed
```bash
git fetch origin --prune
git worktree add /Users/gsus/projects/brnd/.wt-be-p2-blockchain -b codex/p2-blockchain-guards origin/main
npm install
npm run test -- --runInBand
npm run build
```

## Check Results
- npm install: OK
- npm run test -- --runInBand: OK (6 suites, 21 tests)
- npm run build: OK

## Security Changes Confirmed
- Blockchain cast webhook endpoint now guarded by `WebhookApiKeyGuard`.
- Blockchain debug NFT endpoints now guarded by `AdminGuard` + `DebugEndpointGuard`.
- Notification Farcaster webhook endpoint now guarded by `WebhookApiKeyGuard`.

## Release Metadata
- PR: TBD
- Merge SHA: TBD
- Railway deploy URL/log: TBD
- Smoke tests:
  - GET /notification-service/health: TBD
  - GET /embeds/health: TBD
