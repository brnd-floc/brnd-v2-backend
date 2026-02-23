# Cutover Runbook (`brnd.land` -> v2 stack)

## Scope
- Backend: `brnd-v2-backend`
- Frontend: `brnd-v2-frontend`
- Goal: release v2 stack with deterministic prechecks, gates and rollback.

## Release freeze
- Frontend RC tag: `rc-miniapp-hardening-1`
- Backend RC tag: `rc-miniapp-hardening-1`

## Phase A: Pre-release checks

### 1) Verify API contract + baseline JSON

```bash
npm run cutover:verify-api -- \
  --api-base=https://api.brndland.com \
  --brand-ids=431,428,1 \
  --strict-category \
  --out=./tmp/cutover-baseline-pre.json
```

This validates:
- guardian contract fields (`guardianFid`, `guardianHandle`, `guardianPfp`)
- token aliases (`tokenTicker`, `tokenContractAddress`)
- `/brand-service/brand/:id` and `/brand-service/brand/:id/enhanced`
- health endpoint equivalent (`/embeds/health` by default)
- category numeric-name risk (`"13"` style values)

### 2) Audit category integrity in DB

```bash
npm run cutover:audit-categories
```

This reports:
- numeric category names in `categories`
- brands currently mapped to numeric categories
- duplicate category names by normalized value

### 2.1) Fix category `13` name (if needed)

```bash
# dry run
npm run cutover:fix-category-name

# apply (default target: id=13 -> "General")
npm run cutover:fix-category-name -- --apply
```

### 3) Optional token backfill

```bash
# dry run
bun run scripts/backfill-brand-token-fields.ts --dry-run

# apply
bun run scripts/backfill-brand-token-fields.ts --apply
```

## Phase B: Backend deployment gate

After backend deployment to target environment, rerun:

```bash
npm run cutover:verify-api -- \
  --api-base=https://api.brndland.com \
  --brand-ids=431,428,1 \
  --strict-category \
  --out=./tmp/cutover-baseline-post-backend.json
```

Gate:
- `0 failures` required.

## Phase C: Frontend deployment gate

Run frontend production checks from `brnd-v2-frontend`:

```bash
npm run cutover:check-env
npm run cutover:smoke -- \
  --app-url=https://brnd.land \
  --api-base=https://api.brndland.com \
  --brand-ids=431,428,1
```

Gate:
- app HTML is production bundle (no dev `/src/*.tsx` references)
- key routes return 200
- API contract still valid from UI perspective

## Phase D: 60-minute post-cutover monitoring
- Monitor backend 5xx and p95 latency.
- Monitor frontend runtime errors and failed API calls.
- Validate core journey manually: Home -> Brand -> Vote -> Share -> Profile.

## Rollback

### Trigger
- sustained backend 5xx increase (10 min)
- critical frontend runtime errors
- brand contract shape regression breaking profile rendering

### Order
1. Rollback frontend to last stable build.
2. If incident persists, rollback backend to last stable release.
3. Rerun `cutover:verify-api` to validate recovery.

## Notes
- Guardian card fallback rule in UI: render with `guardianFid || onChainFid`.
- Handle/avatar enrichment depends on backend Neynar hydration in deployed backend.
