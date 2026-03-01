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
  --api-base=https://api.brnd.land \
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
  --api-base=https://api.brnd.land \
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
  --api-base=https://api.brnd.land \
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

## Legacy Domains Soft-Cut Policy

### Production target domains
- `brnd.land`
- `www.brnd.land`
- `frame.brnd.land`
- `api.brnd.land` (temporary)

### Legacy domains in phased deprecation
- `miniapp.anky.app`
- `brndland.com`
- `miniapp.brndland.com`
- Legacy share-embed URLs (`rebrnd.lat`, `poiesis.anky.app`, Railway URL) in non-prod only

### Exit criteria
- Keep tracking QuickAuth fallback warnings.
- Retire each legacy domain only after 7 consecutive days with zero legacy fallback hits in production.
- Retire legacy share-embed URLs after 7 consecutive days without non-prod fallback usage.

### Operational log query guidance
- Filter warning logs for `QuickAuth token verified using fallback domain`.
- Group by `domain` and compute daily hit rate for legacy domains.
- Approve removal only when hit rate remains `0` for the full observation window.

## Hard-Cut Readiness Gate (GO/NO-GO)

Evaluation cadence:
- Weekly on Mondays at 10:00 UTC.
- First formal evaluation date: March 2, 2026.

Mandatory criteria for `GO` on a specific legacy domain/URL:
1. 7 consecutive days with zero QuickAuth legacy fallback hits in production.
2. 7 consecutive days with zero legacy share-embed fallback usage in staging/non-prod observability.
3. Zero open incidents related to auth/CORS behavior during the observation window.

Decision outcomes:
- `GO`: remove the specific legacy domain/URL in next scheduled change.
- `NO-GO`: keep transition config, open corrective action, and re-evaluate next Monday.

Decision record template:
```md
### Hard-Cut Decision
- date (UTC): YYYY-MM-DD
- scope: <domain-or-url>
- result: GO / NO-GO
- evidence:
  - auth fallback hits (7d): <count>
  - share-embed fallback hits (7d): <count>
  - open auth/cors incidents: <count>
- owner: Backend/API owner
- next review date (UTC): YYYY-MM-DD
```

## Rollback

### Trigger
- sustained backend 5xx increase (10 min)
- critical frontend runtime errors
- brand contract shape regression breaking profile rendering

### Order
1. Rollback frontend to last stable build.
2. If incident persists, rollback backend to last stable release.
3. Rerun `cutover:verify-api` to validate recovery.

### Config-only emergency rollback (no code revert)
1. Add required legacy domains to `QUICKAUTH_ALLOWED_DOMAINS`.
2. Keep `QUICKAUTH_ALLOWED_DOMAINS_LEGACY` aligned for non-prod validation.
3. If required, temporarily restore legacy CORS origins in `PRO` via controlled config change and change ticket.
4. Every rollback must include incident ticket and explicit expiration date.

## Notes
- Guardian card fallback rule in UI: render with `guardianFid || onChainFid`.
- Handle/avatar enrichment depends on backend Neynar hydration in deployed backend.
