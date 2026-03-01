# Env Hardening Reference

## Scope
This document is the canonical reference for hardening-related environment variables introduced in phases A-F.

## Canonical Variables

| Variable | Code Default | Local Recommended | Staging Recommended | Production Recommended | Used By |
|---|---:|---:|---:|---:|---|
| `QUICKAUTH_ALLOWED_DOMAINS` | `brnd.land,www.brnd.land,frame.brnd.land` (when env unset) | final + legacy as needed for QA | final + legacy (until cutover complete) | final domains only | `src/core/auth/services/auth.service.ts` |
| `QUICKAUTH_ALLOWED_DOMAINS_LEGACY` | `miniapp.anky.app,brndland.com,miniapp.brndland.com,api.brndland.com` (non-prod fallback only) | enable during migration tests | enable during migration window | empty/unset (except incident rollback) | `src/core/auth/services/auth.service.ts` |
| `BRAND_SHARE_EMBED_URLS` | `https://brnd.land,https://www.brnd.land,https://frame.brnd.land` (when env unset) | explicit final list | explicit final list | explicit final list | `src/core/brand/brand.controller.ts` |
| `BRAND_SHARE_EMBED_URLS_LEGACY` | `https://rebrnd.lat,https://poiesis.anky.app,https://brnd-v2-backend-production.up.railway.app` (non-prod fallback only) | enable only for migration tests | enable only during migration window | empty/unset (except incident rollback) | `src/core/brand/brand.controller.ts` |
| `WEBHOOK_API_KEY` | none (required for guarded routes) | strong random test secret | strong secret from secret manager | strong rotated secret from secret manager | `src/security/guards/webhook-api-key.guard.ts` |
| `ENABLE_DEBUG_ENDPOINTS` | `false` behavior unless set to `true` | `true` only when needed | `false` (temporarily `true` for controlled debugging) | `false` | `src/security/guards/debug-endpoint.guard.ts` |
| `PODIUM_IMAGE_TIMEOUT_MS` | `6000` | `6000` | `6000-8000` | `6000-10000` | `src/core/blockchain/services/podium.service.ts` |
| `PODIUM_IPFS_TIMEOUT_MS` | `8000` | `8000` | `8000-10000` | `8000-12000` | `src/core/blockchain/services/podium.service.ts`, `src/core/blockchain/blockchain.controller.ts` |
| `PODIUM_IPFS_RETRIES` | `1` | `1` | `1-2` | `1-2` | `src/core/blockchain/services/podium.service.ts`, `src/core/blockchain/blockchain.controller.ts` |
| `PODIUM_CLAIM_RATE_LIMIT_ENABLED` | `true` | `true` | `true` | `true` | `src/core/blockchain/blockchain.controller.ts` |
| `PODIUM_CLAIM_RATE_LIMIT_WINDOW_MS` | `60000` | `60000` | `60000` | `60000` | `src/core/blockchain/blockchain.controller.ts` |
| `PODIUM_CLAIM_RATE_LIMIT_MAX_PER_FID` | `6` | `6-10` | `6` | `4-6` | `src/core/blockchain/blockchain.controller.ts` |
| `PODIUM_CLAIM_RATE_LIMIT_MAX_PER_IP` | `20` | `20-30` | `20` | `15-20` | `src/core/blockchain/blockchain.controller.ts` |
| `PODIUM_CLAIM_RATE_LIMIT_BLOCK_MS` | `120000` | `120000` | `120000-180000` | `120000-300000` | `src/core/blockchain/blockchain.controller.ts` |

## Semantics and Tuning
- `QUICKAUTH_ALLOWED_DOMAINS`: only listed domains are accepted by JWT verification. Empty/invalid list causes fail-fast in auth initialization.
- `QUICKAUTH_ALLOWED_DOMAINS_LEGACY`: optional transition domains. In production, keep unset unless rollback is required.
- `BRAND_SHARE_EMBED_URLS`: base URLs accepted in share verification embeds.
- `BRAND_SHARE_EMBED_URLS_LEGACY`: optional transition list for legacy share embeds; keep unset in production unless incident rollback.
- `PODIUM_IMAGE_TIMEOUT_MS`: lower values reduce resource lock time but can increase false-negative failures.
- `PODIUM_IPFS_TIMEOUT_MS`: per-attempt timeout for IPFS operations; too low causes avoidable `503`.
- `PODIUM_IPFS_RETRIES`: additional attempts; higher values improve resiliency but increase latency.
- `PODIUM_CLAIM_RATE_LIMIT_*`: controls burst protection for `POST /blockchain-service/podium/claim-signature`.

## Legacy Domains Deprecation Policy

| Domain | Status | Reason | Target Retirement Date | Owner |
|---|---|---|---|---|
| `miniapp.anky.app` | Deprecated (phased) | Client transition window | 2026-03-31 | Backend/API owner |
| `brndland.com` | Deprecated (phased) | Legacy auth clients | 2026-03-31 | Backend/API owner |
| `miniapp.brndland.com` | Deprecated (phased) | Legacy miniapp traffic | 2026-03-31 | Backend/API owner |
| `api.brndland.com` | Temporary allowlisted | Backward compatibility for API base URL | 2026-04-15 | Backend/API owner |

Retirement criterion:
- Remove a legacy domain only after 7 consecutive days with zero fallback verification hits in production logs.

## Transition Status by Component

| Component | Production State | Legacy in Non-Prod | Target Removal Date | Owner |
|---|---|---|---|---|
| Auth QuickAuth domains | Final-only by default (`brnd.land`, `www.brnd.land`, `frame.brnd.land`) | Enabled via `QUICKAUTH_ALLOWED_DOMAINS_LEGACY` | 2026-03-31 (except `api.brndland.com`) | Backend/API owner |
| CORS `PRO` origins | Final-only + `api.brndland.com` temporary | Legacy kept in `LOCAL` for QA | 2026-04-15 (`api.brndland.com` review) | Backend/API owner |
| Share embed URL verification | Final-only in `production` defaults | Legacy via `BRAND_SHARE_EMBED_URLS_LEGACY` | 2026-03-31 | Backend/API owner |

## Hard-Cut Readiness Gate
- Weekly review every Monday at 10:00 UTC (starting March 2, 2026).
- A legacy target can move to removal only if all checks pass:
  - `QuickAuth` fallback legacy hits in production: `0` for 7 consecutive days.
  - share-embed legacy fallback hits: `0` for 7 consecutive days.
  - open auth/CORS incidents: `0` in the same window.
- If any check fails: keep transition config and create corrective action ticket.

## Quick Rollback Playbook
- Disable claim throttling temporarily:
  - `PODIUM_CLAIM_RATE_LIMIT_ENABLED=false`
- Relax throttling without disabling:
  - Increase `PODIUM_CLAIM_RATE_LIMIT_MAX_PER_FID`
  - Increase `PODIUM_CLAIM_RATE_LIMIT_MAX_PER_IP`
  - Decrease `PODIUM_CLAIM_RATE_LIMIT_BLOCK_MS`
- Reduce metadata 503 pressure:
  - Increase `PODIUM_IPFS_TIMEOUT_MS`
  - Increase `PODIUM_IPFS_RETRIES` by `+1`
- Restrict/restore auth domain acceptance:
  - Update `QUICKAUTH_ALLOWED_DOMAINS` to approved set only.
  - For emergency compatibility, temporarily append needed domains to `QUICKAUTH_ALLOWED_DOMAINS` and track via incident ticket.
- Restrict/restore share-embed verification:
  - Update `BRAND_SHARE_EMBED_URLS` to final-only list.
  - For emergency compatibility, temporarily include legacy entries in `BRAND_SHARE_EMBED_URLS`.

Rollback expiration policy:
- Every temporary rollback must include:
  - incident/change ticket id,
  - owner,
  - expiration date (UTC) not later than 7 days after activation,
  - planned review date.

## Misconfiguration Risks
- Empty `QUICKAUTH_ALLOWED_DOMAINS` -> auth initialization failure.
- Setting legacy domains permanently in prod -> migration never converges to `brnd.land`.
- Empty `BRAND_SHARE_EMBED_URLS` -> share verification path fails when URL matching runs.
- Very low `PODIUM_IMAGE_TIMEOUT_MS` or `PODIUM_IPFS_TIMEOUT_MS` -> frequent `PODIUM_METADATA_UNAVAILABLE`.
- Very high `PODIUM_IPFS_RETRIES` -> long tail latency under provider degradation.
- Very strict rate limits -> valid users receive `429` during short spikes.
- Very loose rate limits -> insufficient abuse protection.

## Post-change Validation Checklist
1. `npm run verify:release`
2. Smoke tests for podium claim:
   - Success path returns unchanged success payload keys.
   - Burst requests can produce controlled `429 RATE_LIMITED` with `retryAfterMs`.
   - Forced metadata/IPFS degradation produces controlled `503 PODIUM_METADATA_UNAVAILABLE`.

## Related Files
- `.env.example`
- `docs/release-local-checklist.md`
- `src/core/auth/services/auth.service.ts`
- `src/core/blockchain/blockchain.controller.ts`
- `src/core/blockchain/services/podium.service.ts`
- `src/security/guards/webhook-api-key.guard.ts`
- `src/security/guards/debug-endpoint.guard.ts`
