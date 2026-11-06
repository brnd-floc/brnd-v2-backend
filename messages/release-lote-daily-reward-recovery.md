# Daily reward recovery — 2026-11-06

## Incident and scope

Users reported that daily rewards failed after voting/sharing and that returning to the miniapp did not let them retry. This change makes the existing vote/share recoverable. It does not reset votes, change reward amounts, send transactions, or migrate production data.

Confirmed code defects:

- `verify-share` silently returned success with a null claim signature when signing failed.
- A saved `shared` vote could remain ineligible after its separate `shareVerified` write failed; subsequent requests skipped that transition.
- Reward nonce reads ignored the existing `BASE_RPC_URL` setting.
- Whole-entity saves during verification/signing could overwrite a claim indexed after the earlier read.

The production failure trigger is not established without claim/RPC/Neynar logs. The public Railway `/embeds/health` endpoint returned HTTP 200 at 2026-11-06 09:44 UTC; that endpoint does not exercise reward claims.

## Changes

- Resume a saved share using its stored cast hash and indexed vote day, after checking vote ownership, without granting the share points again on a sequential retry.
- Return retryable HTTP 503 errors for verification/signing failures instead of a misleading success response.
- Return `hasClaimed: true`, `canClaim: false` and no signature for already claimed votes.
- Accept a valid vote transaction hash from `voteId` for recovery links; reject missing/malformed hashes before querying.
- Read reward nonces through the configured Base RPC.
- Update only share/signature metadata so late retries cannot overwrite `claimedAt`, `claimTxHash`, or indexed points.
- Align two pre-existing embed-domain test expectations with the production origins already added in March. Runtime allowlists are unchanged.

## API compatibility

Successful signature payloads are unchanged. The companion frontend change consumes these additional outcomes:

```json
{
  "action": "verifyShare",
  "data": {
    "code": "REWARD_CLAIM_UNAVAILABLE",
    "message": "Your vote and share are saved. Reward claiming is temporarily unavailable. Please retry.",
    "retryable": true
  }
}
```

`SHARE_VERIFICATION_UNAVAILABLE` uses the same HTTP 503 envelope. An already claimed vote returns HTTP 200 with `hasClaimed: true` and `claimSignature: null`.

## Validation

- Base: `86223a75be99884aa0cebbb95c1e1f8278650182` (`origin/main`).
- Validation completed: 2026-11-06 09:52 UTC (10:52 Europe/Madrid).
- Dependencies: installed from the committed `bun.lock` with `bun install --frozen-lockfile --ignore-scripts`.
- `npm run test:all`: PASS — 51 source tests and 2 script tests.
- `npm run build`: PASS.
- `git diff --check`: PASS.
- Critical scope: yes (`src/core/blockchain/**`).
- The local commit SHA and patch integrity are recorded in the delivery manifest.

The repository's npm lock is already out of sync with `package.json`; `npm ci` fails before tests. Resolving fresh npm dependencies also introduces unrelated Viem/Ox build errors. Validation uses the existing frozen Bun lock; no dependency or lockfile changes are part of this fix.

## Release and verification

Publication is blocked: the GitHub integration returned HTTP 403 (`Resource not accessible by integration`). No remote branch, pull request, merge, or deployment was created. The delivery includes an applicable patch for an authorized checkout.

Follow `docs/deployment-sop-v1.1.md`: review the paired frontend/backend changes, squash merge from a clean branch, then verify the deployed SHA and smoke test the saved vote → share → reward flow. A merge to `main` triggers Railway deployment.

Use an affected existing vote to verify: retry after a failed share/signature request, reject a wallet request then retry, reopen the miniapp, and confirm that an already claimed reward remains complete. No second vote or manual flag reset is needed for these recovery paths.

The pre-existing race between two simultaneous *first* share verifications and their points updates is not redesigned here. The regression coverage establishes sequential retry behavior and preservation of concurrently indexed claim fields.
