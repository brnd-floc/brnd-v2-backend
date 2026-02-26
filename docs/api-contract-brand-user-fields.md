# API Contract — Brand/User Token And Guardian Fields

## Scope
This document defines the backend output contract for brand and user brand payloads after lote 2.

## Canonical fields
- `contractAddress: string | null`
- `ticker: string | null`
- `tickerTokenId: string | null`
- `guardianFid: number | null`
- `guardianName: string | null`
- `guardianUsername: string | null`
- `guardianPfpUrl: string | null`

## Rules
- Missing values are returned as `null` (never omitted).
- `tickerTokenId` is optional and can be null when metadata has no CAIP-19 identifier.
- Guardian profile fields remain nullable when hydration is unavailable.
