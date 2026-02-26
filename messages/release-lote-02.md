# Release Lote 02 — Evidencia

## Contexto
- Repo: brnd-v2-backend
- Lote: 02
- Rama: codex/be-lote-2
- Objetivo: consistencia de datos/API para ticker + guardian y toolkit de migración/backfill

## Commits/changes
- 1532f65 (aplicado como 4700929)
- scripts/backfill-brand-token-fields.ts (+ spec)
- scripts/migrations/002_add_ticker_token_id_to_brands.sql
- docs/api-contract-brand-user-fields.md

## Comandos ejecutados
```bash
npm install
npm test
npm run build
npx ts-node scripts/backfill-brand-token-fields.ts --only-onchain-id 1
```

## Resultado de checks
- npm install: OK
- npm test: OK
- npm run build: OK
- dry-run backfill: ejecutado, falló por ausencia de MySQL local (`ECONNREFUSED 127.0.0.1:3306`)

## Notas
- El script queda listo para ejecutarse en entorno con DB reachable y credenciales válidas.
