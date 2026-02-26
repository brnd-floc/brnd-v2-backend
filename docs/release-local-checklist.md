# Release Local Checklist (Pre-merge / Pre-deploy)

## Objetivo
Asegurar una validación determinística y repetible antes de merge/deploy mientras no exista CI versionada en este repositorio.

## Prerrequisitos
- Node.js `>=20.12.0`.
- Dependencias instaladas (`npm install`).
- Rama local actualizada y cambios listos para validar.

## Config previa al release
- Revisar variables de hardening y defaults en [env-hardening-reference.md](/Users/gsus/projects/brnd/brnd-v2-backend/docs/env-hardening-reference.md).
- Tomar como base [/.env.example](/Users/gsus/projects/brnd/brnd-v2-backend/.env.example) y validar que los valores críticos en `.env` coinciden con la política del entorno objetivo (local/staging/prod).
- Confirmar política de retiro de legacy domains y ventana vigente en [cutover-runbook.md](/Users/gsus/projects/brnd/brnd-v2-backend/docs/cutover-runbook.md).
- Para producción, evitar dominios legacy en `QUICKAUTH_ALLOWED_DOMAINS` salvo rollback de incidente con ticket.
- Si el cambio toca Auth/CORS/Embeds legacy, adjuntar decisión del **Hard-Cut Readiness Gate** (GO/NO-GO) con fecha UTC.

## Secuencia obligatoria
Ejecutar en este orden:

```bash
npm run test:all
npm run build
```

## Gate de bloqueo
No se puede mergear ni desplegar si cualquiera de los comandos anteriores termina con exit code distinto de `0`.

Bloquea automáticamente en estos casos:
- Falla `test` (suite de `src`).
- Falla `test:scripts` (suite de `scripts`).
- Falla `build`.

## Política pre-merge y pre-deploy
- **No merge** si no se completó checklist o hubo fallo en cualquier paso.
- **No deploy** si hubo cambios en alcance crítico y no se ejecutó checklist completa.

### Alcance crítico (checklist completa obligatoria)
- `src/security/**`
- `src/core/auth/**`
- `src/core/blockchain/**`
- `scripts/**`

## Evidencia mínima para PR
Adjuntar un resumen de corrida local con:
- Resultado `test:all`: `PASS` o `FAIL`.
- Resultado `build`: `PASS` o `FAIL`.
- Timestamp local de la ejecución.
- Commit SHA validado (`git rev-parse --short HEAD`).
- ¿Hubo cambios en alcance crítico?: `sí/no`.

## Plantilla corta para PR
Copiar/pegar:

```md
### Release Checklist (Local)
- test:all: PASS/FAIL
- build: PASS/FAIL
- timestamp: YYYY-MM-DD HH:mm (zona horaria)
- commit: <short-sha>
- cambios en alcance crítico (security/auth/blockchain/scripts): sí/no
```

## Fallas típicas y acción inmediata
- Falla en `test:scripts`: revisar `scripts/*.spec.ts` y cambios recientes en scripts.
- Falla en `test` (src): revisar regresión funcional/unidad en módulos tocados.
- Falla en `build`: corregir tipos/imports/config antes de continuar.

## Hook para CI futura
Cuando exista CI versionada, usar un único comando de pipeline:

```bash
npm run verify:release
```

Criterio de transición:
- La pipeline pasa a ser gate principal de merge.
- El checklist manual queda como respaldo local previo al push.
