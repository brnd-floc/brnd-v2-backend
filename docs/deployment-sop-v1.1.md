# SOP v1.1 — Deploy y Operación BRND

Estado: 2026-02-26

## Producción
- Backend: merge a `main` => Railway deploy automático.
- Frontend: merge a `main` => GitHub Action `Deploy Frontend to Orbiter`.
- Regla: producción es exclusivamente `main` remoto.

## Ownership de incidentes
| Escenario | Owner inicial | Copropietario |
|---|---|---|
| Fallo backend deploy/health | Autor del PR mergeado | Responsable backend |
| Fallo frontend deploy/action | Autor del PR mergeado | Responsable frontend |
| Regresión funcional cross FE/BE | Autor del PR mergeado | Responsables FE + BE |

## Comunicación mínima obligatoria
1. Qué falló y cuándo (UTC).
2. Impacto al usuario/servicio.
3. Mitigación aplicada.
4. Estado final y evidencia (PR, SHA, deploy, smoke).

## Criterios de release por lote
- Branch limpia desde `origin/main`.
- PR con evidencia en `messages/release-lote-XX.md`.
- Merge squash.
- Confirmar SHA mergeado == SHA desplegado.
- Smoke post-deploy antes de siguiente lote.

## Estado de normalización local (post-plan)
- `brnd-v2-backend` local: `main` ahead 5 / behind 3 + cambios no commiteados.
- `brnd-v2-frontend` local: `main` ahead 7 / behind 8 + cambios no commiteados.

## Plan de limpieza controlada (sin pérdida)
1. Preservar snapshot local en ramas de respaldo no productivas (`backup/local-<fecha>`).
2. Crear worktrees limpios para trabajo diario desde `origin/main`.
3. Migrar cambios necesarios en lotes pequeños por cherry-pick/copia selectiva.
4. Eliminar dependencia de `main` local sucio para cualquier release.

## Rollback
- Si smoke/health falla y no hay mitigación en <=20 min: `git revert` del merge commit en `main` y redeploy automático.
