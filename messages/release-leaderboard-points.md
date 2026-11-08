# Leaderboard point consistency

Reported November 8, 2026. The leaderboard screen reused cached data when reopened, and live vote, share and reward webhooks could persist an event marker before updating a user's points. A later retry would treat the event as complete.

The backend now increments the all-time and S2 counters in one update, and commits the point credit with the vote, first share or claim marker. The vote reconciliation path uses the same transaction boundary. Duplicate claim/share attempts do not credit twice. Existing point amounts are unchanged.

The companion frontend revalidates the leaderboard on mount and focus. Historical differences need to be reconciled separately against confirmed events and persisted counters.
