# Reward verification while vote indexing is delayed

The miniapp cuts off verification after eight seconds. A missing vote could keep the endpoint waiting for more than 30 seconds because the controller and service both retried the same database lookup.

The endpoint now checks for the vote once and returns a retryable `VOTE_INDEXING_PENDING` response if the lookup finds no record. Other callers keep their existing retry behavior. The verification endpoint still checks vote ownership before saving a share or issuing a reward signature.

The change does not reconcile missing votes or repair indexer ingestion. During the incident a confirmed Base vote produced the generic podium embed, while the live weekly rankings returned zero brands. Reconcile ingestion from the confirmed Base events after checking the active Railway indexer and its checkpoint.
