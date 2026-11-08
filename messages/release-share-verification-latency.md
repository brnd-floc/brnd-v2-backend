# Share verification latency

On November 8, 2026, the miniapp still reported an eight-second timeout after the indexed-vote lookup change. Once the vote was available, the endpoint could spend more than 27 seconds polling for the cast. It also waited for the optional Farcaster reply after preparing the claim.

Use one bounded Neynar lookup per attempt and return a retryable response when the cast is unavailable. Bound the nonce read and the optional display-level lookup. Send the Farcaster reply without holding the claim response open.

An isolated controller check passed for the direct and recent-cast lookup paths, pending responses, and returning the signature while the optional reply is still pending. The repository test runner and full build require dependencies unavailable in this workspace. A production smoke test requires an authenticated affected vote.
