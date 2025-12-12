# Recent Podiums API Specification

## Endpoint Overview

**GET** `/brand-service/recent-podiums`

Retrieves a paginated list of recent podiums (user votes) from all users. This endpoint provides a public feed of all voting activity in the system, showing the most recent podiums first.

---

## Authentication

**Required**: Yes  
**Guard**: `AuthorizationGuard`  
**Session**: User must be authenticated via Farcaster session

---

## Query Parameters

| Parameter | Type     | Required | Default | Description                            |
| --------- | -------- | -------- | ------- | -------------------------------------- |
| `page`    | `number` | No       | `1`     | Page number for pagination (1-indexed) |
| `limit`   | `number` | No       | `20`    | Number of podiums per page             |

### Example Request

```
GET /brand-service/recent-podiums?page=1&limit=20
```

---

## Response Structure

### Success Response (200 OK)

```typescript
{
  success: true,
  data: {
    podiums: Podium[],
    pagination: {
      page: number,
      limit: number,
      total: number,
      totalPages: number,
      hasNextPage: boolean,
      hasPrevPage: boolean
    }
  }
}
```

### Podium Object (UserBrandVotes)

Each podium object contains the complete vote record with all related entities:

```typescript
interface Podium {
  // Primary identifier
  transactionHash: string; // Blockchain transaction hash (66 chars, 0x prefix)
  id: string | null; // Optional UUID identifier

  // User information (full User object)
  user: {
    id: number;
    fid: number; // Farcaster ID
    username: string;
    photoUrl: string | null;
    points: number;
    role: string; // UserRoleEnum
    dailyStreak: number;
    maxDailyStreak: number | null;
    totalPodiums: number;
    votedBrandsCount: number;
    brndPowerLevel: number;
    totalVotes: number;
    lastVoteDay: number;
    lastVoteTimestamp: Date | null;
    address: string | null;
    banned: boolean;
    powerups: number;
    verified: boolean;
    favoriteBrand: Brand | null;
    createdAt: Date;
    updatedAt: Date;
    notificationsEnabled: boolean;
    notificationToken: string | null;
    lastVoteReminderSent: Date | null;
    neynarScore: number;
  };

  // Brand rankings (full Brand objects)
  brand1: Brand; // 1st place (60% of points)
  brand2: Brand; // 2nd place (30% of points)
  brand3: Brand; // 3rd place (10% of points)

  // Vote metadata
  date: Date; // When the vote was cast
  day: number | null; // Blockchain day number
  shared: boolean; // Whether user shared their vote
  shareVerified: boolean; // Whether share was verified
  shareVerifiedAt: Date | null; // When share was verified

  // Cast information
  castHash: string | null; // Farcaster cast hash if shared

  // BRND token information
  brndPaidWhenCreatingPodium: number | null; // BRND amount paid (based on user level)
  rewardAmount: string | null; // Reward amount (decimal string, precision 64, scale 18)

  // Claim information
  claimedAt: Date | null; // When reward was claimed
  claimTxHash: string | null; // Claim transaction hash (66 chars)

  // Signature information
  signatureGeneratedAt: Date | null;
  nonce: number | null;
}
```

### Brand Object

Each brand object in `brand1`, `brand2`, `brand3` contains:

```typescript
interface Brand {
  id: number;
  name: string;
  url: string;
  warpcastUrl: string;
  description: string;
  category: Category | null;
  followerCount: number;
  imageUrl: string;
  profile: string;
  channel: string;
  ranking: string;
  score: number; // Total score
  stateScore: number;
  scoreWeek: number;
  stateScoreWeek: number;
  rankingWeek: number;
  scoreMonth: number;
  stateScoreMonth: number;
  rankingMonth: number;
  bonusPoints: number;
  banned: number;
  queryType: number; // 0: Channel, 1: Profile
  createdAt: Date;
  updatedAt: Date;
  currentRanking: number;

  // Contract integration fields
  walletAddress: string | null;
  totalBrndAwarded: string;
  availableBrnd: string;
  onChainCreatedAt: Date | null;
  onChainId: number | null;
  onChainFid: number | null;
  onChainHandle: string | null;
  onChainWalletAddress: string | null;
  metadataHash: string | null;
  isUploadedToContract: boolean;
}
```

### Pagination Object

```typescript
interface Pagination {
  page: number; // Current page number
  limit: number; // Items per page
  total: number; // Total number of podiums
  totalPages: number; // Total number of pages
  hasNextPage: boolean; // Whether there's a next page
  hasPrevPage: boolean; // Whether there's a previous page
}
```

---

## Error Responses

### 401 Unauthorized

```typescript
{
  success: false,
  error: {
    code: "UNAUTHORIZED",
    message: "Authentication required"
  }
}
```

### 500 Internal Server Error

```typescript
{
  success: false,
  error: {
    code: "getRecentPodiums",
    message: "Failed to fetch recent podiums"
  }
}
```

---

## Important Notes for Frontend Implementation

### 1. Data Ordering

- Podiums are returned in **descending order by date** (most recent first)
- Use `date` field for client-side sorting if needed

### 2. BRND Payment Calculation

The `brndPaidWhenCreatingPodium` field represents the BRND tokens paid when creating the podium, based on the user's power level at the time:

- Level 0: 1000 BRND
- Level 1: 1500 BRND
- Level 2: 2000 BRND
- Level 3: 3000 BRND
- Level 4: 4000 BRND
- Level 5: 5000 BRND
- Level 6: 6000 BRND
- Level 7: 7000 BRND
- Level 8: 8000 BRND

### 3. Brand Score Distribution

When a podium is created, brand scores are updated as follows:

- `brand1.score` += `0.6 * brndPaidWhenCreatingPodium` (60%)
- `brand2.score` += `0.3 * brndPaidWhenCreatingPodium` (30%)
- `brand3.score` += `0.1 * brndPaidWhenCreatingPodium` (10%)

### 4. Nullable Fields

Many fields can be `null`, especially:

- `castHash`: Only present if user shared their vote
- `claimedAt` / `claimTxHash`: Only present if reward was claimed
- `shareVerifiedAt`: Only present if share was verified
- `brndPaidWhenCreatingPodium`: May be null for older records

### 5. Date Handling

- All date fields are ISO 8601 strings in UTC
- Parse with `new Date(dateString)` in JavaScript
- Display with appropriate timezone formatting

### 6. Decimal Precision

- `rewardAmount` is a decimal string with precision 64, scale 18
- Use a decimal library (e.g., `decimal.js`, `big.js`) for calculations
- Do not use native JavaScript numbers for precision-critical operations

### 7. Transaction Hashes

- All transaction hashes are 66 characters (0x + 64 hex chars)
- Use `transactionHash` as the primary identifier for podiums
- `claimTxHash` is separate and only present if reward was claimed

---

## Frontend Implementation Recommendations

### TypeScript Interfaces

```typescript
// Use these interfaces in your frontend code
interface RecentPodiumsResponse {
  success: boolean;
  data: {
    podiums: Podium[];
    pagination: Pagination;
  };
}

interface Podium {
  transactionHash: string;
  id: string | null;
  user: User;
  brand1: Brand;
  brand2: Brand;
  brand3: Brand;
  date: string; // ISO 8601 date string
  day: number | null;
  shared: boolean;
  shareVerified: boolean;
  shareVerifiedAt: string | null;
  castHash: string | null;
  brndPaidWhenCreatingPodium: number | null;
  rewardAmount: string | null;
  claimedAt: string | null;
  claimTxHash: string | null;
  signatureGeneratedAt: string | null;
  nonce: number | null;
}
```

### Example Usage

```typescript
// Fetch recent podiums
async function fetchRecentPodiums(page: number = 1, limit: number = 20) {
  const response = await fetch(
    `/brand-service/recent-podiums?page=${page}&limit=${limit}`,
    {
      method: 'GET',
      credentials: 'include', // Include session cookie
    },
  );

  if (!response.ok) {
    throw new Error('Failed to fetch podiums');
  }

  const data: RecentPodiumsResponse = await response.json();
  return data.data;
}

// Usage
const { podiums, pagination } = await fetchRecentPodiums(1, 20);

// Display podium information
podiums.forEach((podium) => {
  console.log(`User ${podium.user.username} voted:`);
  console.log(`  1st: ${podium.brand1.name} (${podium.brand1.score} points)`);
  console.log(`  2nd: ${podium.brand2.name} (${podium.brand2.score} points)`);
  console.log(`  3rd: ${podium.brand3.name} (${podium.brand3.score} points)`);
  console.log(`Paid: ${podium.brndPaidWhenCreatingPodium} BRND`);
  console.log(`Date: ${new Date(podium.date).toLocaleString()}`);
});
```

### Pagination Implementation

```typescript
function PodiumsList() {
  const [page, setPage] = useState(1);
  const [podiums, setPodiums] = useState<Podium[]>([]);
  const [pagination, setPagination] = useState<Pagination | null>(null);

  useEffect(() => {
    fetchRecentPodiums(page, 20).then(({ podiums, pagination }) => {
      setPodiums(podiums);
      setPagination(pagination);
    });
  }, [page]);

  return (
    <div>
      {podiums.map(podium => (
        <PodiumCard key={podium.transactionHash} podium={podium} />
      ))}

      {pagination && (
        <PaginationControls
          currentPage={pagination.page}
          totalPages={pagination.totalPages}
          hasNextPage={pagination.hasNextPage}
          hasPrevPage={pagination.hasPrevPage}
          onPageChange={setPage}
        />
      )}
    </div>
  );
}
```

---

## Testing

### Test Cases

1. **Basic Fetch**: Verify successful retrieval with default pagination
2. **Pagination**: Test page navigation and limit changes
3. **Empty Results**: Handle case when no podiums exist
4. **Null Fields**: Ensure proper handling of nullable fields
5. **Date Parsing**: Verify correct date/time display
6. **Decimal Handling**: Test `rewardAmount` precision
7. **Authentication**: Verify 401 response when not authenticated

---

## Changelog

- **2024-XX-XX**: Endpoint now returns full podium objects instead of formatted response
  - Removed formatted mapping
  - Returns complete `UserBrandVotes` entities with all relations
  - All fields from database are now available in response

---

## Related Endpoints

- `GET /user-service/profile` - Get user's own podiums
- `GET /brand-service/:id` - Get brand details
- `POST /blockchain-service/vote` - Submit a new vote/podium
