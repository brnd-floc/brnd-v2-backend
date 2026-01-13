# Brand Creation/Update Endpoint Summary

## Overview

The `/blockchain/brands` POST endpoint handles brand creation and updates from the Ponder indexer. It synchronizes blockchain data with the database by fetching on-chain brand information and IPFS metadata.

## Endpoint Details

**Route:** `POST /blockchain/brands`  
**Controller:** `blockchain.controller.ts` (lines 613-649)  
**Guard:** `IndexerGuard` (ensures only the indexer can call this endpoint)  
**Service:** `adminService.createOrUpdateBrandFromBlockchain()`

## Request Flow

### 1. **Request Reception**

- The Ponder indexer sends a `BlockchainBrandDto` containing:
  - `id`: On-chain brand ID
  - `fid`: Farcaster ID
  - `walletAddress`: Brand owner's wallet address
  - `handle`: Brand handle
  - `createdAt`: Creation timestamp
  - `blockNumber`, `transactionHash`, `timestamp`: Blockchain event details
  - `createdOrUpdated`: Event type ('created' or 'updated')

### 2. **Brand Existence Check**

```typescript
// Check if brand already exists by onChainId
const existingBrand = await this.brandRepository.findOne({
  where: { onChainId: blockchainBrandDto.id },
});
```

- If brand exists and event is 'created', returns existing brand (idempotent)
- If brand exists and event is 'updated', proceeds to update flow

### 3. **Fetch On-Chain Data**

```typescript
const contractBrand = await this.blockchainService.getBrandFromContract(
  blockchainBrandDto.id,
);
```

**What `getBrandFromContract()` does:**

- Uses Viem to call the smart contract's `getBrand(uint16 brandId)` function
- Reads from the Base blockchain using a public RPC client
- Returns:
  - `fid`: Farcaster ID
  - `walletAddress`: Owner wallet address
  - `totalBrndAwarded`: Total BRND tokens awarded
  - `availableBrnd`: Available BRND tokens
  - `handle`: Brand handle
  - `metadataHash`: IPFS hash pointing to brand metadata
  - `createdAt`: Creation timestamp

### 4. **Fetch IPFS Metadata**

```typescript
metadata = await this.blockchainService.fetchMetadataFromIpfs(
  contractBrand.metadataHash,
);
```

**What `fetchMetadataFromIpfs()` does:**

- Takes the IPFS hash from the contract
- Tries multiple IPFS gateways in order (for redundancy):
  1. `https://ipfs.io/ipfs/{hash}`
  2. `https://cloudflare-ipfs.com/ipfs/{hash}`
  3. `https://gateway.pinata.cloud/ipfs/{hash}`
- Fetches JSON metadata containing:
  - `name`: Brand name
  - `url`: Website URL
  - `warpcastUrl`: Warpcast profile/channel URL
  - `description`: Brand description
  - `categoryId`: Category ID
  - `followerCount`: Follower count
  - `imageUrl`: Brand image URL
  - `profile`: Farcaster profile handle
  - `channel`: Farcaster channel handle
  - `queryType`: 0 for channel, 1 for profile
  - `channelOrProfile`: Combined field
  - `createdAt`: Metadata creation timestamp

**Error Handling:**

- If IPFS fetch fails, continues with contract data only (graceful degradation)
- Logs warning but doesn't fail the entire operation

### 5. **Data Processing**

#### Category Resolution

```typescript
const category = await this.getOrCreateCategory(
  metadata.categoryId || 'General',
);
```

- Gets or creates the category (defaults to 'General' if not specified)

#### Profile/Channel Processing

```typescript
const { profile, channel, queryType } = this.processProfileAndChannel({
  profile: metadata.profile,
  channel: metadata.channel,
  queryType: metadata.queryType || 0,
  name: contractBrand.handle,
});
```

- Normalizes profile/channel format (adds @ or / prefixes)
- Determines query type (channel vs profile)

#### Follower Count Fetch

- Attempts to fetch real-time follower count from Neynar API
- Falls back to metadata value if API call fails

### 6. **Database Storage**

#### For New Brands (created event):

Creates a new `Brand` entity with:

**On-Chain Data (Source of Truth):**

- `onChainId`: Brand ID from contract
- `onChainHandle`: Handle from contract
- `onChainFid`: FID from contract
- `onChainWalletAddress`: Wallet address from contract
- `onChainCreatedAt`: Creation timestamp from contract
- `metadataHash`: IPFS hash from contract
- `totalBrndAwarded`: Total BRND awarded (from contract)
- `availableBrnd`: Available BRND (from contract)

**IPFS Metadata (Updatable):**

- `name`: From IPFS metadata (fallback to handle)
- `url`: From IPFS metadata
- `warpcastUrl`: From IPFS metadata
- `description`: From IPFS metadata
- `imageUrl`: From IPFS metadata
- `profile`: From IPFS metadata
- `channel`: Processed channel
- `queryType`: Processed query type
- `followerCount`: From Neynar API or metadata
- `category`: Resolved category

**Initialized Fields:**

- All scoring fields set to 0 (score, stateScore, scoreDay, etc.)
- Rankings set to 0
- `banned`: 0

#### For Existing Brands (updated event):

Updates existing brand with:

- All on-chain data (source of truth)
- All IPFS metadata (can be updated)
- **Preserves** scoring fields (not reset on update)

## Data Sources Priority

1. **On-Chain Data** (Highest Priority - Source of Truth)
   - FID, wallet address, handle, metadata hash
   - These come directly from the smart contract

2. **IPFS Metadata** (Secondary - Updatable)
   - Brand name, description, images, URLs
   - Can be updated by brand owners

3. **External APIs** (Tertiary - Real-time)
   - Follower count from Neynar API
   - Falls back to metadata value if unavailable

## Error Handling

- **Contract Read Failure**: Throws error, endpoint fails
- **IPFS Fetch Failure**: Logs warning, continues with contract data only
- **Neynar API Failure**: Logs warning, uses metadata follower count
- **Category Missing**: Creates category automatically
- **Duplicate Brand**: Returns existing brand (idempotent for 'created' events)

## Security

- Protected by `IndexerGuard` - only the Ponder indexer can call this endpoint
- Validates all input data through DTO validation
- Uses read-only contract calls (no state changes)

## Key Files

- **Controller**: `src/core/blockchain/blockchain.controller.ts` (lines 613-649)
- **Service**: `src/core/admin/services/admin.service.ts` (lines 757-942)
- **Blockchain Service**: `src/core/blockchain/services/blockchain.service.ts`
  - `getBrandFromContract()` (lines 1088-1157)
  - `fetchMetadataFromIpfs()` (lines 1159-1197)
- **DTO**: `src/core/admin/dto/blockchain-brand.dto.ts`
- **IPFS Service**: `src/utils/ipfs.service.ts`

## Summary

This endpoint serves as a bridge between the blockchain and database, ensuring that:

1. On-chain brand data (immutable) is stored accurately
2. IPFS metadata (mutable) is fetched and stored
3. External data (follower counts) is refreshed
4. The database stays synchronized with blockchain events
5. Operations are idempotent and handle failures gracefully
