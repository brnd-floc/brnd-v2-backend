<p align="center">
  <a href="http://nestjs.com/" target="blank"><img src="https://nestjs.com/img/logo-small.svg" width="200" alt="Nest Logo" /></a>
</p>

[circleci-image]: https://img.shields.io/circleci/build/github/nestjs/nest/master?token=abc123def456
[circleci-url]: https://circleci.com/gh/nestjs/nest

  <p align="center">A progressive <a href="http://nodejs.org" target="_blank">Node.js</a> framework for building efficient and scalable server-side applications.</p>
    <p align="center">
<a href="https://www.npmjs.com/~nestjscore" target="_blank"><img src="https://img.shields.io/npm/v/@nestjs/core.svg" alt="NPM Version" /></a>
<a href="https://www.npmjs.com/~nestjscore" target="_blank"><img src="https://img.shields.io/npm/l/@nestjs/core.svg" alt="Package License" /></a>
<a href="https://www.npmjs.com/~nestjscore" target="_blank"><img src="https://img.shields.io/npm/dm/@nestjs/common.svg" alt="NPM Downloads" /></a>
<a href="https://circleci.com/gh/nestjs/nest" target="_blank"><img src="https://img.shields.io/circleci/build/github/nestjs/nest/master" alt="CircleCI" /></a>
<a href="https://coveralls.io/github/nestjs/nest?branch=master" target="_blank"><img src="https://coveralls.io/repos/github/nestjs/nest/badge.svg?branch=master#9" alt="Coverage" /></a>
<a href="https://discord.gg/G7Qnnhy" target="_blank"><img src="https://img.shields.io/badge/discord-online-brightgreen.svg" alt="Discord"/></a>
<a href="https://opencollective.com/nest#backer" target="_blank"><img src="https://opencollective.com/nest/backers/badge.svg" alt="Backers on Open Collective" /></a>
<a href="https://opencollective.com/nest#sponsor" target="_blank"><img src="https://opencollective.com/nest/sponsors/badge.svg" alt="Sponsors on Open Collective" /></a>
  <a href="https://paypal.me/kamilmysliwiec" target="_blank"><img src="https://img.shields.io/badge/Donate-PayPal-ff3f59.svg"/></a>
    <a href="https://opencollective.com/nest#sponsor"  target="_blank"><img src="https://img.shields.io/badge/Support%20us-Open%20Collective-41B883.svg" alt="Support us"></a>
  <a href="https://twitter.com/nestframework" target="_blank"><img src="https://img.shields.io/twitter/follow/nestframework.svg?style=social&label=Follow"></a>
</p>
  <!--[![Backers on Open Collective](https://opencollective.com/nest/backers/badge.svg)](https://opencollective.com/nest#backer)
  [![Sponsors on Open Collective](https://opencollective.com/nest/sponsors/badge.svg)](https://opencollective.com/nest#sponsor)-->

## Description

[Nest](https://github.com/nestjs/nest) framework TypeScript starter repository.

## Installation

```bash
$ yarn install
```

## Running the app

```bash
# development
$ yarn run start

# watch mode
$ yarn run start:dev

# production mode
$ yarn run start:prod
```

## Test

```bash
# unit tests
$ yarn run test

# e2e tests
$ yarn run test:e2e

# test coverage
$ yarn run test:cov
```

## Support

Nest is an MIT-licensed open source project. It can grow thanks to the sponsors and support by the amazing backers. If you'd like to join them, please [read more here](https://docs.nestjs.com/support).

## Stay in touch

- Author - [Kamil Myśliwiec](https://kamilmysliwiec.com)
- Website - [https://nestjs.com](https://nestjs.com/)
- Twitter - [@nestframework](https://twitter.com/nestframework)

## License

Nest is [MIT licensed](LICENSE).

## Smart Contract Integration

### Brand Upload Tracking System

The backend includes a comprehensive system for managing brand uploads to the smart contract with efficient gas cost management and testing capabilities.

#### Key Features

- **Upload Status Tracking**: Each brand has an `isUploadedToContract` boolean flag
- **Consistent ID Mapping**: Database ID directly maps to contract ID (Brand ID 1 → Contract ID 1)
- **Gas Cost Control**: Upload specific number of brands for testing (default: 20 brands)
- **Fresh Contract Deployment**: Reset all upload flags when deploying new contract versions
- **Voting Filter**: Only uploaded brands appear in voting endpoints

#### Environment Variables

```bash
# Required for contract operations
BRND_SEASON_1_ADDRESS=0x...  # V5 contract address
ADMIN_PRIVATE_KEY=0x...             # Admin wallet private key for uploads
```

### Admin Endpoints (Unprotected for Testing)

#### Check Upload Status

```bash
GET /admin-service/brands/upload-status
```

Returns upload progress and statistics:

```json
{
  "totalBrands": 150,
  "uploadedBrands": 20,
  "remainingBrands": 130,
  "uploadProgress": 13,
  "message": "130 brands remaining to upload"
}
```

#### Upload Limited Brands for Testing

```bash
POST /admin-service/brands/upload-to-contract-testing
Content-Type: application/json

{
  "limit": 20
}
```

- Uploads specific number of non-uploaded brands (default: 20)
- Maintains database ID order for consistent contract IDs
- Tracks successful uploads and marks brands as uploaded
- **Does not reset flags** - incremental upload

#### Upload All Brands (Full Deployment)

```bash
GET /admin-service/brands/upload-to-contract
```

- Resets all upload flags first (fresh contract deployment)
- Uploads all non-uploaded brands to contract
- Use when deploying a new contract version

#### Reset Upload Flags

```bash
POST /admin-service/brands/reset-upload-flags
```

- Marks all brands as non-uploaded (`isUploadedToContract = false`)
- Use before uploading to a new contract deployment
- Prepares database for fresh upload

#### Check Contract vs Database Status

```bash
GET /admin-service/brands/contract-status
```

Compares brands in database vs smart contract.

### Testing Workflow

1. **Deploy New Contract** → Reset upload flags

   ```bash
   POST /admin-service/brands/reset-upload-flags
   ```

2. **Upload Test Brands** → Upload 20 brands for frontend testing

   ```bash
   POST /admin-service/brands/upload-to-contract-testing
   {"limit": 20}
   ```

3. **Check Progress** → Monitor upload status

   ```bash
   GET /admin-service/brands/upload-status
   ```

4. **Test Frontend** → Only uploaded brands will appear in voting
5. **Upload More** → Continue with additional batches as needed
   ```bash
   POST /admin-service/brands/upload-to-contract-testing
   {"limit": 50}
   ```

### Implementation Details

#### Database Schema

```sql
ALTER TABLE brands ADD COLUMN isUploadedToContract BOOLEAN DEFAULT FALSE;
```

#### Voting Filter

All brand listing endpoints automatically filter to only show uploaded brands:

```typescript
where: {
  banned: 0,
  isUploadedToContract: true, // Only show uploaded brands
}
```

#### Upload Process

1. **Query brands** in database ID order (`ORDER BY id ASC`)
2. **Filter non-uploaded** (`isUploadedToContract = false`)
3. **Batch upload** to contract (20 brands per transaction)
4. **Mark successful** uploads in database after each batch
5. **Consistent IDs** ensure Database Brand ID = Contract Brand ID

#### Gas Optimization

- **Batch uploads**: 20 brands per transaction for gas efficiency
- **Incremental uploads**: Only upload non-uploaded brands
- **Testing limits**: Upload small batches to control gas costs
- **Error handling**: Continue with next batch if one fails

### Contract Compatibility

- **V5 Contract Support**: Uses `StoriesInMotionV5` contract
- **FID-based Architecture**: Supports new FID-centric user management
- **Batch Creation**: Uses `batchCreateBrands()` for efficient uploads
- **EIP-712 Signatures**: All operations properly signed by backend

------ DESIGN DECISIONS ---------

- no /login route because this will always be accessed from inside a farcaster miniapp
- smart contract upload tracking prevents duplicate uploads and controls gas costs
- database ID order ensures consistent contract ID mapping for frontend integration
