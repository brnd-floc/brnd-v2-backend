# Database Migration Guide

This guide explains how to migrate data from the production database to the new database with the evolved schema.

## Overview

The migration process is designed to:
- ✅ Verify production database is **READ-ONLY** (safety check)
- ✅ Wipe the new database clean
- ✅ Apply the new schema to the new database
- ✅ Copy all data from production to new database with schema transformations
- ✅ Maintain referential integrity by migrating in dependency order

## Prerequisites

1. **Bun** installed (package manager)
2. **Node.js** >= 20.12.0
3. Access to both production and new databases
4. Production database user must have **READ-ONLY** permissions

## Environment Variables Setup

Add the following environment variables to your `.env` file:

### New Database Configuration (where data will be migrated TO)

```bash
# New Database - Target for migration
DATABASE_HOST=localhost
DATABASE_PORT=3306
DATABASE_NAME=brnd_db_new
DATABASE_USER=root
DATABASE_PASSWORD=your_password_here
DATABASE_SSL=false  # Set to 'true' for production/SSL connections
```

### Production Database Configuration (where data will be migrated FROM - READ-ONLY)

```bash
# Production Database - Source for migration (READ-ONLY)
PROD_DATABASE_HOST=your_production_host
PROD_DATABASE_PORT=3306
PROD_DATABASE_NAME=brnd_db_prod
PROD_DATABASE_USER=readonly_user
PROD_DATABASE_PASSWORD=readonly_password
```

### Important Notes

1. **Production database user MUST be read-only** - The migration script will verify this by attempting a test write operation. If the write succeeds, migration is aborted.

2. **Different databases required** - The script validates that production and new database configurations point to different databases to prevent accidental data loss.

3. **SSL Configuration** - If your production database requires SSL, you may need to modify the connection settings in the migration scripts.

## Migration Process

### Step 1: Install Dependencies

```bash
make install
# or
bun install
```

### Step 2: Run Full Migration

The main migration command runs all steps in sequence:

```bash
make migrate
```

This will:
1. Verify production database is read-only
2. Clean the new database (drops all tables)
3. Apply the new schema (creates all tables)
4. Migrate data from production to new database

### Individual Steps

You can also run each step individually:

```bash
# Step 1: Verify production is read-only
make migrate:verify

# Step 2: Clean new database (WARNING: deletes all data!)
make migrate:clean

# Step 3: Apply new schema
make migrate:schema

# Step 4: Migrate data
make migrate:data
```

## Migration Order

Data is migrated in the following order to maintain referential integrity:

1. **Categories** (no dependencies)
2. **Tags** (no dependencies)
3. **Brands** (depends on Categories)
4. **Users** (no dependencies)
5. **BrandTags** (depends on Brands and Tags)
6. **UserBrandVotes** (depends on Users and Brands)
7. **UserDailyActions** (depends on Users)

## Schema Transformations

The migration script handles the following schema transformations:

### Brand Model
- `ranking` field: **INT → STRING** (converted automatically)
- New blockchain fields: Set to defaults/null (will be populated by application later)

### User Model
- Removed fields: `notificationsEnabled`, `notificationToken`, `notificationUrl`, `lastVoteReminderSent`
- New fields: Set to defaults (e.g., `dailyStreak: 0`, `totalPodiums: 0`, etc.)

### UserBrandVotes Model
- New blockchain/reward fields: Set to defaults/null (will be populated by application later)

## New Entities

The following entities exist only in the new schema and will remain empty after migration:
- `AirdropScore`
- `AirdropSnapshot`
- `AirdropLeaf`
- `RewardClaim`

These will be populated by the application as needed.

## Safety Features

1. **Read-Only Verification**: Attempts a test write to production and expects failure
2. **Database Validation**: Ensures production and new databases are different
3. **Dependency Order**: Migrates data in correct order to maintain referential integrity
4. **Error Handling**: Stops on errors and provides clear error messages

## Troubleshooting

### Error: "Production database is NOT read-only"

**Solution**: Ensure your production database user has only SELECT permissions. The migration script will abort if it can write to production.

### Error: "Production and new database configurations are the same"

**Solution**: This is a safety check. Ensure `PROD_DATABASE_*` and `DATABASE_*` point to different databases.

### Error: "Missing required environment variables"

**Solution**: Check that all required environment variables are set in your `.env` file (see Environment Variables Setup above).

### Error: "Cannot connect to database"

**Solution**: 
- Verify database credentials
- Check network connectivity
- Ensure database server is running
- For SSL connections, verify SSL configuration

## Repeatable Migration

The migration is designed to be **repeatable and consistent**. Each time you run `make migrate`:
- The new database is completely wiped
- The new schema is applied fresh
- All data is copied from production (including any new data since last migration)

This ensures the new database always reflects the current state of production data with the new schema.

## Development Commands

The Makefile also includes common development commands:

```bash
make help          # Show all available commands
make install       # Install dependencies
make build         # Build TypeScript project
make dev           # Start development server
make test          # Run tests
make lint          # Run linter
make format        # Format code with Prettier
```

## Support

If you encounter issues during migration:
1. Check the error messages - they provide specific guidance
2. Verify all environment variables are set correctly
3. Ensure database connections are working
4. Review the migration logs for detailed information

