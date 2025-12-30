# =============================================================================
# BRND API - Database Migration Makefile
# =============================================================================
# This Makefile orchestrates the migration of data from the production database
# to the new development/staging database with an evolved schema.
# IMPORTANT: This process is READ-ONLY for production database.
# =============================================================================
# Default target - shows help message

.PHONY: help
help:
	@echo "# =============================================================================="
	@echo "BRND API - Database Migration Commands"
	@echo "# =============================================================================="
	@echo ""
	@echo "Main Commands:"
	@echo "  make migrate          - Full migration: verify, clean, schema, and data"
	@echo "                        (Interactive: Press Enter at each step to proceed)"
	@echo "  make migrate-verify   - Verify production DB is read-only"
	@echo "  make migrate-clean    - Wipe the new database completely (requires 'yes')"
	@echo "  make migrate-schema   - Apply new schema to clean database"
	@echo "  make migrate-data     - Copy data from production to new database"
	@echo ""
	@echo ""
	@echo "Utility Commands:"
	@echo "  make install          - Install dependencies using bun"
	@echo "  make build            - Build the TypeScript project"
	@echo "  make dev              - Start development server"
	@echo "  make test             - Run tests"
	@echo "  make lint             - Run linter"
	@echo "  make format           - Format code with Prettier"
	@echo ""
	@echo "# =============================================================================="

# =============================================================================
# Main Migration Target
# =============================================================================
# This is the primary command that orchestrates the entire migration process.

.PHONY: migrate
migrate:
	@echo ""
	@echo "# =============================================================================="
	@echo "BRND API - Database Migration Process"
	@echo "# =============================================================================="
	@echo ""
	@echo "This will perform a complete migration from production to new database."
	@echo "The process includes:"
	@echo "  1. Verifying production database is read-only"
	@echo "  2. Cleaning the new database (ALL DATA WILL BE DELETED)"
	@echo "  3. Applying the new schema"
	@echo "  4. Migrating all data from production"
	@echo ""
	@echo "Press Enter to continue, or Ctrl+C to cancel..."
	@bash -c 'read -r dummy'
	@$(MAKE) migrate-verify
	@echo ""
	@echo "Press Enter to proceed to database cleaning..."
	@bash -c 'read -r dummy'
	@$(MAKE) migrate-clean
	@echo ""
	@echo "Press Enter to proceed to schema application..."
	@bash -c 'read -r dummy'
	@$(MAKE) migrate-schema
	@echo ""
	@echo "Press Enter to proceed to data migration..."
	@bash -c 'read -r dummy'
	@$(MAKE) migrate-data
	@echo ""
	@echo ""
	@echo "# =============================================================================="
	@echo "✓ Migration completed successfully!"
	@echo "# =============================================================================="
	@echo "The new database has been populated with production data using the new schema."
	@echo ""
	@echo "Press Enter to exit..."
	@bash -c 'read -r dummy'

# =============================================================================
# Step 1: Verify Production Database is Read-Only
# =============================================================================

.PHONY: migrate-verify
migrate-verify:
	@echo "# =============================================================================="
	@echo "Step 1: Verifying Production Database is Read-Only"
	@echo "# =============================================================================="
	@echo ""
	@echo "This step will:"
	@echo "  • Connect to the production database"
	@echo "  • Attempt a test write operation (should FAIL if read-only)"
	@echo "  • Verify read access works correctly"
	@echo ""
	@echo "Press Enter to start verification..."
	@bash -c 'read -r dummy'
	@echo ""
	@bun run scripts/migrate-verify.ts || (echo "" && echo "❌ ERROR: Production database verification failed!" && echo "Please check your PROD_DATABASE_* environment variables." && exit 1)
	@echo ""
	@echo "✓ Production database is verified as read-only"

# =============================================================================
# Step 2: Clean New Database
# =============================================================================

.PHONY: migrate-clean
migrate-clean:
	@echo "# =============================================================================="
	@echo "Step 2: Cleaning New Database"
	@echo "# =============================================================================="
	@echo ""
	@echo "⚠️  WARNING: This will DELETE ALL data in the new database!"
	@echo ""
	@echo "This step will:"
	@echo "  • Connect to the new database"
	@echo "  • List all existing tables"
	@echo "  • Drop ALL tables (complete wipe)"
	@echo ""
	@echo "Type 'yes' and press Enter to continue, or anything else to cancel:"
	@bash -c 'read -r confirmation; if [ "$$confirmation" != "yes" ]; then echo ""; echo "Migration cancelled by user."; exit 1; fi'
	@echo ""
	@echo "Dropping all tables in the new database..."
	@bun run scripts/migrate-clean.ts || (echo "" && echo "❌ ERROR: Failed to clean new database!" && exit 1)
	@echo ""
	@echo "✓ New database has been cleaned"

# =============================================================================
# Step 3: Apply New Schema
# =============================================================================

.PHONY: migrate-schema
migrate-schema:
	@echo "# =============================================================================="
	@echo "Step 3: Applying New Schema"
	@echo "# =============================================================================="
	@echo ""
	@echo "This step will create all tables in the new database based on entity models."
	@echo ""
	@echo "Tables to be created:"
	@echo "  • Core entities:"
	@echo "    - users"
	@echo "    - brands"
	@echo "    - categories"
	@echo "    - tags"
	@echo "    - brand_tags"
	@echo "  • Voting entities:"
	@echo "    - user_brand_votes"
	@echo "    - user_daily_actions"
	@echo "  • New entities (will be empty):"
	@echo "    - reward_claims"
	@echo ""
	@echo "Press Enter to start schema application..."
	@bash -c 'read -r dummy'
	@echo ""
	@bun run scripts/migrate-schema.ts || (echo "" && echo "❌ ERROR: Failed to apply schema!" && exit 1)
	@echo ""
	@echo "✓ New schema has been applied successfully"

# =============================================================================
# Step 4: Migrate Data from Production
# =============================================================================

.PHONY: migrate-data
migrate-data:
	@echo "# =============================================================================="
	@echo "Step 4: Migrating Data from Production"
	@echo "# =============================================================================="
	@echo ""
	@echo "This step will copy all data from production to the new database."
	@echo ""
	@echo "Migration order (respecting dependencies):"
	@echo "  1. Categories (no dependencies)"
	@echo "  2. Tags (no dependencies)"
	@echo "  3. Brands (depends on Categories) - ranking INT→STRING conversion"
	@echo "  4. Users (no dependencies) - includes notifications fields"
	@echo "  5. BrandTags (depends on Brands and Tags)"
	@echo "  6. UserBrandVotes (depends on Users and Brands) - new fields set to defaults"
	@echo "  7. Calculate User Fields - totalPodiums, votedBrandsCount, totalVotes,"
	@echo "                            lastVoteDay, lastVoteTimestamp, favoriteBrandId"
	@echo "  8. UserDailyActions (depends on Users)"
	@echo ""
	@echo "Note: New entities (AirdropScore, AirdropSnapshot, AirdropLeaf, RewardClaim)"
	@echo "      will remain empty as they don't exist in production."
	@echo ""
	@echo "Press Enter to start data migration..."
	@bash -c 'read -r dummy'
	@echo ""
	@bun run scripts/migrate-data.ts || (echo "" && echo "❌ ERROR: Data migration failed!" && exit 1)
	@echo ""
	@echo "✓ Data migration completed successfully"


# =============================================================================
# Development Commands
# =============================================================================
# Install all dependencies using bun

.PHONY: install
install:
	@echo "Installing dependencies with bun..."
	@bun install

# Build the TypeScript project
.PHONY: build
build:
	@echo "Building TypeScript project..."
	@bun run build

# Start development server with hot reload
.PHONY: dev
dev:
	@echo "Starting development server..."
	@bun run start:dev

# Run all tests
.PHONY: test
test:
	@echo "Running tests..."
	@bun run test

# Run linter with auto-fix
.PHONY: lint
lint:
	@echo "Running linter..."
	@bun run lint

# Format code with Prettier
.PHONY: format
format:
	@echo "Formatting code with Prettier..."
	@bun run format

# =============================================================================
# Environment Setup Instructions
# =============================================================================
# To use this Makefile, you need to set up the following environment variables:
#
# For the NEW database (where data will be migrated TO):
# DATABASE_HOST=localhost
# DATABASE_PORT=3306
# DATABASE_NAME=brnd_db_new
# DATABASE_USER=root
# DATABASE_PASSWORD=your_password
#
# For the PRODUCTION database (where data will be migrated FROM - READ-ONLY):
# PROD_DATABASE_HOST=your_prod_host
# PROD_DATABASE_PORT=3306
# PROD_DATABASE_NAME=brnd_db_prod
# PROD_DATABASE_USER=readonly_user
# PROD_DATABASE_PASSWORD=readonly_password
#
# These should be added to your .env file.
# =============================================================================