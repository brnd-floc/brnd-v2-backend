/**
 * Migration Script: Apply New Schema
 *
 * This script creates all tables in the new database using TypeORM's synchronize feature.
 * It uses the entity models defined in src/models/ to create the schema.
 *
 * This is safe because we just cleaned the database in the previous step.
 */

import { DataSource } from 'typeorm';
import {
  User,
  Brand,
  Category,
  Tag,
  BrandTags,
  UserBrandVotes,
  UserDailyActions,
  AirdropScore,
  AirdropSnapshot,
  AirdropLeaf,
  RewardClaim,
} from '../src/models';

async function applyNewSchema() {
  console.log('Initializing TypeORM DataSource for new database...');

  // Get new database configuration from environment variables
  const newConfig = {
    host: process.env.DATABASE_HOST || 'localhost',
    port: parseInt(process.env.DATABASE_PORT || '3306', 10),
    database: process.env.DATABASE_NAME,
    username: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
    requireSSL:
      process.env.DATABASE_SSL === 'true' ||
      process.env.NODE_ENV === 'production',
  };

  // Validate that all required environment variables are set
  if (!newConfig.database || !newConfig.username || !newConfig.password) {
    console.error('❌ ERROR: Missing required database environment variables:');
    console.error(
      '   Required: DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD',
    );
    console.error(
      '   Optional: DATABASE_HOST (defaults to localhost), DATABASE_PORT (defaults to 3306)',
    );
    process.exit(1);
  }

  // Create TypeORM DataSource with synchronize enabled
  const dataSource = new DataSource({
    type: 'mysql',
    host: newConfig.host,
    port: newConfig.port,
    username: newConfig.username,
    password: newConfig.password,
    database: newConfig.database,
    entities: [
      User,
      Brand,
      Category,
      Tag,
      BrandTags,
      UserBrandVotes,
      UserDailyActions,
      AirdropScore,
      AirdropSnapshot,
      AirdropLeaf,
      RewardClaim,
    ],
    synchronize: true, // Enable synchronize to create tables
    logging: false,
    ssl: newConfig.requireSSL
      ? {
          rejectUnauthorized: false,
        }
      : false,
    extra: {
      insecureAuth: !newConfig.requireSSL,
    },
  });

  try {
    console.log('Connecting to new database...');
    console.log(`  Host: ${newConfig.host}:${newConfig.port}`);
    console.log(`  Database: ${newConfig.database}`);
    console.log(`  User: ${newConfig.username}`);

    // Initialize the connection
    // With synchronize: true, TypeORM will automatically create tables during initialize()
    await dataSource.initialize();
    console.log('✓ Connected to new database');

    console.log('\nApplying schema (creating tables)...');
    console.log('  This will create the following tables:');
    console.log('    - Core: users, brands, categories, tags, brand_tags');
    console.log('    - Voting: user_brand_votes, user_daily_actions');
    console.log(
      '    - New: airdrop_scores, airdrop_snapshots, airdrop_leaves, reward_claims',
    );

    // TypeORM's synchronize: true automatically creates tables during initialize()
    // Small delay to ensure synchronization completes
    await new Promise((resolve) => setTimeout(resolve, 500));

    // Verify tables were created by querying information_schema
    const queryRunner = dataSource.createQueryRunner();
    const tables = (await queryRunner.query(
      `SELECT TABLE_NAME 
       FROM information_schema.TABLES 
       WHERE TABLE_SCHEMA = ? 
       AND TABLE_TYPE = 'BASE TABLE'
       ORDER BY TABLE_NAME`,
      [newConfig.database],
    )) as Array<{ TABLE_NAME: string }>;

    console.log(`\n✓ Schema applied successfully`);
    console.log(`  Created ${tables?.length || 0} table(s):`);
    if (tables && Array.isArray(tables)) {
      tables.forEach((table, index) => {
        console.log(`    ${index + 1}. ${table.TABLE_NAME}`);
      });
    }

    // Release query runner
    await queryRunner.release();
  } catch (error: any) {
    console.error('\n❌ ERROR: Failed to apply schema:');
    console.error(`   ${error.message}`);
    console.error('');
    console.error(
      '   Please check your DATABASE_* environment variables and connection.',
    );
    process.exit(1);
  } finally {
    // Close the connection
    if (dataSource.isInitialized) {
      await dataSource.destroy();
      console.log('\n✓ Connection closed');
    }
  }
}

// Run the schema application
applyNewSchema()
  .then(() => {
    console.log('\n✓ Schema application completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n❌ Schema application failed:');
    console.error(error);
    process.exit(1);
  });
