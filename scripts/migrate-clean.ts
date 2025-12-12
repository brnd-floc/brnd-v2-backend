/**
 * Migration Script: Clean New Database
 * 
 * This script completely wipes the new database by dropping all tables.
 * This ensures a clean slate for the new schema and data migration.
 * 
 * WARNING: This will delete ALL data in the new database!
 */

import { DataSource } from 'typeorm';
import * as mysql from 'mysql2/promise';

async function cleanNewDatabase() {
  console.log('Connecting to new database...');

  // Get new database configuration from environment variables
  const newConfig = {
    host: process.env.DATABASE_HOST || 'localhost',
    port: parseInt(process.env.DATABASE_PORT || '3306', 10),
    database: process.env.DATABASE_NAME,
    user: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
  };

  // Validate that all required environment variables are set
  if (!newConfig.database || !newConfig.user || !newConfig.password) {
    console.error('❌ ERROR: Missing required database environment variables:');
    console.error('   Required: DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD');
    console.error('   Optional: DATABASE_HOST (defaults to localhost), DATABASE_PORT (defaults to 3306)');
    process.exit(1);
  }

  let connection: mysql.Connection | null = null;

  try {
    // Create a direct MySQL connection for dropping tables
    connection = await mysql.createConnection({
      host: newConfig.host,
      port: newConfig.port,
      database: newConfig.database,
      user: newConfig.user,
      password: newConfig.password,
    });

    console.log('✓ Connected to new database');
    console.log(`  Host: ${newConfig.host}:${newConfig.port}`);
    console.log(`  Database: ${newConfig.database}`);
    console.log(`  User: ${newConfig.user}`);

    // Disable foreign key checks temporarily to allow dropping tables in any order
    console.log('\nDisabling foreign key checks...');
    await connection.execute('SET FOREIGN_KEY_CHECKS = 0');
    console.log('✓ Foreign key checks disabled');

    // Get all table names
    console.log('\nFetching list of tables...');
    const [tables] = await connection.execute<mysql.RowDataPacket[]>(
      `SELECT TABLE_NAME 
       FROM information_schema.TABLES 
       WHERE TABLE_SCHEMA = ? 
       AND TABLE_TYPE = 'BASE TABLE'`,
      [newConfig.database]
    );

    if (tables.length === 0) {
      console.log('✓ Database is already empty (no tables found)');
    } else {
      console.log(`Found ${tables.length} table(s) to drop:`);
      tables.forEach((table, index) => {
        console.log(`  ${index + 1}. ${table.TABLE_NAME}`);
      });

      // Drop each table
      console.log('\nDropping tables...');
      for (const table of tables) {
        const tableName = table.TABLE_NAME;
        try {
          await connection.execute(`DROP TABLE IF EXISTS \`${tableName}\``);
          console.log(`  ✓ Dropped table: ${tableName}`);
        } catch (error: any) {
          console.error(`  ❌ Failed to drop table ${tableName}: ${error.message}`);
          throw error;
        }
      }
    }

    // Re-enable foreign key checks
    console.log('\nRe-enabling foreign key checks...');
    await connection.execute('SET FOREIGN_KEY_CHECKS = 1');
    console.log('✓ Foreign key checks re-enabled');

    console.log('\n✓ Database cleaning completed successfully');
    console.log('  All tables have been dropped');
    console.log('  Database is now ready for new schema');

  } catch (error: any) {
    console.error('\n❌ ERROR: Failed to clean database:');
    console.error(`   ${error.message}`);
    console.error('');
    console.error('   Please check your DATABASE_* environment variables and connection.');
    
    // Try to re-enable foreign key checks even if there was an error
    if (connection) {
      try {
        await connection.execute('SET FOREIGN_KEY_CHECKS = 1');
      } catch (e) {
        // Ignore cleanup errors
      }
    }

    process.exit(1);
  } finally {
    if (connection) {
      await connection.end();
      console.log('\n✓ Connection closed');
    }
  }
}

// Run the cleaning
cleanNewDatabase()
  .then(() => {
    console.log('\n✓ Clean operation completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n❌ Clean operation failed:');
    console.error(error);
    process.exit(1);
  });

