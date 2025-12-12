/**
 * Migration Script: Verify Production Database is Read-Only
 * 
 * This script verifies that the production database connection is read-only
 * by attempting a test write operation. If the write succeeds, the migration
 * is aborted for safety.
 */

import { DataSource } from 'typeorm';
import * as mysql from 'mysql2/promise';

async function verifyProductionReadOnly() {
  console.log('Connecting to production database...');

  // Get production database configuration from environment variables
  const prodConfig = {
    host: process.env.PROD_DATABASE_HOST,
    port: parseInt(process.env.PROD_DATABASE_PORT || '3306', 10),
    database: process.env.PROD_DATABASE_NAME,
    user: process.env.PROD_DATABASE_USER,
    password: process.env.PROD_DATABASE_PASSWORD,
  };

  // Validate that all required environment variables are set
  if (!prodConfig.host || !prodConfig.database || !prodConfig.user || !prodConfig.password) {
    console.error('❌ ERROR: Missing required production database environment variables:');
    console.error('   Required: PROD_DATABASE_HOST, PROD_DATABASE_NAME, PROD_DATABASE_USER, PROD_DATABASE_PASSWORD');
    console.error('   Optional: PROD_DATABASE_PORT (defaults to 3306)');
    process.exit(1);
  }

  // Validate that production and new database are different
  const newConfig = {
    host: process.env.DATABASE_HOST,
    database: process.env.DATABASE_NAME,
  };

  if (prodConfig.host === newConfig.host && prodConfig.database === newConfig.database) {
    console.error('❌ ERROR: Production and new database configurations are the same!');
    console.error('   This is a safety check to prevent accidental data loss.');
    console.error('   Please ensure PROD_DATABASE_* and DATABASE_* point to different databases.');
    process.exit(1);
  }

  let connection: mysql.Connection | null = null;

  try {
    // Create a direct MySQL connection (not TypeORM) for testing
    connection = await mysql.createConnection({
      host: prodConfig.host,
      port: prodConfig.port,
      database: prodConfig.database,
      user: prodConfig.user,
      password: prodConfig.password,
    });

    console.log('✓ Connected to production database');
    console.log(`  Host: ${prodConfig.host}:${prodConfig.port}`);
    console.log(`  Database: ${prodConfig.database}`);
    console.log(`  User: ${prodConfig.user}`);

    // Attempt to create a test table (this should fail if read-only)
    console.log('\nAttempting test write operation (should fail if read-only)...');
    
    try {
      await connection.execute(`
        CREATE TABLE IF NOT EXISTS _migration_readonly_test (
          id INT PRIMARY KEY AUTO_INCREMENT,
          test_value VARCHAR(255),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB
      `);

      // If we get here, the write succeeded - this is BAD!
      console.error('❌ ERROR: Production database is NOT read-only!');
      console.error('   The test write operation succeeded, which means the connection has write permissions.');
      console.error('   Migration aborted for safety.');
      console.error('');
      console.error('   Please ensure the production database user has READ-ONLY permissions.');
      
      // Clean up the test table if it was created
      try {
        await connection.execute('DROP TABLE IF EXISTS _migration_readonly_test');
      } catch (e) {
        // Ignore cleanup errors
      }

      process.exit(1);
    } catch (writeError: any) {
      // Good! The write failed, which means we're read-only
      const errorMessage = writeError.message || String(writeError);
      
      // Check if it's a permission error (which is what we want)
      if (
        errorMessage.includes('Access denied') ||
        errorMessage.includes('read-only') ||
        errorMessage.includes('readonly') ||
        errorMessage.includes('SELECT command denied') ||
        errorMessage.includes('CREATE command denied') ||
        errorMessage.includes('INSERT command denied') ||
        errorMessage.includes('UPDATE command denied') ||
        errorMessage.includes('DELETE command denied') ||
        errorMessage.includes('DROP command denied')
      ) {
        console.log('✓ Test write operation failed as expected (read-only verified)');
        console.log(`  Error type: ${errorMessage.substring(0, 100)}...`);
        console.log('');
        console.log('✓ Production database is confirmed READ-ONLY');
        console.log('  Safe to proceed with migration');
      } else {
        // Unexpected error - might be a connection issue
        console.error('⚠️  WARNING: Unexpected error during read-only test:');
        console.error(`   ${errorMessage}`);
        console.error('');
        console.error('   This might indicate a connection issue rather than read-only permissions.');
        console.error('   Please verify your production database connection manually.');
        process.exit(1);
      }
    }

    // Test that we CAN read (this should succeed)
    console.log('\nTesting read access...');
    try {
      const [rows] = await connection.execute('SELECT 1 as test');
      console.log('✓ Read access confirmed');
    } catch (readError: any) {
      console.error('❌ ERROR: Cannot read from production database!');
      console.error(`   ${readError.message}`);
      process.exit(1);
    }

  } catch (error: any) {
    console.error('❌ ERROR: Failed to connect to production database:');
    console.error(`   ${error.message}`);
    console.error('');
    console.error('   Please check your PROD_DATABASE_* environment variables.');
    process.exit(1);
  } finally {
    if (connection) {
      await connection.end();
      console.log('\n✓ Connection closed');
    }
  }
}

// Run the verification
verifyProductionReadOnly()
  .then(() => {
    console.log('\n✓ Verification completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n❌ Verification failed:');
    console.error(error);
    process.exit(1);
  });

