/**
 * Migration Script: Migrate Data from Production to New Database
 *
 * This script copies all data from the production database to the new database.
 * It handles schema transformations (e.g., INT to STRING conversions) and
 * migrates data in dependency order to maintain referential integrity.
 *
 * Migration Order (respecting dependencies):
 * 1. Categories (no dependencies)
 * 2. Tags (no dependencies)
 * 3. Brands (depends on Categories)
 * 4. Users (no dependencies)
 * 5. BrandTags (depends on Brands and Tags)
 * 6. UserBrandVotes (depends on Users and Brands)
 * 7. UserDailyActions (depends on Users)
 *
 * Note: New entities (RewardClaim) will remain empty as they don't exist in production.
 */

import { DataSource } from 'typeorm';
import * as mysql from 'mysql2/promise';
import { createHash } from 'crypto';

// Import new schema entities
import {
  User,
  Brand,
  Category,
  Tag,
  BrandTags,
  UserBrandVotes,
  UserDailyActions,
} from '../src/models';

/**
 * Converts a UUID to a transaction hash-like format (0x + 64 hex characters)
 * Uses SHA-256 hash of the UUID to ensure uniqueness and proper length
 */
function uuidToTransactionHash(uuid: string): string {
  // Remove hyphens from UUID if present
  const cleanUuid = uuid.replace(/-/g, '');

  // Create a deterministic hash to ensure 64 hex characters
  // Using SHA-256 which always produces 64 hex characters
  const hash = createHash('sha256').update(uuid).digest('hex');

  // Return in transaction hash format: 0x + 64 hex chars
  return `0x${hash}`;
}

interface MigrationStats {
  categories: number;
  tags: number;
  brands: number;
  users: number;
  brandTags: number;
  userBrandVotes: number;
  userDailyActions: number;
}

async function migrateData() {
  console.log('Initializing database connections...');

  // Production database configuration (read-only)
  const prodConfig = {
    host: process.env.PROD_DATABASE_HOST,
    port: parseInt(process.env.PROD_DATABASE_PORT || '3306', 10),
    database: process.env.PROD_DATABASE_NAME,
    user: process.env.PROD_DATABASE_USER,
    password: process.env.PROD_DATABASE_PASSWORD,
  };

  // New database configuration
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

  // Validate configurations
  if (
    !prodConfig.host ||
    !prodConfig.database ||
    !prodConfig.user ||
    !prodConfig.password
  ) {
    console.error(
      '❌ ERROR: Missing required production database environment variables',
    );
    process.exit(1);
  }

  if (!newConfig.database || !newConfig.username || !newConfig.password) {
    console.error(
      '❌ ERROR: Missing required new database environment variables',
    );
    process.exit(1);
  }

  let prodConnection: mysql.Connection | null = null;
  let newDataSource: DataSource | null = null;
  const stats: MigrationStats = {
    categories: 0,
    tags: 0,
    brands: 0,
    users: 0,
    brandTags: 0,
    userBrandVotes: 0,
    userDailyActions: 0,
  };

  try {
    // Connect to production database (read-only, direct MySQL)
    console.log('Connecting to production database (read-only)...');
    prodConnection = await mysql.createConnection({
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

    // Connect to new database (TypeORM)
    console.log('Connecting to new database...');
    newDataSource = new DataSource({
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
      ],
      synchronize: false, // Don't auto-sync, we're just inserting data
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

    await newDataSource.initialize();
    console.log('✓ Connected to new database');
    console.log(`  Host: ${newConfig.host}:${newConfig.port}`);
    console.log(`  Database: ${newConfig.database}`);
    console.log(`  User: ${newConfig.username}`);
    console.log('');

    // ========================================================================
    // Step 1: Migrate Categories
    // ========================================================================
    console.log('Step 1: Migrating Categories...');
    const [prodCategories] = await prodConnection.execute<
      mysql.RowDataPacket[]
    >('SELECT * FROM categories ORDER BY id');

    if (prodCategories.length > 0) {
      const categoryRepo = newDataSource.getRepository(Category);
      for (const cat of prodCategories) {
        const newCategory = categoryRepo.create({
          id: cat.id,
          name: cat.name,
          createdAt: cat.createdAt || cat.created_at,
          updatedAt: cat.updatedAt || cat.updated_at,
        });
        await categoryRepo.save(newCategory);
      }
      stats.categories = prodCategories.length;
      console.log(`  ✓ Migrated ${stats.categories} categories`);
    } else {
      console.log('  ✓ No categories to migrate');
    }

    // ========================================================================
    // Step 2: Migrate Tags
    // ========================================================================
    console.log('\nStep 2: Migrating Tags...');
    const [prodTags] = await prodConnection.execute<mysql.RowDataPacket[]>(
      'SELECT * FROM tags ORDER BY id',
    );

    if (prodTags.length > 0) {
      const tagRepo = newDataSource.getRepository(Tag);
      for (const tag of prodTags) {
        const newTag = tagRepo.create({
          id: tag.id,
          name: tag.name,
          createdAt: tag.createdAt || tag.created_at,
          updatedAt: tag.updatedAt || tag.updated_at,
        });
        await tagRepo.save(newTag);
      }
      stats.tags = prodTags.length;
      console.log(`  ✓ Migrated ${stats.tags} tags`);
    } else {
      console.log('  ✓ No tags to migrate');
    }

    // ========================================================================
    // Step 3: Migrate Brands
    // ========================================================================
    console.log('\nStep 3: Migrating Brands...');
    // Select all columns - date formatting will be handled in code to preserve exact values
    const [prodBrands] = await prodConnection.execute<mysql.RowDataPacket[]>(
      'SELECT * FROM brands ORDER BY id',
    );

    if (prodBrands.length > 0) {
      console.log(`  Found ${prodBrands.length} brands to migrate`);

      // Get category IDs for foreign key mapping
      const categoryRepo = newDataSource.getRepository(Category);
      const allCategories = await categoryRepo.find();
      const categoryMap = new Map(
        allCategories.map((cat) => {
          const id = Number(cat.id);
          return [id, id];
        }),
      ); // Map ID to ID for validation (ensure numbers)

      console.log(
        `  Found ${allCategories.length} categories in new database for foreign key validation`,
      );
      if (allCategories.length > 0) {
        const categoryIds = allCategories
          .map((c) => Number(c.id))
          .sort((a, b) => a - b);
        console.log(
          `     Category IDs (as numbers): ${categoryIds.join(', ')}`,
        );
        // Verify categoryMap is built correctly
        console.log(
          `     CategoryMap keys: ${Array.from(categoryMap.keys())
            .sort((a, b) => a - b)
            .join(', ')}`,
        );
        // Test lookup
        categoryIds.forEach((id) => {
          if (!categoryMap.has(id)) {
            console.error(
              `     ⚠️  WARNING: Category ID ${id} not found in categoryMap!`,
            );
          }
        });
      }

      if (allCategories.length === 0) {
        console.warn(
          '  ⚠️  WARNING: No categories found in new database! All brands will be migrated with categoryId = NULL.',
        );
      }

      // Use raw SQL for much faster bulk insert - process all at once since it's only 402 brands
      const batchSize = prodBrands.length; // Insert all brands in one go
      let processed = 0;
      let invalidCategoryCount = 0;
      const invalidCategoryIds = new Set<number>();
      const actualCategoryIdsInSQL: (number | 'NULL')[] = []; // Track what we're actually inserting
      const allCategoryIdsInSQL: (number | 'NULL')[] = []; // Track ALL categoryIds for validation

      // Build VALUES clause for batch insert
      const values = prodBrands
        .map((brand, index) => {
          processed++;

          // Debug first 5 brands to see what's happening
          if (index < 5) {
            const rawCatId = brand.categoryId || brand.category_id;
            console.log(
              `  [DEBUG Brand ${brand.id}] rawCategoryId: ${rawCatId}, type: ${typeof rawCatId}`,
            );
          }

          // Escape values for SQL safety
          const escapeValue = (val: any) => {
            if (val === null || val === undefined) return 'NULL';
            if (typeof val === 'number') return val.toString();
            if (typeof val === 'boolean') return val ? '1' : '0';
            if (val instanceof Date) {
              // Format date preserving local time components (no timezone conversion)
              const year = val.getFullYear();
              const month = String(val.getMonth() + 1).padStart(2, '0');
              const day = String(val.getDate()).padStart(2, '0');
              const hours = String(val.getHours()).padStart(2, '0');
              const minutes = String(val.getMinutes()).padStart(2, '0');
              const seconds = String(val.getSeconds()).padStart(2, '0');
              return `'${year}-${month}-${day} ${hours}:${minutes}:${seconds}'`;
            }
            if (typeof val === 'string' && val.match(/^\d{4}-\d{2}-\d{2}/)) {
              // Already a date string, just escape
              return `'${val}'`;
            }
            return `'${String(val).replace(/'/g, "''")}'`;
          };

          // Helper to format dates - preserves exact date from production DB
          const formatDate = (dateVal: any) => {
            if (!dateVal) return 'NOW()'; // Use current timestamp if null

            // If it's already a MySQL datetime string, use it directly
            if (typeof dateVal === 'string') {
              const mysqlMatch = dateVal.match(
                /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/,
              );
              if (mysqlMatch) {
                return `'${mysqlMatch[1]} ${mysqlMatch[2]}'`;
              }
            }

            // If it's a Date object, format preserving local time components
            if (dateVal instanceof Date) {
              const year = dateVal.getFullYear();
              const month = String(dateVal.getMonth() + 1).padStart(2, '0');
              const day = String(dateVal.getDate()).padStart(2, '0');
              const hours = String(dateVal.getHours()).padStart(2, '0');
              const minutes = String(dateVal.getMinutes()).padStart(2, '0');
              const seconds = String(dateVal.getSeconds()).padStart(2, '0');
              return `'${year}-${month}-${day} ${hours}:${minutes}:${seconds}'`;
            }

            return 'NOW()'; // Fallback to current timestamp
          };

          // Get category ID (foreign key) - ALWAYS default to null if there's any issue
          let validCategoryId: number | null = null;

          const rawCategoryId =
            brand.categoryId !== null && brand.categoryId !== undefined
              ? brand.categoryId
              : brand.category_id !== null && brand.category_id !== undefined
                ? brand.category_id
                : null;

          // Only process if we have a raw category ID that's not empty
          if (
            rawCategoryId !== null &&
            rawCategoryId !== undefined &&
            rawCategoryId !== '' &&
            String(rawCategoryId).trim() !== ''
          ) {
            const categoryIdNum = Number(rawCategoryId);
            // Triple-check: must be valid number, integer, positive, AND exist in categoryMap
            const isValid =
              !isNaN(categoryIdNum) &&
              Number.isInteger(categoryIdNum) &&
              categoryIdNum > 0 &&
              categoryMap.has(categoryIdNum);

            if (isValid) {
              validCategoryId = categoryIdNum;
            } else {
              // Track invalid category IDs for reporting
              invalidCategoryCount++;
              if (!isNaN(categoryIdNum) && categoryIdNum > 0) {
                invalidCategoryIds.add(categoryIdNum);
              }
              // CRITICAL: Always set to null for invalid IDs
              validCategoryId = null;
            }
          }
          // CRITICAL: Ensure validCategoryId is ALWAYS null if not valid
          // Double-check one more time before using
          if (
            validCategoryId !== null &&
            (!categoryMap.has(validCategoryId) || validCategoryId <= 0)
          ) {
            validCategoryId = null;
          }

          // Determine final categoryId value for SQL
          let finalCategoryIdForSQL: number | 'NULL';
          if (validCategoryId === null || validCategoryId === undefined) {
            finalCategoryIdForSQL = 'NULL';
          } else if (
            !categoryMap.has(validCategoryId) ||
            validCategoryId <= 0
          ) {
            finalCategoryIdForSQL = 'NULL';
          } else {
            finalCategoryIdForSQL = validCategoryId;
          }

          // Track what we're actually inserting
          allCategoryIdsInSQL.push(finalCategoryIdForSQL);
          if (index < 20) {
            actualCategoryIdsInSQL.push(finalCategoryIdForSQL);
          }

          // Transform ranking from INT to STRING
          const ranking =
            brand.ranking !== null && brand.ranking !== undefined
              ? String(brand.ranking)
              : '0';

          return `(
                ${escapeValue(brand.id)},
                ${escapeValue(brand.name)},
                ${escapeValue(brand.url)},
                ${escapeValue(brand.warpcastUrl || brand.warpcast_url)},
                ${escapeValue(brand.description)},
                ${escapeValue(brand.followerCount || brand.follower_count || 0)},
                ${escapeValue(brand.imageUrl || brand.image_url)},
                ${escapeValue(brand.profile)},
                ${escapeValue(brand.channel)},
                ${escapeValue(ranking)},
                ${escapeValue(0)},
                ${escapeValue(0)},
                ${escapeValue(0)},
                ${escapeValue(0)},
                ${escapeValue(0)},
                ${escapeValue(0)},
                ${escapeValue(0)},
                ${escapeValue(0)},
                ${escapeValue(0)},
                ${escapeValue(0)},
                ${escapeValue(0)},
                ${escapeValue(brand.banned || 0)},
                ${escapeValue(brand.queryType || brand.query_type || 0)},
                ${escapeValue(brand.currentRanking || brand.current_ranking || 0)},
                ${(() => {
                  if (finalCategoryIdForSQL === 'NULL') {
                    return 'NULL';
                  }
                  if (typeof finalCategoryIdForSQL !== 'number') {
                    console.error(
                      `  ❌ CRITICAL: Brand ${brand.id} has non-numeric finalCategoryIdForSQL: ${finalCategoryIdForSQL} (type: ${typeof finalCategoryIdForSQL})`,
                    );
                    return 'NULL';
                  }
                  if (finalCategoryIdForSQL <= 0) {
                    console.error(
                      `  ❌ CRITICAL: Brand ${brand.id} has invalid finalCategoryIdForSQL (<= 0): ${finalCategoryIdForSQL}`,
                    );
                    return 'NULL';
                  }
                  if (!categoryMap.has(finalCategoryIdForSQL)) {
                    console.error(
                      `  ❌ CRITICAL: Brand ${brand.id} has finalCategoryIdForSQL not in categoryMap: ${finalCategoryIdForSQL}`,
                    );
                    console.error(
                      `     Available category IDs: ${Array.from(
                        categoryMap.keys(),
                      )
                        .sort((a, b) => a - b)
                        .join(', ')}`,
                    );
                    return 'NULL';
                  }
                  return finalCategoryIdForSQL;
                })()},
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                0,
                NULL,
                NULL,
                NULL,
                ${formatDate(brand.createdAt || brand.created_at)},
                ${formatDate(brand.updatedAt || brand.updated_at)}
              )`;
        })
        .join(',\n');

      // Report invalid category IDs
      if (invalidCategoryCount > 0) {
        console.warn(
          `  ⚠️  WARNING: Found ${invalidCategoryCount} brands with invalid categoryIds that will be set to NULL`,
        );
        console.warn(
          `     Invalid category IDs from production: ${Array.from(
            invalidCategoryIds,
          )
            .sort((a, b) => a - b)
            .join(', ')}`,
        );
        console.warn(
          `     Available category IDs in new database: ${Array.from(
            categoryMap.keys(),
          )
            .sort((a, b) => a - b)
            .join(', ')}`,
        );
        console.warn(
          `     These brands will be migrated with categoryId = NULL`,
        );
      }

      // Report what categoryId values we're actually inserting
      if (actualCategoryIdsInSQL.length > 0) {
        console.log(
          `  Actual categoryId values being inserted (first 20): ${actualCategoryIdsInSQL.join(', ')}`,
        );
      }

      // CRITICAL: Validate ALL categoryIds before executing SQL
      const invalidInSQL = allCategoryIdsInSQL.filter(
        (id) =>
          id !== 'NULL' &&
          (typeof id !== 'number' || !categoryMap.has(id) || id <= 0),
      );

      if (invalidInSQL.length > 0) {
        console.error(
          `  ❌ ERROR: Found ${invalidInSQL.length} invalid categoryIds in SQL!`,
        );
        console.error(
          `     Invalid IDs: ${Array.from(new Set(invalidInSQL)).slice(0, 20).join(', ')}${invalidInSQL.length > 20 ? '...' : ''}`,
        );
        console.error(
          `     Available category IDs: ${Array.from(categoryMap.keys())
            .sort((a, b) => a - b)
            .join(', ')}`,
        );
        throw new Error(
          `Cannot proceed: Found ${invalidInSQL.length} invalid categoryIds in generated SQL`,
        );
      }

      // Also check for any numeric values that aren't in the map
      const numericIds = allCategoryIdsInSQL.filter(
        (id) => typeof id === 'number',
      ) as number[];
      const invalidNumericIds = numericIds.filter((id) => !categoryMap.has(id));
      if (invalidNumericIds.length > 0) {
        console.error(
          `  ❌ ERROR: Found ${invalidNumericIds.length} numeric categoryIds not in categoryMap!`,
        );
        console.error(
          `     Invalid numeric IDs: ${Array.from(new Set(invalidNumericIds)).slice(0, 20).join(', ')}${invalidNumericIds.length > 20 ? '...' : ''}`,
        );
        throw new Error(
          `Cannot proceed: Found ${invalidNumericIds.length} categoryIds not in categoryMap`,
        );
      }

      console.log(
        `  ✓ Validated all ${allCategoryIdsInSQL.length} categoryIds in tracking array - all are valid or NULL`,
      );

      // Validation already done in tracking array and template check
      // The template has a final safety check that will catch any invalid values
      console.log(
        `  Column count: 39, Value count: ${values.split(',').length / prodBrands.length}`,
      );
      const insertSQL = `
        INSERT INTO brands (
          id, name, url, warpcastUrl, description, followerCount, imageUrl,
          profile, channel, ranking, score, stateScore, scoreDay, stateScoreDay,
          scoreWeek, stateScoreWeek, rankingWeek, scoreMonth, stateScoreMonth,
          rankingMonth, bonusPoints, banned, queryType, currentRanking, categoryId,
          walletAddress, totalBrndAwarded, availableBrnd, onChainCreatedAt, onChainId,
          onChainFid, onChainHandle, onChainWalletAddress, metadataHash,
          isUploadedToContract, founderFid, ticker, contractAddress, createdAt, updatedAt
        ) VALUES ${values}
      `;

      try {
        console.log(
          `  Inserting all ${prodBrands.length} brands in one batch...`,
        );
        const queryRunner = newDataSource.createQueryRunner();
        await queryRunner.query(insertSQL);
        await queryRunner.release();

        console.log(
          `  ✓ Migrated all ${processed} brands in single batch using raw SQL`,
        );
      } catch (error: any) {
        console.error(`  ❌ Failed to migrate brands: ${error.message}`);
        console.error(`  SQL: ${insertSQL.substring(0, 500)}...`);
        throw error;
      }

      stats.brands = prodBrands.length;
      console.log(
        `  ✓ Migrated ${stats.brands} brands successfully using raw SQL`,
      );
    } else {
      console.log('  ✓ No brands to migrate');
    }

    // ========================================================================
    // Step 4: Migrate Users
    // ========================================================================
    console.log('\nStep 4: Migrating Users...');
    // Select all columns - date formatting will be handled in code to preserve exact values
    const [prodUsers] = await prodConnection.execute<mysql.RowDataPacket[]>(
      'SELECT * FROM users ORDER BY id',
    );

    if (prodUsers.length > 0) {
      console.log(`  Found ${prodUsers.length} users to migrate`);

      // Use raw SQL for much faster bulk insert
      const batchSize = 1000; // Much larger batches with raw SQL
      let processed = 0;

      for (let i = 0; i < prodUsers.length; i += batchSize) {
        const batch = prodUsers.slice(i, i + batchSize);

        // Build VALUES clause for batch insert
        const values = batch
          .map((user) => {
            processed++;

            // Escape values for SQL safety
            const escapeValue = (val: any) => {
              if (val === null || val === undefined) return 'NULL';
              if (typeof val === 'number') return val.toString();
              if (typeof val === 'boolean') return val ? '1' : '0';
              if (val instanceof Date) {
                // Format date preserving local time components (no timezone conversion)
                const year = val.getFullYear();
                const month = String(val.getMonth() + 1).padStart(2, '0');
                const day = String(val.getDate()).padStart(2, '0');
                const hours = String(val.getHours()).padStart(2, '0');
                const minutes = String(val.getMinutes()).padStart(2, '0');
                const seconds = String(val.getSeconds()).padStart(2, '0');
                return `'${year}-${month}-${day} ${hours}:${minutes}:${seconds}'`;
              }
              if (typeof val === 'string' && val.match(/^\d{4}-\d{2}-\d{2}/)) {
                // Already a date string, just escape
                return `'${val}'`;
              }
              return `'${String(val).replace(/'/g, "''")}'`;
            };

            // Helper to format dates - preserves exact date from production DB
            const formatDate = (dateVal: any) => {
              if (!dateVal) return 'NOW()'; // Use current timestamp if null

              // If it's already a MySQL datetime string, use it directly
              if (typeof dateVal === 'string') {
                const mysqlMatch = dateVal.match(
                  /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/,
                );
                if (mysqlMatch) {
                  return `'${mysqlMatch[1]} ${mysqlMatch[2]}'`;
                }
              }

              // If it's a Date object, format preserving local time components
              if (dateVal instanceof Date) {
                const year = dateVal.getFullYear();
                const month = String(dateVal.getMonth() + 1).padStart(2, '0');
                const day = String(dateVal.getDate()).padStart(2, '0');
                const hours = String(dateVal.getHours()).padStart(2, '0');
                const minutes = String(dateVal.getMinutes()).padStart(2, '0');
                const seconds = String(dateVal.getSeconds()).padStart(2, '0');
                return `'${year}-${month}-${day} ${hours}:${minutes}:${seconds}'`;
              }

              return 'NOW()'; // Fallback to current timestamp
            };

            return `(
            ${escapeValue(user.id)},
            ${escapeValue(user.fid)},
            ${escapeValue(user.username)},
            ${escapeValue(user.photoUrl || user.photo_url)},
            ${escapeValue(user.points || 0)},
            ${escapeValue(user.role || 'user')},
            0,
            NULL,
            0,
            0,
            0,
            0,
            0,
            NULL,
            NULL,
            0,
            0,
            0,
            NULL,
            ${user.notificationsEnabled === true || user.notificationsEnabled === 1 ? '1' : '0'},
            ${escapeValue(user.notificationToken || user.notification_token)},
            ${formatDate(user.lastVoteReminderSent || user.last_vote_reminder_sent)},
            0.0,
            ${formatDate(user.createdAt || user.created_at)},
            ${formatDate(user.updatedAt || user.updated_at)}
          )`;
          })
          .join(',\n');

        const insertSQL = `
          INSERT INTO users (
            id, fid, username, photoUrl, points, role,
            dailyStreak, maxDailyStreak, totalPodiums, votedBrandsCount,
            brndPowerLevel, totalVotes, lastVoteDay, lastVoteTimestamp,
            address, banned, powerups, verified, favoriteBrandId,
            notificationsEnabled, notificationToken, lastVoteReminderSent,
            neynarScore, createdAt, updatedAt
          ) VALUES ${values}
        `;

        try {
          const queryRunner = newDataSource.createQueryRunner();
          await queryRunner.query(insertSQL);
          await queryRunner.release();

          console.log(
            `  ✓ Migrated batch ${Math.ceil((i + batchSize) / batchSize)}/${Math.ceil(prodUsers.length / batchSize)} (${processed}/${prodUsers.length} users)`,
          );
        } catch (error: any) {
          console.error(
            `  ❌ Failed to migrate user batch starting at ${i + 1}: ${error.message}`,
          );
          console.error(`  SQL: ${insertSQL.substring(0, 500)}...`);
          throw error;
        }
      }

      stats.users = prodUsers.length;
      console.log(
        `  ✓ Migrated ${stats.users} users successfully using raw SQL`,
      );
    } else {
      console.log('  ✓ No users to migrate');
    }

    // ========================================================================
    // Step 5: Migrate BrandTags
    // ========================================================================
    console.log('\nStep 5: Migrating BrandTags...');
    const [prodBrandTags] = await prodConnection.execute<mysql.RowDataPacket[]>(
      'SELECT * FROM brand_tags ORDER BY id',
    );

    if (prodBrandTags.length > 0) {
      const brandTagsRepo = newDataSource.getRepository(BrandTags);
      const brandRepo = newDataSource.getRepository(Brand);
      const tagRepo = newDataSource.getRepository(Tag);

      for (const bt of prodBrandTags) {
        const brand = await brandRepo.findOne({
          where: { id: bt.brandId || bt.brand_id },
        });
        const tag = await tagRepo.findOne({
          where: { id: bt.tagId || bt.tag_id },
        });

        if (brand && tag) {
          const newBrandTag = brandTagsRepo.create({
            id: bt.id,
            brand: brand,
            tag: tag,
          });
          await brandTagsRepo.save(newBrandTag);
        }
      }
      stats.brandTags = prodBrandTags.length;
      console.log(`  ✓ Migrated ${stats.brandTags} brand-tag relationships`);
    } else {
      console.log('  ✓ No brand-tag relationships to migrate');
    }

    // ========================================================================
    // Step 6: Migrate UserBrandVotes
    // ========================================================================
    console.log('\nStep 6: Migrating UserBrandVotes...');
    // Select all columns - date formatting will be handled in code to preserve exact values
    const [prodVotes] = await prodConnection.execute<mysql.RowDataPacket[]>(
      'SELECT * FROM user_brand_votes ORDER BY id',
    );

    if (prodVotes.length > 0) {
      console.log(`  Found ${prodVotes.length} user brand votes to migrate`);
      const votesRepo = newDataSource.getRepository(UserBrandVotes);
      const userRepo = newDataSource.getRepository(User);
      const brandRepo = newDataSource.getRepository(Brand);

      // Pre-load all users and brands to avoid repeated lookups
      console.log('  Pre-loading users and brands...');
      const allUsers = await userRepo.find();
      const allBrands = await brandRepo.find();
      const userMap = new Map(allUsers.map((user) => [user.id, user]));
      const brandMap = new Map(allBrands.map((brand) => [brand.id, brand]));
      console.log(
        `  Loaded ${allUsers.length} users and ${allBrands.length} brands`,
      );

      // Use raw SQL for much faster bulk insert
      const batchSize = 2000; // Much larger batches with raw SQL
      let processed = 0;
      let skipped = 0;

      for (let i = 0; i < prodVotes.length; i += batchSize) {
        const batch = prodVotes.slice(i, i + batchSize);
        const validVotes = [];

        for (const vote of batch) {
          processed++;

          const user = userMap.get(vote.userId || vote.user_id);
          if (user) {
            // Get brand IDs (can be null)
            const brand1Id = vote.brand1Id || vote.brand1_id;
            const brand2Id = vote.brand2Id || vote.brand2_id;
            const brand3Id = vote.brand3Id || vote.brand3_id;

            // Validate brands exist (but allow null)
            const validBrand1 =
              brand1Id && brandMap.has(brand1Id) ? brand1Id : null;
            const validBrand2 =
              brand2Id && brandMap.has(brand2Id) ? brand2Id : null;
            const validBrand3 =
              brand3Id && brandMap.has(brand3Id) ? brand3Id : null;

            // Convert UUID id to transaction hash format for primary key
            const transactionHash = uuidToTransactionHash(vote.id);

            validVotes.push({
              id: vote.id, // Keep original UUID for reference
              transactionHash: transactionHash, // Use as primary key
              userId: user.id,
              brand1Id: validBrand1,
              brand2Id: validBrand2,
              brand3Id: validBrand3,
              date: vote.date,
              shared: vote.shared || false,
              castHash: vote.castHash || vote.cast_hash,
            });
          } else {
            skipped++;
          }
        }

        if (validVotes.length > 0) {
          // Helper to format dates - preserves exact date from production DB
          // MySQL returns dates as Date objects or strings depending on configuration
          // We preserve the exact value without timezone conversion
          const formatDate = (dateVal: any) => {
            if (!dateVal) return 'NULL';

            // If it's already a MySQL datetime string, use it directly (most common case)
            if (typeof dateVal === 'string') {
              // MySQL datetime format: YYYY-MM-DD HH:MM:SS
              // Handle both space and T separator, and strip timezone if present
              const mysqlMatch = dateVal.match(
                /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/,
              );
              if (mysqlMatch) {
                return `'${mysqlMatch[1]} ${mysqlMatch[2]}'`;
              }
              // If it's a different string format, try to parse it
              const parsed = new Date(dateVal);
              if (!isNaN(parsed.getTime())) {
                // Format using the date's components (preserves the moment in time)
                // MySQL DATETIME doesn't store timezone, so we format as-is
                const year = parsed.getFullYear();
                const month = String(parsed.getMonth() + 1).padStart(2, '0');
                const day = String(parsed.getDate()).padStart(2, '0');
                const hours = String(parsed.getHours()).padStart(2, '0');
                const minutes = String(parsed.getMinutes()).padStart(2, '0');
                const seconds = String(parsed.getSeconds()).padStart(2, '0');
                return `'${year}-${month}-${day} ${hours}:${minutes}:${seconds}'`;
              }
              return 'NULL';
            }

            // If it's a Date object (mysql2 returns Date objects by default)
            if (dateVal instanceof Date) {
              // Format using the date's local time components
              // This preserves the exact date/time as stored in MySQL (which doesn't have timezone)
              const year = dateVal.getFullYear();
              const month = String(dateVal.getMonth() + 1).padStart(2, '0');
              const day = String(dateVal.getDate()).padStart(2, '0');
              const hours = String(dateVal.getHours()).padStart(2, '0');
              const minutes = String(dateVal.getMinutes()).padStart(2, '0');
              const seconds = String(dateVal.getSeconds()).padStart(2, '0');
              return `'${year}-${month}-${day} ${hours}:${minutes}:${seconds}'`;
            }

            return 'NULL';
          };

          const escapeValue = (val: any) => {
            if (val === null || val === undefined) return 'NULL';
            if (typeof val === 'number') return val.toString();
            if (typeof val === 'boolean') return val ? '1' : '0';
            return `'${String(val).replace(/'/g, "''")}'`;
          };

          // Build VALUES clause for batch insert
          // Note: transactionHash is now the primary key, so it must be first
          const values = validVotes
            .map((vote) => {
              return `(
              ${escapeValue(vote.transactionHash)},
              ${escapeValue(vote.id)},
              ${escapeValue(vote.userId)},
              ${vote.brand1Id || 'NULL'},
              ${vote.brand2Id || 'NULL'},
              ${vote.brand3Id || 'NULL'},
              ${formatDate(vote.date)},
              ${vote.shared ? '1' : '0'},
              ${escapeValue(vote.castHash)},
              NULL,
              NULL,
              NULL,
              0,
              NULL,
              NULL,
              NULL,
              NULL,
              NULL,
              NULL,
              NULL
            )`;
            })
            .join(',\n');

          const insertSQL = `
            INSERT INTO user_brand_votes (
              transactionHash, id, userId, brand1Id, brand2Id, brand3Id, date, shared, castHash,
              brndPaidWhenCreatingPodium, rewardAmount, day, shareVerified, shareVerifiedAt,
              signatureGeneratedAt, nonce, claimedAt, claimTxHash, podiumImageUrl, castedFromFid
            ) VALUES ${values}
          `;

          try {
            const queryRunner = newDataSource.createQueryRunner();
            await queryRunner.query(insertSQL);
            await queryRunner.release();

            console.log(
              `  ✓ Migrated batch ${Math.ceil((i + batchSize) / batchSize)}/${Math.ceil(prodVotes.length / batchSize)} (${processed}/${prodVotes.length} votes, ${skipped} skipped)`,
            );
          } catch (error: any) {
            console.error(
              `  ❌ Failed to migrate votes batch starting at ${i + 1}: ${error.message}`,
            );
            console.error(`  SQL: ${insertSQL.substring(0, 500)}...`);
            throw error;
          }
        } else {
          console.log(
            `  ✓ Skipped batch ${Math.ceil((i + batchSize) / batchSize)} (no valid votes)`,
          );
        }
      }

      stats.userBrandVotes = prodVotes.length - skipped;
      console.log(
        `  ✓ Migrated ${stats.userBrandVotes} user brand votes successfully (${skipped} skipped due to missing users)`,
      );
    } else {
      console.log('  ✓ No user brand votes to migrate');
    }

    // ========================================================================
    // Step 7: Calculate and Update User Calculated Fields
    // ========================================================================
    console.log('\nStep 7: Calculating and updating user calculated fields...');
    console.log('  This includes: totalPodiums, votedBrandsCount, totalVotes,');
    console.log(
      '                 lastVoteDay, lastVoteTimestamp, favoriteBrandId',
    );

    try {
      const queryRunner = newDataSource.createQueryRunner();

      // Step 7a: Calculate totalPodiums, totalVotes, lastVoteTimestamp, lastVoteDay
      console.log('  7a. Calculating vote counts and timestamps...');
      await queryRunner.query(`
        UPDATE users u
        INNER JOIN (
          SELECT 
            userId,
            COUNT(*) as totalPodiums,
            MAX(date) as lastVoteTimestamp,
            MAX(FLOOR(UNIX_TIMESTAMP(date) / 86400)) as lastVoteDay
          FROM user_brand_votes
          GROUP BY userId
        ) v ON u.id = v.userId
        SET 
          u.totalPodiums = v.totalPodiums,
          u.totalVotes = v.totalPodiums,
          u.lastVoteTimestamp = v.lastVoteTimestamp,
          u.lastVoteDay = v.lastVoteDay
      `);
      console.log(
        '    ✓ Updated totalPodiums, totalVotes, lastVoteTimestamp, lastVoteDay',
      );

      // Step 7b: Calculate votedBrandsCount (unique brands voted for)
      console.log('  7b. Calculating unique brands voted count...');
      await queryRunner.query(`
        UPDATE users u
        INNER JOIN (
          SELECT 
            userId,
            COUNT(DISTINCT brand_id) as votedBrandsCount
          FROM (
            SELECT userId, brand1Id as brand_id FROM user_brand_votes WHERE brand1Id IS NOT NULL
            UNION
            SELECT userId, brand2Id as brand_id FROM user_brand_votes WHERE brand2Id IS NOT NULL
            UNION
            SELECT userId, brand3Id as brand_id FROM user_brand_votes WHERE brand3Id IS NOT NULL
          ) unique_brands
          GROUP BY userId
        ) v ON u.id = v.userId
        SET u.votedBrandsCount = v.votedBrandsCount
      `);
      console.log('    ✓ Updated votedBrandsCount');

      // Step 7c: Calculate favoriteBrandId (weighted: 1st=3, 2nd=2, 3rd=1)
      console.log('  7c. Calculating favorite brand (weighted by position)...');
      await queryRunner.query(`
        UPDATE users u
        SET u.favoriteBrandId = (
          SELECT brand_id
          FROM (
            SELECT 
              brand_id,
              SUM(weight) as total_weight
            FROM (
              SELECT brand1Id as brand_id, 3 as weight FROM user_brand_votes WHERE userId = u.id AND brand1Id IS NOT NULL
              UNION ALL
              SELECT brand2Id as brand_id, 2 as weight FROM user_brand_votes WHERE userId = u.id AND brand2Id IS NOT NULL
              UNION ALL
              SELECT brand3Id as brand_id, 1 as weight FROM user_brand_votes WHERE userId = u.id AND brand3Id IS NOT NULL
            ) all_votes
            GROUP BY brand_id
            ORDER BY total_weight DESC, brand_id ASC
            LIMIT 1
          ) top_brand
        )
        WHERE EXISTS (
          SELECT 1 FROM user_brand_votes WHERE userId = u.id
        )
      `);
      console.log('    ✓ Updated favoriteBrandId (using simpler query)');

      // Step 7d: Calculate maxDailyStreak and dailyStreak using optimized SQL
      // Using window functions for bulk calculation (much faster than per-user loops)
      console.log(
        '  7d. Calculating daily streak and max daily streak (optimized SQL)...',
      );

      // Reset all streaks to 0 first
      await queryRunner.query(
        `UPDATE users SET dailyStreak = 0, maxDailyStreak = 0`,
      );

      // Calculate streaks using window functions in a single query
      await queryRunner.query(`
        UPDATE users u
        INNER JOIN (
          WITH vote_days AS (
            -- Get unique vote days per user (UTC dates)
            SELECT DISTINCT
              userId,
              DATE(CONVERT_TZ(date, @@session.time_zone, '+00:00')) as vote_date
            FROM user_brand_votes
          ),
          ranked_days AS (
            -- Use LAG to find gaps between consecutive days
            SELECT 
              userId,
              vote_date,
              DATEDIFF(
                vote_date, 
                LAG(vote_date) OVER (PARTITION BY userId ORDER BY vote_date)
              ) as gap
            FROM vote_days
          ),
          streak_groups AS (
            -- Create streak groups: each gap > 1 starts a new streak
            SELECT 
              userId,
              vote_date,
              SUM(CASE WHEN gap > 1 OR gap IS NULL THEN 1 ELSE 0 END) 
                OVER (PARTITION BY userId ORDER BY vote_date) as streak_id
            FROM ranked_days
          ),
          streak_lengths AS (
            -- Calculate length of each streak
            SELECT 
              userId,
              streak_id,
              COUNT(*) as length,
              MIN(vote_date) as start_date,
              MAX(vote_date) as end_date
            FROM streak_groups
            GROUP BY userId, streak_id
          ),
          max_streaks AS (
            -- Find maximum streak per user
            SELECT 
              userId,
              MAX(length) as maxDailyStreak
            FROM streak_lengths
            GROUP BY userId
          ),
          current_streaks AS (
            -- Find current streak (most recent streak if vote was today/yesterday)
            SELECT 
              s.userId,
              s.length as dailyStreak
            FROM streak_lengths s
            INNER JOIN (
              SELECT userId, MAX(end_date) as last_end
              FROM streak_lengths
              GROUP BY userId
            ) last ON s.userId = last.userId AND s.end_date = last.last_end
            INNER JOIN (
              SELECT userId, MAX(vote_date) as most_recent
              FROM vote_days
              GROUP BY userId
            ) recent ON s.userId = recent.userId
            WHERE DATEDIFF(CURDATE(), recent.most_recent) <= 1
          )
          -- Combine results: max_streaks has all users with votes, current_streaks only has active streaks
          SELECT 
            m.userId,
            m.maxDailyStreak,
            COALESCE(c.dailyStreak, 0) as dailyStreak
          FROM max_streaks m
          LEFT JOIN current_streaks c ON m.userId = c.userId
        ) streaks ON u.id = streaks.userId
        SET 
          u.maxDailyStreak = streaks.maxDailyStreak,
          u.dailyStreak = streaks.dailyStreak
      `);

      console.log(
        '    ✓ Updated dailyStreak and maxDailyStreak for all users (optimized)',
      );

      await queryRunner.release();
      console.log('  ✓ All user calculated fields updated successfully');
    } catch (error: any) {
      console.error(`  ❌ Failed to calculate user fields: ${error.message}`);
      console.error(`  Stack: ${error.stack}`);
      throw error;
    }

    // ========================================================================
    // Step 8: Migrate UserDailyActions
    // ========================================================================
    console.log('\nStep 8: Migrating UserDailyActions...');
    const [prodActions] = await prodConnection.execute<mysql.RowDataPacket[]>(
      'SELECT * FROM user_daily_actions ORDER BY id',
    );

    if (prodActions.length > 0) {
      const actionsRepo = newDataSource.getRepository(UserDailyActions);
      const userRepo = newDataSource.getRepository(User);

      for (const action of prodActions) {
        const user = await userRepo.findOne({
          where: { id: action.userId || action.user_id },
        });

        if (user) {
          const newAction = actionsRepo.create({
            id: action.id,
            user: user,
            shareFirstTime:
              action.shareFirstTime || action.share_first_time || false,
          });
          await actionsRepo.save(newAction);
        }
      }
      stats.userDailyActions = prodActions.length;
      console.log(`  ✓ Migrated ${stats.userDailyActions} user daily actions`);
    } else {
      console.log('  ✓ No user daily actions to migrate');
    }

    // ========================================================================
    // Summary
    // ========================================================================
    console.log(
      '\n==============================================================================',
    );
    console.log('Migration Summary');
    console.log(
      '==============================================================================',
    );
    console.log(`Categories:        ${stats.categories}`);
    console.log(`Tags:              ${stats.tags}`);
    console.log(`Brands:            ${stats.brands}`);
    console.log(`Users:             ${stats.users}`);
    console.log(`BrandTags:         ${stats.brandTags}`);
    console.log(`UserBrandVotes:    ${stats.userBrandVotes}`);
    console.log(`UserDailyActions:  ${stats.userDailyActions}`);
    console.log(
      '==============================================================================',
    );
    console.log(
      `Total records migrated: ${Object.values(stats).reduce((a, b) => a + b, 0)}`,
    );
    console.log('');
    console.log(
      'Note: New entities (RewardClaim) remain empty as they do not exist in production.',
    );
    console.log('');
  } catch (error: any) {
    console.error('\n❌ ERROR: Data migration failed:');
    console.error(`   ${error.message}`);
    if (error.stack) {
      console.error(`\nStack trace:\n${error.stack}`);
    }
    process.exit(1);
  } finally {
    // Close connections
    if (prodConnection) {
      await prodConnection.end();
    }
    if (newDataSource && newDataSource.isInitialized) {
      await newDataSource.destroy();
    }
    console.log('✓ Connections closed');
  }
}

// Run the migration
migrateData()
  .then(() => {
    console.log('✓ Data migration completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n❌ Data migration failed:');
    console.error(error);
    process.exit(1);
  });
