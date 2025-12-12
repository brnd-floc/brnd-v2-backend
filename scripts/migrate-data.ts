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
 * Note: New entities (AirdropScore, AirdropSnapshot, AirdropLeaf, RewardClaim)
 * will remain empty as they don't exist in production.
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
    const [prodBrands] = await prodConnection.execute<mysql.RowDataPacket[]>(
      'SELECT * FROM brands ORDER BY id',
    );

    if (prodBrands.length > 0) {
      console.log(`  Found ${prodBrands.length} brands to migrate`);

      // Get category IDs for foreign key mapping
      const categoryRepo = newDataSource.getRepository(Category);
      const allCategories = await categoryRepo.find();
      const categoryMap = new Map(allCategories.map((cat) => [cat.id, cat.id])); // Map ID to ID for validation

      // Use raw SQL for much faster bulk insert - process all at once since it's only 402 brands
      const batchSize = prodBrands.length; // Insert all brands in one go
      let processed = 0;

      // Build VALUES clause for batch insert
      const values = prodBrands
        .map((brand) => {
          processed++;

          // Escape values for SQL safety
          const escapeValue = (val: any) => {
            if (val === null || val === undefined) return 'NULL';
            if (typeof val === 'number') return val.toString();
            if (typeof val === 'boolean') return val ? '1' : '0';
            if (val instanceof Date) {
              // Format date as MySQL datetime: YYYY-MM-DD HH:MM:SS
              return `'${val.toISOString().slice(0, 19).replace('T', ' ')}'`;
            }
            if (typeof val === 'string' && val.match(/^\d{4}-\d{2}-\d{2}/)) {
              // Already a date string, just escape
              return `'${val}'`;
            }
            return `'${String(val).replace(/'/g, "''")}'`;
          };

          // Helper to format dates specifically
          const formatDate = (dateVal: any) => {
            if (!dateVal) return 'NOW()'; // Use current timestamp if null
            if (dateVal instanceof Date) {
              return `'${dateVal.toISOString().slice(0, 19).replace('T', ' ')}'`;
            }
            if (typeof dateVal === 'string') {
              // Try to parse and reformat
              const parsed = new Date(dateVal);
              if (!isNaN(parsed.getTime())) {
                return `'${parsed.toISOString().slice(0, 19).replace('T', ' ')}'`;
              }
            }
            return 'NOW()'; // Fallback to current timestamp
          };

          // Get category ID (foreign key)
          const categoryId = brand.categoryId || brand.category_id;
          const validCategoryId =
            categoryId && categoryMap.has(categoryId) ? categoryId : null;

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
          ${validCategoryId ? validCategoryId : 'NULL'},
          NULL,
          '0',
          '0',
          NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          NULL,
          0,
          ${formatDate(brand.createdAt || brand.created_at)},
          ${formatDate(brand.updatedAt || brand.updated_at)}
        )`;
        })
        .join(',\n');

      const insertSQL = `
        INSERT INTO brands (
          id, name, url, warpcastUrl, description, followerCount, imageUrl,
          profile, channel, ranking, score, stateScore, scoreWeek, stateScoreWeek,
          rankingWeek, scoreMonth, stateScoreMonth, rankingMonth, bonusPoints,
          banned, queryType, currentRanking, categoryId, walletAddress,
          totalBrndAwarded, availableBrnd, onChainCreatedAt, onChainId,
          onChainFid, onChainHandle, onChainWalletAddress, metadataHash,
          isUploadedToContract, createdAt, updatedAt
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
                // Format date as MySQL datetime: YYYY-MM-DD HH:MM:SS
                return `'${val.toISOString().slice(0, 19).replace('T', ' ')}'`;
              }
              if (typeof val === 'string' && val.match(/^\d{4}-\d{2}-\d{2}/)) {
                // Already a date string, just escape
                return `'${val}'`;
              }
              return `'${String(val).replace(/'/g, "''")}'`;
            };

            // Helper to format dates specifically
            const formatDate = (dateVal: any) => {
              if (!dateVal) return 'NOW()'; // Use current timestamp if null
              if (dateVal instanceof Date) {
                return `'${dateVal.toISOString().slice(0, 19).replace('T', ' ')}'`;
              }
              if (typeof dateVal === 'string') {
                // Try to parse and reformat
                const parsed = new Date(dateVal);
                if (!isNaN(parsed.getTime())) {
                  return `'${parsed.toISOString().slice(0, 19).replace('T', ' ')}'`;
                }
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
            createdAt, updatedAt
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
          // Helper to format dates specifically
          const formatDate = (dateVal: any) => {
            if (!dateVal) return 'NULL';
            if (dateVal instanceof Date) {
              return `'${dateVal.toISOString().slice(0, 19).replace('T', ' ')}'`;
            }
            if (typeof dateVal === 'string') {
              const parsed = new Date(dateVal);
              if (!isNaN(parsed.getTime())) {
                return `'${parsed.toISOString().slice(0, 19).replace('T', ' ')}'`;
              }
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
              0,
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
              rewardAmount, day, shareVerified, shareVerifiedAt,
              signatureGeneratedAt, nonce, claimedAt, claimTxHash
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

      // Step 7d: Calculate maxDailyStreak and dailyStreak
      // We'll use a stored procedure approach or calculate in batches
      // For now, let's use a simpler approach: calculate for each user individually
      console.log('  7d. Calculating daily streak and max daily streak...');

      // Get all users who have votes
      const usersWithVotes = await queryRunner.query(`
        SELECT DISTINCT userId as id
        FROM user_brand_votes
      `);

      let updatedCount = 0;
      const batchSize = 100;

      for (let i = 0; i < usersWithVotes.length; i += batchSize) {
        const batch = usersWithVotes.slice(i, i + batchSize);

        for (const user of batch) {
          // Get unique voting days for this user (UTC days)
          const voteDays = await queryRunner.query(
            `
            SELECT DISTINCT 
              DATE(CONVERT_TZ(date, @@session.time_zone, '+00:00')) as vote_date
            FROM user_brand_votes
            WHERE userId = ?
            ORDER BY vote_date ASC
          `,
            [user.id],
          );

          if (voteDays.length === 0) {
            await queryRunner.query(
              `
              UPDATE users 
              SET dailyStreak = 0, maxDailyStreak = 0
              WHERE id = ?
            `,
              [user.id],
            );
            continue;
          }

          // Helper to parse date string to UTC date (handles both Date objects and strings)
          const parseUTCDate = (dateValue: any): Date => {
            const date =
              dateValue instanceof Date ? dateValue : new Date(dateValue);
            // Ensure it's treated as UTC
            return new Date(
              Date.UTC(
                date.getUTCFullYear(),
                date.getUTCMonth(),
                date.getUTCDate(),
              ),
            );
          };

          // Calculate maxDailyStreak: find maximum consecutive days
          let maxStreak = 1;
          let currentStreak = 1;

          for (let j = 1; j < voteDays.length; j++) {
            const currentDate = parseUTCDate(voteDays[j].vote_date);
            const prevDate = parseUTCDate(voteDays[j - 1].vote_date);
            const daysDiff = Math.floor(
              (currentDate.getTime() - prevDate.getTime()) /
                (1000 * 60 * 60 * 24),
            );

            if (daysDiff === 1) {
              // Consecutive day
              currentStreak++;
            } else {
              // Gap found, update max and reset
              maxStreak = Math.max(maxStreak, currentStreak);
              currentStreak = 1;
            }
          }
          maxStreak = Math.max(maxStreak, currentStreak);

          // Calculate dailyStreak: consecutive days from most recent vote
          // Most recent vote day (UTC)
          const mostRecentDate = parseUTCDate(
            voteDays[voteDays.length - 1].vote_date,
          );
          const today = new Date();
          today.setUTCHours(0, 0, 0, 0);

          const daysSinceLastVote = Math.floor(
            (today.getTime() - mostRecentDate.getTime()) /
              (1000 * 60 * 60 * 24),
          );

          let dailyStreak = 0;
          if (daysSinceLastVote <= 1) {
            // Count consecutive days backwards from most recent
            dailyStreak = 1;
            let expectedDate = new Date(mostRecentDate);

            for (let j = voteDays.length - 2; j >= 0; j--) {
              const voteDate = parseUTCDate(voteDays[j].vote_date);
              expectedDate.setUTCDate(expectedDate.getUTCDate() - 1);

              if (voteDate.getTime() === expectedDate.getTime()) {
                dailyStreak++;
              } else {
                break;
              }
            }
          }

          // Update user with calculated values
          await queryRunner.query(
            `
            UPDATE users 
            SET dailyStreak = ?, maxDailyStreak = ?
            WHERE id = ?
          `,
            [dailyStreak, maxStreak, user.id],
          );

          updatedCount++;
        }

        if (
          (i + batchSize) % 1000 === 0 ||
          i + batchSize >= usersWithVotes.length
        ) {
          console.log(
            `    Processed ${Math.min(i + batchSize, usersWithVotes.length)}/${usersWithVotes.length} users...`,
          );
        }
      }

      console.log(
        `    ✓ Updated dailyStreak and maxDailyStreak for ${updatedCount} users`,
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
      'Note: New entities (AirdropScore, AirdropSnapshot, AirdropLeaf, RewardClaim)',
    );
    console.log('      remain empty as they do not exist in production.');
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
