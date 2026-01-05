/**
 * Fix Daily Streaks Script
 *
 * This script recalculates the daily streaks for all users based on their actual voting history.
 * It's designed to fix the issue where daily streaks were lost due to indexer downtime,
 * where votes were cast on-chain but not properly indexed in the backend database.
 *
 * The script:
 * 1. Gets all users who have votes
 * 2. For each user, recalculates their current daily streak based on consecutive voting days
 * 3. Updates both dailyStreak and maxDailyStreak fields
 * 4. Provides detailed logging and progress tracking
 *
 * Usage: 
 * - To test without making changes: npx ts-node scripts/fix-daily-streaks.ts --dry-run
 * - To actually fix streaks: npx ts-node scripts/fix-daily-streaks.ts
 */

import { config } from 'dotenv';
import { DataSource } from 'typeorm';
import { 
  User, 
  UserBrandVotes, 
  Brand, 
  Category, 
  Tag, 
  BrandTags,
  UserDailyActions,
  AirdropScore,
  AirdropSnapshot,
  AirdropLeaf,
  RewardClaim
} from '../src/models';
import { getConfig } from '../src/security/config';

// Load environment variables from .env file
config();

/**
 * Calculates the daily streak for a user based on their voting history
 * This mirrors the logic from UserService.calculateDailyStreak()
 */
function calculateDailyStreak(votes: UserBrandVotes[]): number {
  if (votes.length === 0) return 0;

  let streak = 0;
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());

  // Sort votes by date descending (most recent first)
  const sortedVotes = votes.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

  // Check if user voted today or yesterday (streak can continue)
  const mostRecentVote = new Date(sortedVotes[0].date);
  const mostRecentVoteDate = new Date(
    mostRecentVote.getFullYear(),
    mostRecentVote.getMonth(),
    mostRecentVote.getDate(),
  );

  const daysDiff = Math.floor(
    (today.getTime() - mostRecentVoteDate.getTime()) / (1000 * 60 * 60 * 24),
  );

  console.log(`  📅 Most recent vote: ${mostRecentVoteDate.toDateString()} (${daysDiff} days ago)`);

  // If more than 1 day since last vote, streak is broken
  if (daysDiff > 1) {
    console.log(`  ❌ Streak broken - more than 1 day since last vote`);
    return 0;
  }

  // Count consecutive days backwards from most recent vote
  const expectedDate = new Date(mostRecentVoteDate);
  const seenDates = new Set<string>();

  for (const vote of sortedVotes) {
    const voteDate = new Date(vote.date);
    const voteDateOnly = new Date(
      voteDate.getFullYear(),
      voteDate.getMonth(),
      voteDate.getDate(),
    );
    
    const voteDateString = voteDateOnly.toDateString();
    
    // Skip if we've already seen this date (multiple votes same day)
    if (seenDates.has(voteDateString)) {
      continue;
    }
    seenDates.add(voteDateString);

    if (voteDateOnly.getTime() === expectedDate.getTime()) {
      streak++;
      expectedDate.setDate(expectedDate.getDate() - 1);
    } else {
      // Check if this vote is earlier than expected (gap found)
      if (voteDateOnly.getTime() < expectedDate.getTime()) {
        console.log(`  ✋ Gap found at ${voteDateString} - streak stops at ${streak} consecutive days`);
        break;
      }
      // If vote is later than expected, continue looking for the expected date
    }
  }

  console.log(`  🏆 Calculated streak: ${streak} consecutive days`);
  return streak;
}

/**
 * Main function to fix daily streaks for all users
 */
async function fixDailyStreaks(dryRun: boolean = false) {
  console.log('🏁 Starting daily streak recalculation...');
  
  if (dryRun) {
    console.log('🧪 DRY RUN MODE - No database changes will be made');
  }
  
  const config = getConfig();
  
  // Initialize database connection
  const dataSource = new DataSource({
    type: 'mysql',
    host: config.db.host,
    port: config.db.port,
    username: config.db.username,
    password: config.db.password,
    database: config.db.name,
    ssl: config.db.requireSSL ? { rejectUnauthorized: false } : false,
    entities: [
      User, 
      UserBrandVotes, 
      Brand, 
      Category, 
      Tag, 
      BrandTags,
      UserDailyActions,
      AirdropScore,
      AirdropSnapshot,
      AirdropLeaf,
      RewardClaim
    ],
    synchronize: false,
    logging: false,
  });

  try {
    await dataSource.initialize();
    console.log('✅ Database connection established');

    const userRepository = dataSource.getRepository(User);
    const voteRepository = dataSource.getRepository(UserBrandVotes);

    // Get all users who have votes
    const usersWithVotes = await userRepository
      .createQueryBuilder('user')
      .innerJoin('user.userBrandVotes', 'vote')
      .select(['user.id', 'user.fid', 'user.username', 'user.dailyStreak', 'user.maxDailyStreak'])
      .groupBy('user.id')
      .orderBy('user.id', 'ASC')
      .getMany();

    console.log(`📊 Found ${usersWithVotes.length} users with votes to process`);

    let processed = 0;
    let updated = 0;
    let errors = 0;
    const batchSize = 50;

    // Process users in batches
    for (let i = 0; i < usersWithVotes.length; i += batchSize) {
      const batch = usersWithVotes.slice(i, i + batchSize);
      
      console.log(`\n🔄 Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(usersWithVotes.length / batchSize)} (users ${i + 1}-${Math.min(i + batchSize, usersWithVotes.length)})`);

      for (const user of batch) {
        try {
          console.log(`\n👤 Processing user ${user.fid} (${user.username || 'Unknown'})`);
          console.log(`  Current streak: ${user.dailyStreak}, Max streak: ${user.maxDailyStreak}`);

          // Get all votes for this user
          const userVotes = await voteRepository.find({
            where: { user: { id: user.id } },
            select: ['date', 'transactionHash'],
            order: { date: 'DESC' },
          });

          console.log(`  Found ${userVotes.length} votes`);

          if (userVotes.length === 0) {
            console.log(`  No votes found - skipping`);
            processed++;
            continue;
          }

          // Calculate the correct daily streak
          const correctStreak = calculateDailyStreak(userVotes);
          
          // Calculate correct max streak (either current max or new streak if higher)
          const correctMaxStreak = Math.max(user.maxDailyStreak || 0, correctStreak);

          // Check if update is needed
          if (user.dailyStreak !== correctStreak || (user.maxDailyStreak || 0) !== correctMaxStreak) {
            console.log(`  🔧 ${dryRun ? 'NEEDS UPDATE' : 'UPDATING'} user ${user.fid}:`);
            console.log(`    📊 Daily streak: ${user.dailyStreak} → ${correctStreak}`);
            console.log(`    🏅 Max streak: ${user.maxDailyStreak || 0} → ${correctMaxStreak}`);

            if (!dryRun) {
              await userRepository.update(user.id, {
                dailyStreak: correctStreak,
                maxDailyStreak: correctMaxStreak,
              });
            }

            updated++;
          } else {
            console.log(`  ✅ Streak already correct (${correctStreak})`);
          }

          processed++;

        } catch (error) {
          console.error(`❌ Error processing user ${user.fid}:`, error);
          errors++;
        }
      }

      // Progress update
      const progress = ((i + batchSize) / usersWithVotes.length * 100).toFixed(1);
      console.log(`\n📈 Progress: ${Math.min(i + batchSize, usersWithVotes.length)}/${usersWithVotes.length} (${progress}%)`);
    }

    // Final summary
    console.log('\n🎉 Daily streak recalculation completed!');
    console.log(`📊 Summary:`);
    console.log(`  • Total users processed: ${processed}`);
    console.log(`  • Users updated: ${updated}`);
    console.log(`  • Errors: ${errors}`);
    console.log(`  • Users with correct streaks: ${processed - updated - errors}`);

    if (updated > 0) {
      console.log(`\n✅ ${dryRun ? 'Would fix' : 'Successfully fixed'} daily streaks for ${updated} users!`);
      if (dryRun) {
        console.log(`\n⚡ To actually apply these changes, run the script without the --dry-run flag`);
      }
    } else {
      console.log(`\n✅ All user streaks were already correct!`);
    }

  } catch (error) {
    console.error('❌ Fatal error during streak recalculation:', error);
    process.exit(1);
  } finally {
    if (dataSource.isInitialized) {
      await dataSource.destroy();
      console.log('🔌 Database connection closed');
    }
  }
}

// Run the script
if (require.main === module) {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run') || args.includes('--dry') || args.includes('-d');
  
  fixDailyStreaks(dryRun)
    .then(() => {
      console.log('✅ Script completed successfully');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Script failed:', error);
      process.exit(1);
    });
}

export { fixDailyStreaks };