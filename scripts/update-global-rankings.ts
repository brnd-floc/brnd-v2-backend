/**
 * Update Global Rankings Script
 *
 * This script updates the global ranking of all brands in the database
 * based on their current score. It fetches all brands, sorts them by
 * score in descending order, and updates the currentRanking property
 * to reflect their position in the global ranking.
 *
 * Usage:
 * - To test without making changes: npx ts-node scripts/update-global-rankings.ts --dry-run
 * - To actually update rankings: npx ts-node scripts/update-global-rankings.ts
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
 * Interface for brand ranking data
 */
interface BrandRankingData {
  id: number;
  name: string;
  score: number;
  currentRanking: string;
  newRanking: string;
}

/**
 * Main function to update global rankings for all brands
 */
async function updateGlobalRankings(dryRun: boolean = false) {
  console.log('🏆 Starting global brand rankings update...');
  
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

    const brandRepository = dataSource.getRepository(Brand);

    // Get all brands with their current scores and rankings
    const brands = await brandRepository
      .createQueryBuilder('brand')
      .select(['brand.id', 'brand.name', 'brand.score', 'brand.ranking'])
      .orderBy('brand.score', 'DESC')
      .getMany();

    console.log(`📊 Found ${brands.length} brands to process`);

    if (brands.length === 0) {
      console.log('ℹ️ No brands found in database');
      return;
    }

    // Calculate new rankings based on score
    const brandRankings: BrandRankingData[] = brands.map((brand, index) => ({
      id: brand.id,
      name: brand.name,
      score: brand.score,
      currentRanking: brand.ranking || '0',
      newRanking: (index + 1).toString(), // Ranking starts from 1
    }));

    // Find brands that need ranking updates
    const brandsToUpdate = brandRankings.filter(
      brand => brand.currentRanking !== brand.newRanking
    );

    console.log(`\n📈 Ranking Analysis:`);
    console.log(`  • Total brands: ${brandRankings.length}`);
    console.log(`  • Brands needing update: ${brandsToUpdate.length}`);
    console.log(`  • Brands already correct: ${brandRankings.length - brandsToUpdate.length}`);

    if (brandsToUpdate.length === 0) {
      console.log('\n✅ All brand rankings are already correct!');
      return;
    }

    // Show top 10 rankings for verification
    console.log(`\n🏆 Top 10 Brand Rankings:`);
    brandRankings.slice(0, 10).forEach((brand, index) => {
      const statusIcon = brand.currentRanking !== brand.newRanking ? '🔄' : '✅';
      console.log(`  ${statusIcon} #${brand.newRanking}: ${brand.name} (Score: ${brand.score})`);
      if (brand.currentRanking !== brand.newRanking) {
        console.log(`    Current ranking: ${brand.currentRanking} → New ranking: ${brand.newRanking}`);
      }
    });

    if (brandsToUpdate.length > 10) {
      console.log(`  ... and ${brandsToUpdate.length - Math.min(10, brandsToUpdate.length)} more brands need updates`);
    }

    // Update rankings in batches
    let updated = 0;
    let errors = 0;
    const batchSize = 50;

    if (!dryRun) {
      console.log(`\n🔄 Updating brand rankings in batches of ${batchSize}...`);

      for (let i = 0; i < brandsToUpdate.length; i += batchSize) {
        const batch = brandsToUpdate.slice(i, i + batchSize);
        
        console.log(`\n📦 Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(brandsToUpdate.length / batchSize)} (${batch.length} brands)`);

        for (const brand of batch) {
          try {
            await brandRepository.update(brand.id, {
              ranking: brand.newRanking,
            });

            console.log(`  ✅ Updated ${brand.name}: ranking ${brand.currentRanking} → ${brand.newRanking}`);
            updated++;

          } catch (error) {
            console.error(`  ❌ Error updating brand ${brand.name} (ID: ${brand.id}):`, error);
            errors++;
          }
        }

        // Progress update
        const progress = ((i + batchSize) / brandsToUpdate.length * 100).toFixed(1);
        console.log(`📈 Progress: ${Math.min(i + batchSize, brandsToUpdate.length)}/${brandsToUpdate.length} (${progress}%)`);
      }
    }

    // Final summary
    console.log('\n🎉 Global ranking update completed!');
    console.log(`📊 Summary:`);
    console.log(`  • Total brands processed: ${brandRankings.length}`);
    console.log(`  • Brands needing update: ${brandsToUpdate.length}`);
    
    if (!dryRun) {
      console.log(`  • Successfully updated: ${updated}`);
      console.log(`  • Errors: ${errors}`);
      console.log(`  • Already correct: ${brandRankings.length - brandsToUpdate.length}`);
    }

    if (brandsToUpdate.length > 0) {
      if (dryRun) {
        console.log(`\n⚡ To actually apply these ranking changes, run the script without the --dry-run flag`);
      } else {
        console.log(`\n✅ Successfully updated global rankings for ${updated} brands!`);
        if (errors > 0) {
          console.log(`⚠️ ${errors} errors occurred during the update process`);
        }
      }
    }

    // Show final top 5 for confirmation
    if (!dryRun && updated > 0) {
      console.log(`\n🏆 Final Top 5 Global Rankings:`);
      brandRankings.slice(0, 5).forEach((brand) => {
        console.log(`  #${brand.newRanking}: ${brand.name} (Score: ${brand.score})`);
      });
    }

  } catch (error) {
    console.error('❌ Fatal error during ranking update:', error);
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
  
  updateGlobalRankings(dryRun)
    .then(() => {
      console.log('✅ Script completed successfully');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Script failed:', error);
      process.exit(1);
    });
}

export { updateGlobalRankings };