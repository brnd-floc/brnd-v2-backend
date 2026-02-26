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
  RewardClaim,
  CollectibleActivity,
} from '../../src/models';
import { getConfig } from '../../src/security/config';

config();

function hasFlag(flag: string): boolean {
  return process.argv.slice(2).includes(flag);
}

async function run(): Promise<void> {
  const allowNumericNames = hasFlag('--allow-numeric-names');
  const appConfig = getConfig();

  console.log('🧪 Category integrity audit');
  console.log(`DB host: ${appConfig.db.host}`);

  const dataSource = new DataSource({
    type: 'mysql',
    host: appConfig.db.host,
    port: appConfig.db.port,
    username: appConfig.db.username,
    password: appConfig.db.password,
    database: appConfig.db.name,
    ssl: appConfig.db.requireSSL ? { rejectUnauthorized: false } : false,
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
      RewardClaim,
      CollectibleActivity,
    ],
    synchronize: false,
    logging: false,
  });

  try {
    await dataSource.initialize();

    const numericCategories = await dataSource
      .getRepository(Category)
      .createQueryBuilder('category')
      .where("TRIM(category.name) REGEXP '^[0-9]+$'")
      .getMany();

    const brandsWithNumericCategory = await dataSource
      .getRepository(Brand)
      .createQueryBuilder('brand')
      .leftJoinAndSelect('brand.category', 'category')
      .where("TRIM(category.name) REGEXP '^[0-9]+$'")
      .orderBy('brand.id', 'ASC')
      .getMany();

    const duplicateCategoryNames = await dataSource.query(
      `
      SELECT LOWER(TRIM(name)) AS normalizedName, COUNT(*) AS total
      FROM categories
      GROUP BY LOWER(TRIM(name))
      HAVING COUNT(*) > 1
      ORDER BY total DESC, normalizedName ASC
      `,
    );

    console.log(`Numeric categories: ${numericCategories.length}`);
    if (numericCategories.length > 0) {
      for (const category of numericCategories.slice(0, 20)) {
        console.log(` - category id=${category.id} name="${category.name}"`);
      }
    }

    console.log(`Brands mapped to numeric categories: ${brandsWithNumericCategory.length}`);
    if (brandsWithNumericCategory.length > 0) {
      for (const brand of brandsWithNumericCategory.slice(0, 30)) {
        console.log(
          ` - brand id=${brand.id} onChainId=${brand.onChainId ?? 'n/a'} name="${brand.name}" category="${brand.category?.name}"`,
        );
      }
    }

    console.log(`Duplicate normalized category names: ${duplicateCategoryNames.length}`);
    if (duplicateCategoryNames.length > 0) {
      for (const row of duplicateCategoryNames.slice(0, 20)) {
        console.log(` - ${row.normalizedName}: ${row.total}`);
      }
    }

    if (!allowNumericNames && (numericCategories.length > 0 || brandsWithNumericCategory.length > 0)) {
      console.error(
        '❌ Category integrity failed: numeric category names detected. Use --allow-numeric-names to ignore.',
      );
      process.exitCode = 1;
      return;
    }

    console.log('✅ Category integrity audit completed');
  } finally {
    if (dataSource.isInitialized) {
      await dataSource.destroy();
    }
  }
}

run().catch((error) => {
  console.error('❌ audit-category-integrity failed:', error);
  process.exitCode = 1;
});
