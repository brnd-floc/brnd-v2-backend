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

function readArg(name: string): string | undefined {
  const args = process.argv.slice(2);
  const inline = args.find((arg) => arg.startsWith(`${name}=`));
  if (inline) return inline.split('=').slice(1).join('=');
  const splitIndex = args.findIndex((arg) => arg === name);
  if (splitIndex >= 0) return args[splitIndex + 1];
  return undefined;
}

function hasFlag(name: string): boolean {
  return process.argv.slice(2).includes(name);
}

async function run(): Promise<void> {
  const appConfig = getConfig();
  const categoryId = Number(readArg('--category-id') ?? 13);
  const targetName = readArg('--name') ?? 'General';
  const apply = hasFlag('--apply');

  if (!Number.isFinite(categoryId) || categoryId <= 0) {
    throw new Error('Invalid --category-id value');
  }

  if (!targetName.trim()) {
    throw new Error('Invalid --name value');
  }

  console.log('🛠️ Category Rename Fix');
  console.log(`DB host: ${appConfig.db.host}`);
  console.log(`Mode: ${apply ? 'APPLY' : 'DRY RUN'}`);
  console.log(`Target: categories.id=${categoryId} -> "${targetName}"`);

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
    const categoryRepo = dataSource.getRepository(Category);

    const existing = await categoryRepo.findOne({ where: { id: categoryId } });
    if (!existing) {
      throw new Error(`Category ${categoryId} not found`);
    }

    console.log(`Current category: id=${existing.id} name="${existing.name}"`);

    const brandsUsingCategory = await dataSource
      .getRepository(Brand)
      .createQueryBuilder('brand')
      .where('brand.categoryId = :categoryId', { categoryId })
      .select(['brand.id', 'brand.onChainId', 'brand.name'])
      .orderBy('brand.id', 'ASC')
      .getMany();

    console.log(`Brands linked to category ${categoryId}: ${brandsUsingCategory.length}`);
    for (const brand of brandsUsingCategory.slice(0, 30)) {
      console.log(
        ` - brand id=${brand.id} onChainId=${brand.onChainId ?? 'n/a'} name="${brand.name}"`,
      );
    }

    if (!apply) {
      console.log('🧪 DRY RUN complete (no DB changes applied)');
      return;
    }

    if (existing.name === targetName) {
      console.log('ℹ️ No-op: category already has target name');
      return;
    }

    await categoryRepo.update({ id: categoryId }, { name: targetName });
    const updated = await categoryRepo.findOne({ where: { id: categoryId } });
    console.log(`✅ Updated category: id=${updated?.id} name="${updated?.name}"`);
  } finally {
    if (dataSource.isInitialized) {
      await dataSource.destroy();
    }
  }
}

run().catch((error) => {
  console.error('❌ fix-category-13-name failed:', error);
  process.exitCode = 1;
});

