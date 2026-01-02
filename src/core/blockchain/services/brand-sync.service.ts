import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { createPublicClient, http } from 'viem';
import { base } from 'viem/chains';

import { Brand, UserBrandVotes } from '../../../models';
import { getConfig } from '../../../security/config';

// Hardcoded ABI for brand-related functions
const BRAND_ABI = [
  {
    inputs: [{ internalType: 'uint16', name: 'brandId', type: 'uint16' }],
    name: 'getBrand',
    outputs: [{
      components: [
        { internalType: 'uint256', name: 'fid', type: 'uint256' },
        { internalType: 'address', name: 'walletAddress', type: 'address' },
        { internalType: 'uint256', name: 'totalBrndAwarded', type: 'uint256' },
        { internalType: 'uint256', name: 'availableBrnd', type: 'uint256' },
        { internalType: 'string', name: 'handle', type: 'string' },
        { internalType: 'string', name: 'metadataHash', type: 'string' },
        { internalType: 'uint256', name: 'createdAt', type: 'uint256' }
      ],
      internalType: 'struct BRNDSeason2.Brand',
      name: '',
      type: 'tuple'
    }],
    stateMutability: 'view',
    type: 'function'
  }
];

interface ContractBrand {
  contractId: number;
  fid: bigint;
  walletAddress: string;
  totalBrndAwarded: bigint;
  availableBrnd: bigint;
  handle: string;
  metadataHash: string;
  createdAt: bigint;
}

interface BrandMapping {
  contractId: number;
  databaseId: number | null;
  handle: string;
  exists: boolean;
  needsCreate: boolean;
  needsUpdate: boolean;
}

@Injectable()
export class BrandSyncService {
  private readonly logger = new Logger(BrandSyncService.name);
  private readonly publicClient;
  private readonly contractAddress;

  constructor(
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
    
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
  ) {
    const config = getConfig();
    
    this.contractAddress = process.env.BRND_SEASON_2_ADDRESS as `0x${string}`;
    
    if (!this.contractAddress) {
      throw new Error('BRND_SEASON_2_ADDRESS environment variable not set');
    }

    this.publicClient = createPublicClient({
      chain: base,
      transport: http(config.blockchain.baseRpcUrl),
    });
  }

  /**
   * Query a single brand from the smart contract
   */
  private async getContractBrand(brandId: number): Promise<ContractBrand | null> {
    try {
      const result = await this.publicClient.readContract({
        address: this.contractAddress,
        abi: BRAND_ABI,
        functionName: 'getBrand',
        args: [brandId],
      }) as any;

      // If handle is empty, brand doesn't exist
      if (!result.handle || result.handle === '') {
        return null;
      }

      return {
        contractId: brandId,
        fid: result.fid,
        walletAddress: result.walletAddress,
        totalBrndAwarded: result.totalBrndAwarded,
        availableBrnd: result.availableBrnd,
        handle: result.handle,
        metadataHash: result.metadataHash,
        createdAt: result.createdAt,
      };
    } catch (error) {
      this.logger.debug(`Brand ${brandId} not found on contract (likely doesn't exist)`);
      return null;
    }
  }

  /**
   * Query all brands from smart contract (brute force approach)
   */
  async getAllContractBrands(maxBrandId: number = 500): Promise<ContractBrand[]> {
    this.logger.log(`🔍 Querying all brands from smart contract (1 to ${maxBrandId})...`);
    
    const contractBrands: ContractBrand[] = [];
    
    for (let i = 1; i <= maxBrandId; i++) {
      process.stdout.write(`\r⏳ Checking brand ID ${i}/${maxBrandId}...`);
      
      try {
        const brand = await this.getContractBrand(i);
        if (brand) {
          contractBrands.push(brand);
        }
        
        // Small delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 50));
      } catch (error) {
        this.logger.debug(`Error querying brand ${i}:`, error);
      }
    }

    console.log(); // New line after progress
    this.logger.log(`✅ Found ${contractBrands.length} brands on smart contract`);
    return contractBrands;
  }

  /**
   * Create brand mapping between contract and database
   */
  async createBrandMapping(contractBrands: ContractBrand[]): Promise<BrandMapping[]> {
    this.logger.log('🗺️  Creating brand mapping between contract and database...');
    
    // Get all database brands
    const databaseBrands = await this.brandRepository.find({
      select: ['id', 'name', 'channel', 'profile'],
      order: { id: 'ASC' }
    });

    this.logger.log(`Database has ${databaseBrands.length} brands`);

    const mapping: BrandMapping[] = [];

    for (const contractBrand of contractBrands) {
      // Try to find matching database brand by handle/name
      const dbBrand = databaseBrands.find(db => {
        // Try exact handle match first
        if (db.name?.toLowerCase() === contractBrand.handle.toLowerCase()) {
          return true;
        }
        
        // Try channel/profile match
        if (db.channel?.toLowerCase().includes(contractBrand.handle.toLowerCase()) ||
            db.profile?.toLowerCase().includes(contractBrand.handle.toLowerCase())) {
          return true;
        }
        
        // Try partial name match
        if (db.name?.toLowerCase().includes(contractBrand.handle.toLowerCase()) ||
            contractBrand.handle.toLowerCase().includes(db.name?.toLowerCase() || '')) {
          return true;
        }
        
        return false;
      });

      mapping.push({
        contractId: contractBrand.contractId,
        databaseId: dbBrand?.id || null,
        handle: contractBrand.handle,
        exists: !!dbBrand,
        needsCreate: !dbBrand,
        needsUpdate: !!dbBrand && dbBrand.id !== contractBrand.contractId
      });
    }

    // Log mapping summary
    const summary = {
      total: mapping.length,
      matched: mapping.filter(m => m.exists).length,
      needsCreate: mapping.filter(m => m.needsCreate).length,
      needsUpdate: mapping.filter(m => m.needsUpdate).length,
    };

    this.logger.log('📊 Brand mapping summary:', summary);

    // Log some examples of mismatched brands
    const mismatched = mapping.filter(m => m.needsUpdate).slice(0, 10);
    if (mismatched.length > 0) {
      this.logger.warn('🚨 Sample brand ID mismatches:');
      mismatched.forEach(m => {
        this.logger.warn(`  Contract ID ${m.contractId} → Database ID ${m.databaseId} (${m.handle})`);
      });
    }

    return mapping;
  }

  /**
   * Fix vote records to use correct brand IDs
   */
  async fixVoteReferences(brandMapping: BrandMapping[]): Promise<{
    processed: number;
    fixed: number;
    failed: number;
  }> {
    this.logger.log('🔧 Fixing vote references to use correct brand IDs...');
    
    // Create quick lookup map
    const contractToDbMap = new Map<number, number>();
    brandMapping.forEach(mapping => {
      if (mapping.exists && mapping.databaseId) {
        contractToDbMap.set(mapping.contractId, mapping.databaseId);
      }
    });

    // Get all votes that might need fixing
    const allVotes = await this.userBrandVotesRepository.find({
      select: ['transactionHash', 'brand1', 'brand2', 'brand3'],
      relations: ['brand1', 'brand2', 'brand3'],
    });

    this.logger.log(`Processing ${allVotes.length} votes for brand ID corrections...`);

    let processed = 0;
    let fixed = 0;
    let failed = 0;

    for (const vote of allVotes) {
      processed++;
      
      if (processed % 100 === 0) {
        process.stdout.write(`\r⏳ Processed ${processed}/${allVotes.length} votes...`);
      }

      try {
        let hasChanges = false;
        const updates: any = {};

        // Check each brand position
        if (vote.brand1) {
          const correctDbId = contractToDbMap.get(vote.brand1.id);
          if (correctDbId && correctDbId !== vote.brand1.id) {
            updates.brand1 = { id: correctDbId };
            hasChanges = true;
          }
        }

        if (vote.brand2) {
          const correctDbId = contractToDbMap.get(vote.brand2.id);
          if (correctDbId && correctDbId !== vote.brand2.id) {
            updates.brand2 = { id: correctDbId };
            hasChanges = true;
          }
        }

        if (vote.brand3) {
          const correctDbId = contractToDbMap.get(vote.brand3.id);
          if (correctDbId && correctDbId !== vote.brand3.id) {
            updates.brand3 = { id: correctDbId };
            hasChanges = true;
          }
        }

        if (hasChanges) {
          await this.userBrandVotesRepository.update(
            { transactionHash: vote.transactionHash },
            updates
          );
          fixed++;
        }

      } catch (error) {
        this.logger.error(`Failed to fix vote ${vote.transactionHash}:`, error);
        failed++;
      }
    }

    console.log(); // New line after progress
    
    const result = { processed, fixed, failed };
    this.logger.log('📊 Vote fixing completed:', result);
    return result;
  }

  /**
   * Master synchronization function
   */
  async synchronizeAllBrands(): Promise<{
    contractBrands: number;
    databaseBrands: number;
    matched: number;
    created: number;
    votesFixed: number;
    issues: string[];
  }> {
    this.logger.log('🚀 Starting complete brand synchronization...');
    
    const issues: string[] = [];
    
    // Step 1: Get all contract brands
    const contractBrands = await this.getAllContractBrands();
    
    // Step 2: Create mapping
    const brandMapping = await this.createBrandMapping(contractBrands);
    
    // Step 3: Fix vote references
    const voteResults = await this.fixVoteReferences(brandMapping);
    
    // Step 4: Get final counts
    const databaseBrandCount = await this.brandRepository.count();
    
    const summary = {
      contractBrands: contractBrands.length,
      databaseBrands: databaseBrandCount,
      matched: brandMapping.filter(m => m.exists).length,
      created: 0, // We're not creating missing brands in this version
      votesFixed: voteResults.fixed,
      issues
    };

    // Add issues for missing brands
    const missingBrands = brandMapping.filter(m => m.needsCreate);
    if (missingBrands.length > 0) {
      issues.push(`${missingBrands.length} brands exist on contract but not in database`);
    }

    const mismatchedIds = brandMapping.filter(m => m.needsUpdate);
    if (mismatchedIds.length > 0) {
      issues.push(`${mismatchedIds.length} brands have different IDs between contract and database`);
    }

    this.logger.log('🎉 Brand synchronization completed:', summary);
    return summary;
  }

  /**
   * Validate brand-vote integrity after sync
   */
  async validateBrandVoteIntegrity(): Promise<{
    totalVotes: number;
    validBrandReferences: number;
    invalidBrandReferences: number;
    nullBrandReferences: number;
  }> {
    this.logger.log('🔍 Validating brand-vote integrity...');
    
    const totalVotes = await this.userBrandVotesRepository.count();
    
    // Count votes with valid brand references
    const validVotes = await this.userBrandVotesRepository
      .createQueryBuilder('vote')
      .leftJoin('vote.brand1', 'b1')
      .leftJoin('vote.brand2', 'b2') 
      .leftJoin('vote.brand3', 'b3')
      .where('b1.id IS NOT NULL AND b2.id IS NOT NULL AND b3.id IS NOT NULL')
      .getCount();

    const nullVotes = await this.userBrandVotesRepository
      .createQueryBuilder('vote')
      .where('vote.brand1Id IS NULL OR vote.brand2Id IS NULL OR vote.brand3Id IS NULL')
      .getCount();

    const result = {
      totalVotes,
      validBrandReferences: validVotes,
      invalidBrandReferences: totalVotes - validVotes - nullVotes,
      nullBrandReferences: nullVotes,
    };

    this.logger.log('📊 Brand-vote integrity report:', result);
    return result;
  }
}