import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, IsNull } from 'typeorm';
import { createPublicClient, http, parseAbi, decodeEventLog } from 'viem';
import { base } from 'viem/chains';

import { UserBrandVotes, Brand } from '../../../models';
import { getConfig } from '../../../security/config';

@Injectable()
export class DataRepairService {
  private readonly logger = new Logger(DataRepairService.name);
  private readonly publicClient;
  private readonly contractAddress;

  // Hardcoded ABI for PodiumCreated event (won't change)
  private readonly PODIUM_CREATED_ABI = parseAbi([
    'event PodiumCreated(address indexed voter, uint256 indexed fid, uint256 indexed day, uint16[3] brandIds, uint256 cost)',
  ]);

  constructor(
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,

    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
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
   * Find all votes with NULL brand associations (corrupted data)
   */
  async findCorruptedVotes(): Promise<UserBrandVotes[]> {
    this.logger.log('🔍 Searching for corrupted votes with NULL brands...');

    const corruptedVotes = await this.userBrandVotesRepository.find({
      where: [{ brand1: IsNull() }, { brand2: IsNull() }, { brand3: IsNull() }],
      relations: ['user'],
      order: { date: 'DESC' },
    });

    this.logger.log(`Found ${corruptedVotes.length} corrupted vote records`);
    return corruptedVotes;
  }

  /**
   * Query blockchain transaction and extract vote data from logs
   */
  async queryBlockchainTransaction(txHash: string): Promise<{
    brandIds: [number, number, number];
    cost: bigint;
    fid: bigint;
    voter: string;
    day: bigint;
  } | null> {
    try {
      this.logger.debug(`Querying blockchain for transaction: ${txHash}`);

      // Get transaction receipt with logs
      const receipt = await this.publicClient.getTransactionReceipt({
        hash: txHash as `0x${string}`,
      });

      // Find PodiumCreated event in logs
      for (const log of receipt.logs) {
        if (log.address.toLowerCase() === this.contractAddress.toLowerCase()) {
          try {
            const decodedLog = decodeEventLog({
              abi: this.PODIUM_CREATED_ABI,
              data: log.data,
              topics: log.topics,
            }) as { eventName: string; args: any };

            if (decodedLog.eventName === 'PodiumCreated') {
              const { voter, fid, day, brandIds, cost } = decodedLog.args;

              this.logger.debug(`Decoded PodiumCreated event:`, {
                voter,
                fid: fid.toString(),
                day: day.toString(),
                brandIds: brandIds.map((id) => Number(id)),
                cost: cost.toString(),
              });

              return {
                brandIds: [
                  Number(brandIds[0]),
                  Number(brandIds[1]),
                  Number(brandIds[2]),
                ],
                cost,
                fid,
                voter,
                day,
              };
            }
          } catch (decodeError) {
            this.logger.debug(
              `Failed to decode log for tx ${txHash}:`,
              decodeError,
            );
            continue;
          }
        }
      }

      this.logger.warn(
        `No PodiumCreated event found for transaction: ${txHash}`,
      );
      return null;
    } catch (error) {
      this.logger.error(`Error querying blockchain for tx ${txHash}:`, error);
      return null;
    }
  }

  /**
   * Repair a single corrupted vote record
   */
  async repairVoteRecord(
    voteRecord: UserBrandVotes,
    blockchainData: {
      brandIds: [number, number, number];
      cost: bigint;
      fid: bigint;
      voter: string;
      day: bigint;
    },
  ): Promise<boolean> {
    try {
      // Verify brands exist in database
      const [brand1, brand2, brand3] = await Promise.all([
        this.brandRepository.findOne({
          where: { id: blockchainData.brandIds[0] },
        }),
        this.brandRepository.findOne({
          where: { id: blockchainData.brandIds[1] },
        }),
        this.brandRepository.findOne({
          where: { id: blockchainData.brandIds[2] },
        }),
      ]);

      if (!brand1 || !brand2 || !brand3) {
        const missingBrands = [];
        if (!brand1) missingBrands.push(blockchainData.brandIds[0]);
        if (!brand2) missingBrands.push(blockchainData.brandIds[1]);
        if (!brand3) missingBrands.push(blockchainData.brandIds[2]);

        this.logger.warn(
          `Cannot repair vote ${voteRecord.transactionHash}: Missing brands ${missingBrands.join(', ')}`,
        );
        return false;
      }

      // Calculate BRND amount (cost is in wei, convert to BRND)
      const WEI_PER_BRND = BigInt(10 ** 18);
      const brndPaid = Number(blockchainData.cost / WEI_PER_BRND);

      // Update the vote record with blockchain data
      voteRecord.brand1 = brand1;
      voteRecord.brand2 = brand2;
      voteRecord.brand3 = brand3;
      voteRecord.brndPaidWhenCreatingPodium = brndPaid;
      voteRecord.day = Number(blockchainData.day);

      await this.userBrandVotesRepository.save(voteRecord);

      this.logger.log(`✅ Repaired vote record: ${voteRecord.transactionHash}`);
      return true;
    } catch (error) {
      this.logger.error(
        `Failed to repair vote ${voteRecord.transactionHash}:`,
        error,
      );
      return false;
    }
  }

  /**
   * Main repair function - fixes all corrupted votes
   */
  async repairAllCorruptedVotes(): Promise<{
    total: number;
    repaired: number;
    failed: number;
    skipped: number;
  }> {
    this.logger.log('🔧 Starting comprehensive data repair...');

    const corruptedVotes = await this.findCorruptedVotes();
    let repaired = 0;
    let failed = 0;
    let skipped = 0;

    for (let i = 0; i < corruptedVotes.length; i++) {
      const vote = corruptedVotes[i];

      process.stdout.write(
        `\r⏳ Processing vote ${i + 1}/${corruptedVotes.length}: ${vote.transactionHash.substring(0, 10)}...`,
      );

      // Skip if transaction hash looks like a placeholder (claim tx hash used as primary key)
      // These are created when claim comes before vote and should be handled differently
      if (vote.claimTxHash && vote.transactionHash === vote.claimTxHash) {
        this.logger.debug(`Skipping placeholder vote: ${vote.transactionHash}`);
        skipped++;
        continue;
      }

      const blockchainData = await this.queryBlockchainTransaction(
        vote.transactionHash,
      );

      if (blockchainData) {
        const success = await this.repairVoteRecord(vote, blockchainData);
        if (success) {
          repaired++;
        } else {
          failed++;
        }
      } else {
        this.logger.warn(
          `Could not extract data from blockchain for: ${vote.transactionHash}`,
        );
        failed++;
      }

      // Add small delay to avoid rate limiting
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    console.log(); // New line after progress indicator

    const report = {
      total: corruptedVotes.length,
      repaired,
      failed,
      skipped,
    };

    this.logger.log('📊 Data repair completed:', report);
    return report;
  }

  /**
   * Validate vote data integrity
   */
  async validateVoteIntegrity(): Promise<{
    totalVotes: number;
    validVotes: number;
    corruptedVotes: number;
    issues: string[];
  }> {
    this.logger.log('🔍 Validating vote data integrity...');

    const totalVotes = await this.userBrandVotesRepository.count();
    const corruptedVotes = await this.findCorruptedVotes();
    const validVotes = totalVotes - corruptedVotes.length;

    const issues: string[] = [];

    // Check for votes with inconsistent BRND amounts
    const votesWithZeroBrnd = await this.userBrandVotesRepository.count({
      where: { brndPaidWhenCreatingPodium: 0 },
    });

    if (votesWithZeroBrnd > 0) {
      issues.push(`${votesWithZeroBrnd} votes have zero BRND amount`);
    }

    // Check for votes with invalid brand associations
    if (corruptedVotes.length > 0) {
      issues.push(
        `${corruptedVotes.length} votes have NULL brand associations`,
      );
    }

    const report = {
      totalVotes,
      validVotes,
      corruptedVotes: corruptedVotes.length,
      issues,
    };

    this.logger.log('📋 Integrity validation completed:', report);
    return report;
  }
}
