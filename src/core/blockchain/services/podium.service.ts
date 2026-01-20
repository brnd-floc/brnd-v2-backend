import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { createPublicClient, http, keccak256, encodeAbiParameters } from 'viem';
import { base } from 'viem/chains';
import {
  createCanvas,
  loadImage,
  GlobalFonts,
  CanvasRenderingContext2D,
} from '@napi-rs/canvas';
import * as path from 'path';
import * as fs from 'fs';

import { Brand, CollectibleActivity, User, UserBrandVotes } from '../../../models';
import { logger } from '../../../main';
import { IpfsService } from 'src/utils/ipfs.service';

// Podium Contract ABI
const PODIUM_CONTRACT_ABI = [{"inputs":[{"internalType":"address","name":"_brndToken","type":"address"},{"internalType":"address","name":"_season2","type":"address"},{"internalType":"address","name":"_backendSigner","type":"address"},{"internalType":"address","name":"_protocolFeeRecipient","type":"address"},{"internalType":"address","name":"_escrowWallet","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"AlreadyMinted","type":"error"},{"inputs":[],"name":"CannotBuyOwnPodium","type":"error"},{"inputs":[],"name":"ECDSAInvalidSignature","type":"error"},{"inputs":[{"internalType":"uint256","name":"length","type":"uint256"}],"name":"ECDSAInvalidSignatureLength","type":"error"},{"inputs":[{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"ECDSAInvalidSignatureS","type":"error"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"address","name":"owner","type":"address"}],"name":"ERC721IncorrectOwner","type":"error"},{"inputs":[{"internalType":"address","name":"operator","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ERC721InsufficientApproval","type":"error"},{"inputs":[{"internalType":"address","name":"approver","type":"address"}],"name":"ERC721InvalidApprover","type":"error"},{"inputs":[{"internalType":"address","name":"operator","type":"address"}],"name":"ERC721InvalidOperator","type":"error"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"ERC721InvalidOwner","type":"error"},{"inputs":[{"internalType":"address","name":"receiver","type":"address"}],"name":"ERC721InvalidReceiver","type":"error"},{"inputs":[{"internalType":"address","name":"sender","type":"address"}],"name":"ERC721InvalidSender","type":"error"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ERC721NonexistentToken","type":"error"},{"inputs":[],"name":"EmptyMetadataURI","type":"error"},{"inputs":[],"name":"Expired","type":"error"},{"inputs":[],"name":"InsufficientBalance","type":"error"},{"inputs":[],"name":"InvalidFid","type":"error"},{"inputs":[],"name":"InvalidInput","type":"error"},{"inputs":[],"name":"NotMinted","type":"error"},{"inputs":[],"name":"NothingToClaim","type":"error"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"OwnableInvalidOwner","type":"error"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"OwnableUnauthorizedAccount","type":"error"},{"inputs":[],"name":"ReentrancyGuardReentrantCall","type":"error"},{"inputs":[],"name":"TransferBlocked","type":"error"},{"inputs":[],"name":"Unauthorized","type":"error"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"oldSigner","type":"address"},{"indexed":true,"internalType":"address","name":"newSigner","type":"address"}],"name":"BackendSignerUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"string","name":"oldURI","type":"string"},{"indexed":false,"internalType":"string","name":"newURI","type":"string"}],"name":"ContractURIUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"oldEscrow","type":"address"},{"indexed":true,"internalType":"address","name":"newEscrow","type":"address"}],"name":"EscrowWalletUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"ownerFid","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"FeesClaimed","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"fid","type":"uint256"},{"indexed":true,"internalType":"address","name":"newWallet","type":"address"}],"name":"FidWalletUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"newOwnerFid","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"previousOwnerFid","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"sellerProceeds","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"genesisRoyalty","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"protocolFee","type":"uint256"}],"name":"PodiumBought","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"},{"indexed":true,"internalType":"bytes32","name":"arrangementHash","type":"bytes32"},{"indexed":true,"internalType":"uint256","name":"ownerFid","type":"uint256"},{"indexed":false,"internalType":"uint16[3]","name":"brandIds","type":"uint16[3]"},{"indexed":false,"internalType":"uint256","name":"price","type":"uint256"},{"indexed":false,"internalType":"address","name":"wallet","type":"address"},{"indexed":false,"internalType":"string","name":"metadataURI","type":"string"}],"name":"PodiumMinted","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"oldRecipient","type":"address"},{"indexed":true,"internalType":"address","name":"newRecipient","type":"address"}],"name":"ProtocolFeeRecipientUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"BASE_PRICE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"BPS_DENOMINATOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"BRND_TOKEN","outputs":[{"internalType":"contract IBRND","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"GENESIS_ROYALTY_BPS","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MULTIPLIER_DENOMINATOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PRICE_MULTIPLIER","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PROTOCOL_FEE_BPS","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"SEASON2","outputs":[{"internalType":"contract IBRNDSeason2","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"arrangementToTokenId","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"backendSigner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"buyerFid","type":"uint256"}],"name":"buyPodium","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint16[3]","name":"brandIds","type":"uint16[3]"},{"internalType":"uint256","name":"fid","type":"uint256"},{"internalType":"string","name":"metadataURI","type":"string"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bytes","name":"signature","type":"bytes"}],"name":"claimPodium","outputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"uint256","name":"feeAmount","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bytes","name":"signature","type":"bytes"}],"name":"claimRepeatFees","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"contractURI","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"escrowWallet","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"feeClaimNonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"fidNonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"fidWallet","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint16[3]","name":"brandIds","type":"uint16[3]"}],"name":"getArrangementHash","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"getDomainSeparator","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getFeeClaimNonce","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"fid","type":"uint256"}],"name":"getNonce","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getPodium","outputs":[{"components":[{"internalType":"uint16[3]","name":"brandIds","type":"uint16[3]"},{"internalType":"uint256","name":"genesisCreatorFid","type":"uint256"},{"internalType":"uint256","name":"ownerFid","type":"uint256"},{"internalType":"uint256","name":"claimCount","type":"uint256"},{"internalType":"uint256","name":"lastSalePrice","type":"uint256"},{"internalType":"uint256","name":"totalFeesEarned","type":"uint256"},{"internalType":"uint256","name":"createdAt","type":"uint256"}],"internalType":"struct BRNDPodiumCollectables.PodiumData","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getPriceByTokenId","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"podiumData","outputs":[{"internalType":"uint256","name":"genesisCreatorFid","type":"uint256"},{"internalType":"uint256","name":"ownerFid","type":"uint256"},{"internalType":"uint256","name":"claimCount","type":"uint256"},{"internalType":"uint256","name":"lastSalePrice","type":"uint256"},{"internalType":"uint256","name":"totalFeesEarned","type":"uint256"},{"internalType":"uint256","name":"createdAt","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"protocolFeeRecipient","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"operator","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newSigner","type":"address"}],"name":"setBackendSigner","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"newContractURI","type":"string"}],"name":"setContractURI","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newEscrow","type":"address"}],"name":"setEscrowWallet","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newRecipient","type":"address"}],"name":"setProtocolFeeRecipient","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalMinted","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}] as const;

@Injectable()
export class PodiumService implements OnModuleInit {
  private readonly serviceLogger = new Logger(PodiumService.name);

  private readonly PODIUM_CONTRACT_ADDRESS =
    '0x78e84851343dd61594a6588a38d1b154435b5db2' as `0x${string}`;
  private readonly BASE_PRICE = BigInt('1000000000000000000000000'); // 1,000,000 BRND in wei
  private readonly PRICE_INCREMENT = BigInt('1000000000000000000000000'); // 1,000,000 BRND in wei
  private readonly REPEAT_FEE_BPS = 1000; // 10%

  private readonly publicClient;

  // NFT Image generation config
  private readonly nftBaseLayerPath = path.join(
    process.cwd(),
    'assets',
    'nft_podium_base_layer.png',
  );

  private nftBaseLayerCache: {
    image: Awaited<ReturnType<typeof loadImage>>;
    width: number;
    height: number;
  } | null = null;

  private readonly CONFIG = {
    colors: {
      textWhite: '#FFFFFF',
      textGray: '#CCCCCC',
      background: '#000000',
    },
    fonts: {
      primary: 'Geist-Bold',
      fallback: 'Arial',
    },
    // Positions based on the NFT base layer image (1200x1200)
    nftInfo: {
      x: 1460, // Right-aligned
      y: 67,   // Top area
    },
    // Brand slots on podium [rank2-left, rank1-center, rank3-right]
    slots: [
      { rank: 2, centerX: 266, y: 460, size: 413 }, // Left podium (2nd place)
      { rank: 1, centerX: 750, y: 310, size: 413 }, // Center podium (1st place)
      { rank: 3, centerX: 1234, y: 611, size: 413 }, // Right podium (3rd place)
    ],
    // Brand names at bottom
    brandNames: {
      y: 1340, // Y position for brand names
      fontSize: 47.61,
    },
  };

  constructor(
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
    @InjectRepository(CollectibleActivity)
    private readonly collectibleActivityRepository: Repository<CollectibleActivity>,
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    @InjectRepository(Brand)
    private readonly brandRepository: Repository<Brand>,
    private readonly ipfsService: IpfsService,
  ) {
    this.publicClient = createPublicClient({
      chain: base,
      transport: http(),
    });
  }

  onModuleInit() {
    try {
      const fontPath = path.join(
        process.cwd(),
        'assets',
        'fonts',
        'Geist-Bold.ttf',
      );
      if (fs.existsSync(fontPath)) {
        GlobalFonts.registerFromPath(fontPath, 'Geist-Bold');
        this.serviceLogger.log('Custom font Geist-Bold registered successfully.');
      } else {
        this.serviceLogger.warn(`Custom font not found at ${fontPath}.`);
      }
    } catch (e) {
      this.serviceLogger.warn('Failed to register custom font', e as any);
    }
  }

  // ============================================================================
  //                    COLLECTIBLE EVENT HANDLERS (NEW)
  // ============================================================================

  /**
   * Handle PodiumMinted event from indexer
   * Updates all votes with matching brand combination to mark them as collectibles
   */
  async handleCollectibleMinted(data: {
    tokenId: number;
    brandIds: [number, number, number];
    ownerFid: number;
    ownerWallet: string;
    price: string;
    txHash: string;
    metadataURI: string;
  }): Promise<{ affected: number }> {
    try {
      logger.log(
        `🏆 [COLLECTIBLE] Processing mint for Token #${data.tokenId}, Brands: [${data.brandIds.join(', ')}]`,
      );
      logger.log(`🏆 [COLLECTIBLE] Metadata URI: ${data.metadataURI}`);

      const user = await this.userRepository.findOne({
        where: {
          fid: data.ownerFid,
        },
      });

      const result = await this.userBrandVotesRepository
        .createQueryBuilder()
        .update(UserBrandVotes)
        .set({
          isCollectible: true,
          collectibleTokenId: data.tokenId,
          collectibleOwnerFid: data.ownerFid,
          collectibleOwnerWallet: data.ownerWallet,
          collectibleOwner: user || null,
          collectiblePrice: data.price,
          collectibleMintTxHash: data.txHash,
          collectibleGenesisCreatorFid: data.ownerFid,
          collectibleClaimCount: 1,
          collectibleMetadataURI: data.metadataURI,
        })
        .where('brand1Id = :b1 AND brand2Id = :b2 AND brand3Id = :b3', {
          b1: data.brandIds[0],
          b2: data.brandIds[1],
          b3: data.brandIds[2],
        })
        .execute();

      logger.log(
        `✅ [COLLECTIBLE] Mint processed - Token #${data.tokenId} - Updated ${result.affected} votes`,
      );

      return { affected: result.affected || 0 };
    } catch (error) {
      logger.error(
        `❌ [COLLECTIBLE] Failed to process mint for Token #${data.tokenId}:`,
        error,
      );
      throw error;
    }
  }

  /**
   * Handle PodiumBought event from indexer
   * Updates all votes with matching tokenId to reflect new ownership
   */
  async handleCollectibleBought(data: {
    tokenId: number;
    newOwnerFid: number;
    newOwnerWallet: string;
    price: string;
    claimCount: number;
    totalFeesEarned?: string; // Add this
  }): Promise<{ affected: number }> {
    const result = await this.userBrandVotesRepository
      .createQueryBuilder()
      .update(UserBrandVotes)
      .set({
        collectibleOwnerFid: data.newOwnerFid,
        collectibleOwnerWallet: data.newOwnerWallet,
        collectiblePrice: data.price,
        collectibleClaimCount: data.claimCount,
        collectibleTotalFeesEarned: data.totalFeesEarned || '0', // Add this
      })
      .where('collectibleTokenId = :tokenId', { tokenId: data.tokenId })
      .execute();

    return { affected: result.affected || 0 };
  }

  /**
   * Get collectible info for a specific vote/podium arrangement
   */
  async getCollectibleInfo(brandIds: [number, number, number]): Promise<{
    isCollectible: boolean;
    tokenId: number | null;
    ownerFid: number | null;
    ownerWallet: string | null;
    price: string | null;
    genesisCreatorFid: number | null;
    claimCount: number | null;
  }> {
    try {
      const vote = await this.userBrandVotesRepository
        .createQueryBuilder('vote')
        .where(
          'vote.brand1Id = :b1 AND vote.brand2Id = :b2 AND vote.brand3Id = :b3',
          {
            b1: brandIds[0],
            b2: brandIds[1],
            b3: brandIds[2],
          },
        )
        .andWhere('vote.isCollectible = :isCollectible', {
          isCollectible: true,
        })
        .getOne();

      if (!vote) {
        return {
          isCollectible: false,
          tokenId: null,
          ownerFid: null,
          ownerWallet: null,
          price: null,
          genesisCreatorFid: null,
          claimCount: null,
        };
      }

      return {
        isCollectible: true,
        tokenId: vote.collectibleTokenId,
        ownerFid: vote.collectibleOwnerFid,
        ownerWallet: vote.collectibleOwnerWallet,
        price: vote.collectiblePrice,
        genesisCreatorFid: vote.collectibleGenesisCreatorFid,
        claimCount: vote.collectibleClaimCount,
      };
    } catch (error) {
      logger.error('Error getting collectible info:', error);
      throw error;
    }
  }

  // ============================================================================
  //                    EXISTING METHODS (UNCHANGED)
  // ============================================================================

  /**
   * Calculate arrangement hash from brand IDs
   */
  private calculateArrangementHash(
    brandIds: [number, number, number],
  ): `0x${string}` {
    const encoded = encodeAbiParameters(
      [
        { name: 'brand1', type: 'uint16' },
        { name: 'brand2', type: 'uint16' },
        { name: 'brand3', type: 'uint16' },
      ],
      [brandIds[0], brandIds[1], brandIds[2]],
    );
    return keccak256(encoded);
  }

  /**
   * Check if an arrangement is already minted
   */
  async isArrangementMinted(
    brandIds: [number, number, number],
  ): Promise<boolean> {
    try {
      const arrangementHash = this.calculateArrangementHash(brandIds);
      const tokenId = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'arrangementToTokenId',
        args: [arrangementHash],
      });

      return (tokenId as bigint) !== BigInt(0);
    } catch (error) {
      logger.error('Error checking if arrangement is minted:', error);
      throw new Error('Failed to check arrangement mint status');
    }
  }

  /**
   * Get FID nonce from contract
   */
  async getFidNonce(fid: number): Promise<bigint> {
    try {
      const nonce = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'fidNonces',
        args: [BigInt(fid)],
      });

      return nonce as bigint;
    } catch (error) {
      logger.error(`Error getting nonce for FID ${fid}:`, error);
      throw new Error('Failed to get FID nonce from contract');
    }
  }

  /**
   * Get fee claim nonce for a token
   */
  async getFeeClaimNonce(tokenId: string | number): Promise<bigint> {
    try {
      const tokenIdBigInt =
        typeof tokenId === 'string' ? BigInt(tokenId) : BigInt(tokenId);
      const nonce = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'feeClaimNonces',
        args: [tokenIdBigInt],
      });

      return nonce as bigint;
    } catch (error) {
      logger.error(
        `Error getting fee claim nonce for token ${tokenId}:`,
        error,
      );
      throw new Error('Failed to get fee claim nonce from contract');
    }
  }

  /**
   * Get podium data from contract
   */
  async getPodiumData(tokenId: string | number): Promise<{
    ownerFid: bigint;
    claimCount: bigint;
  }> {
    try {
      const tokenIdBigInt =
        typeof tokenId === 'string' ? BigInt(tokenId) : BigInt(tokenId);
      const data = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'podiumData',
        args: [tokenIdBigInt],
      });

      const result = data as readonly [
        bigint,
        bigint,
        bigint,
        bigint,
        bigint,
        bigint,
      ];
      return {
        ownerFid: result[1],
        claimCount: result[2],
      };
    } catch (error) {
      logger.error(`Error getting podium data for token ${tokenId}:`, error);
      throw new Error('Failed to get podium data from contract');
    }
  }

  /**
   * Get current price for an arrangement
   */
  async getCurrentPrice(brandIds: [number, number, number]): Promise<bigint> {
    try {
      const arrangementHash = this.calculateArrangementHash(brandIds);
      const price = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'getCurrentPrice',
        args: [arrangementHash],
      });

      return price as bigint;
    } catch (error) {
      logger.error('Error getting current price:', error);
      const isMinted = await this.isArrangementMinted(brandIds);
      if (!isMinted) {
        return this.BASE_PRICE;
      }
      const arrangementHash = this.calculateArrangementHash(brandIds);
      const tokenId = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'arrangementToTokenId',
        args: [arrangementHash],
      });
      const podiumData = await this.getPodiumData(Number(tokenId as bigint));
      return this.BASE_PRICE + podiumData.claimCount * this.PRICE_INCREMENT;
    }
  }

  /**
   * Calculate price from claim count
   */
  calculatePrice(claimCount: bigint): bigint {
    return this.BASE_PRICE + claimCount * this.PRICE_INCREMENT;
  }

  /**
   * Check if user is eligible to claim a podium
   */
  async checkClaimEligibility(
    fid: number,
    brandIds: [number, number, number],
  ): Promise<{ eligible: boolean; reason: string | null }> {
    try {
      const isMinted = await this.isArrangementMinted(brandIds);
      if (isMinted) {
        return {
          eligible: false,
          reason: 'This podium arrangement has already been minted',
        };
      }

      const votes = await this.userBrandVotesRepository
        .createQueryBuilder('vote')
        .leftJoinAndSelect('vote.user', 'user')
        .leftJoinAndSelect('vote.brand1', 'brand1')
        .leftJoinAndSelect('vote.brand2', 'brand2')
        .leftJoinAndSelect('vote.brand3', 'brand3')
        .where('brand1.id = :brand1', { brand1: brandIds[0] })
        .andWhere('brand2.id = :brand2', { brand2: brandIds[1] })
        .andWhere('brand3.id = :brand3', { brand3: brandIds[2] })
        .orderBy('vote.date', 'DESC')
        .getMany();

      if (votes.length === 0) {
        return {
          eligible: false,
          reason: 'No votes found for this podium arrangement',
        };
      }

      const lastVote = votes[0];
      const userVote = votes.find((vote) => vote.user.fid === fid);

      if (!userVote) {
        return {
          eligible: false,
          reason: 'You have not created or voted this podium arrangement',
        };
      }

      if (lastVote.user.fid !== fid) {
        return {
          eligible: false,
          reason: 'Someone else has voted this arrangement after you',
        };
      }

      return { eligible: true, reason: null };
    } catch (error) {
      logger.error('Error checking claim eligibility:', error);
      return {
        eligible: false,
        reason: 'Error checking eligibility',
      };
    }
  }

  /**
   * Get full podium data including brandIds
   */
  async getPodium(tokenId: string | number): Promise<{
    brandIds: [number, number, number];
    genesisCreatorFid: bigint;
    ownerFid: bigint;
    claimCount: bigint;
    currentPrice: bigint;
    totalFeesEarned: bigint;
    createdAt: bigint;
  }> {
    try {
      const tokenIdBigInt =
        typeof tokenId === 'string' ? BigInt(tokenId) : BigInt(tokenId);
      const data = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'getPodium',
        args: [tokenIdBigInt],
      });

      const result = data as readonly [
        readonly [bigint, bigint, bigint],
        bigint,
        bigint,
        bigint,
        bigint,
        bigint,
        bigint,
      ];
      return {
        brandIds: [
          Number(result[0][0]),
          Number(result[0][1]),
          Number(result[0][2]),
        ] as [number, number, number],
        genesisCreatorFid: result[1],
        ownerFid: result[2],
        claimCount: result[3],
        currentPrice: result[4],
        totalFeesEarned: result[5],
        createdAt: result[6],
      };
    } catch (error) {
      logger.error(`Error getting podium for token ${tokenId}:`, error);
      throw new Error('Failed to get podium data from contract');
    }
  }

  async recordActivity(data: {
    tokenId: number;
    eventType: string;
    price?: string;
    fromFid: number;
    toFid?: number;
    fromWallet: string;
    toWallet?: string;
    txHash: string;
    timestamp: number;
  }) {
    const activity = this.collectibleActivityRepository.create(data);
    return this.collectibleActivityRepository.save(activity);
  }

  async getActivity(tokenId: number) {
    const activities = await this.collectibleActivityRepository.find({
      where: { tokenId },
      relations: ['fromUser', 'toUser'],
      order: { timestamp: 'DESC' },
      take: 20,
    });

    // Get podium info from any vote with this tokenId
    const podiumInfo = await this.userBrandVotesRepository.findOne({
      where: { collectibleTokenId: tokenId },
      relations: ['brand1', 'brand2', 'brand3'],
    });

    return { activities, podiumInfo };
  }
  /**
   * Calculate accumulated fees for a podium
   */
  async calculateAccumulatedFees(
    tokenId: string | number,
    feeClaimNonce: bigint,
  ): Promise<bigint> {
    try {
      const podium = await this.getPodium(tokenId);
      const brandIds = podium.brandIds;

      const votes = await this.userBrandVotesRepository
        .createQueryBuilder('vote')
        .leftJoinAndSelect('vote.brand1', 'brand1')
        .leftJoinAndSelect('vote.brand2', 'brand2')
        .leftJoinAndSelect('vote.brand3', 'brand3')
        .where('brand1.id = :brand1', { brand1: brandIds[0] })
        .andWhere('brand2.id = :brand2', { brand2: brandIds[1] })
        .andWhere('brand3.id = :brand3', { brand3: brandIds[2] })
        .orderBy('vote.date', 'ASC')
        .getMany();

      let totalFees = BigInt(0);
      for (const vote of votes) {
        if (vote.brndPaidWhenCreatingPodium) {
          const voteCost = BigInt(vote.brndPaidWhenCreatingPodium);
          const fee = (voteCost * BigInt(this.REPEAT_FEE_BPS)) / BigInt(10000);
          totalFees += fee;
        }
      }

      return totalFees;
    } catch (error) {
      logger.error(`Error calculating fees for token ${tokenId}:`, error);
      throw new Error('Failed to calculate accumulated fees');
    }
  }

  // ============================================================================
  //                    NFT IMAGE GENERATION
  // ============================================================================

  /**
   * Get brand names for the given brand IDs
   */
  async getBrandNames(brandIds: [number, number, number]): Promise<[string, string, string]> {
    const brands = await this.brandRepository.findByIds(brandIds);
    const brandMap = new Map(brands.map(b => [b.id, b.name]));
    return [
      brandMap.get(brandIds[0]) || 'Unknown',
      brandMap.get(brandIds[1]) || 'Unknown',
      brandMap.get(brandIds[2]) || 'Unknown',
    ];
  }

  /**
   * Get brands with full data for the given brand IDs
   */
  async getBrandsForNFT(brandIds: [number, number, number]): Promise<Brand[]> {
    const brands = await this.brandRepository.findByIds(brandIds);
    // Sort to match the order of brandIds
    return brandIds.map(id => brands.find(b => b.id === id)).filter(Boolean) as Brand[];
  }

  /**
   * Get the next token ID from the contract (totalMinted + 1)
   */
  async getNextTokenId(): Promise<number> {
    try {
      const totalMinted = await this.publicClient.readContract({
        address: this.PODIUM_CONTRACT_ADDRESS,
        abi: PODIUM_CONTRACT_ABI,
        functionName: 'totalMinted',
      });
      return Number(totalMinted as bigint) + 1;
    } catch (error) {
      logger.error('Error getting next token ID:', error);
      // Fallback: return 1 if contract call fails
      return 1;
    }
  }

  /**
   * Load the NFT base layer image
   */
  private async getNFTBaseLayer() {
    if (this.nftBaseLayerCache) return this.nftBaseLayerCache;

    if (!fs.existsSync(this.nftBaseLayerPath)) {
      throw new Error(`NFT base layer image not found at: ${this.nftBaseLayerPath}`);
    }

    const image = await loadImage(this.nftBaseLayerPath);
    this.nftBaseLayerCache = { image, width: image.width, height: image.height };

    this.serviceLogger.log(
      `NFT base layer loaded: ${image.width}x${image.height}`,
    );

    return this.nftBaseLayerCache;
  }

  /**
   * Generate the NFT podium image
   */
  async generatePodiumNFTImage(brandIds: [number, number, number]): Promise<Buffer> {
    this.serviceLogger.log(`🎨 Generating NFT image for brands: [${brandIds.join(', ')}]`);

    try {
      // Load base layer
      const { image: baseImage, width, height } = await this.getNFTBaseLayer();

      // Get brand data
      const brands = await this.getBrandsForNFT(brandIds);
      if (brands.length !== 3) {
        throw new Error('Could not find all 3 brands');
      }

      // Get next token ID from contract
      const nextTokenId = await this.getNextTokenId();

      // Create canvas
      const canvas = createCanvas(width, height);
      const ctx = canvas.getContext('2d');

      // Draw black background
      ctx.fillStyle = this.CONFIG.colors.background;
      ctx.fillRect(0, 0, width, height);

      // Draw base layer
      ctx.drawImage(baseImage, 0, 0, width, height);

      // Draw NFT info in top right (NFT #{tokenId} and brand IDs)
      await this.drawNFTInfo(ctx, nextTokenId, brandIds);

      // Draw brand images on podium slots
      // brands[0] = 1st place (gold), brands[1] = 2nd place (silver), brands[2] = 3rd place (bronze)
      await this.drawBrandOnSlot(ctx, brands[0], 1); // Gold - center
      await this.drawBrandOnSlot(ctx, brands[1], 2); // Silver - left
      await this.drawBrandOnSlot(ctx, brands[2], 3); // Bronze - right

      // Draw brand names at bottom
      await this.drawBrandNames(ctx, brands, width);

      // Encode to PNG buffer
      const buffer = canvas.encode('png');
      this.serviceLogger.log(`✅ NFT image generated successfully`);
      return buffer;
    } catch (error) {
      this.serviceLogger.error('Error generating NFT image:', error as any);
      throw error;
    }
  }

  /**
   * Draw NFT info in the top right corner
   */
  private async drawNFTInfo(
    ctx: CanvasRenderingContext2D,
    tokenId: number,
    brandIds: [number, number, number],
  ) {
    const { nftInfo, colors, fonts } = this.CONFIG;

    ctx.textAlign = 'right';
    ctx.textBaseline = 'top';

    // Draw "NFT #X"
    ctx.font = `bold 27px ${fonts.primary}, ${fonts.fallback}`;
    ctx.fillStyle = colors.textWhite;
    ctx.fillText(`#${tokenId}`, nftInfo.x, nftInfo.y);

    // Draw brand IDs below (e.g., "34-55-201")
    ctx.font = `27px ${fonts.primary}, ${fonts.fallback}`;
    ctx.fillStyle = colors.textWhite;
    ctx.fillText(`${brandIds[0]}-${brandIds[1]}-${brandIds[2]}`, nftInfo.x, nftInfo.y + 32);
  }

  /**
   * Draw a brand image on its podium slot
   */
  private async drawBrandOnSlot(
    ctx: CanvasRenderingContext2D,
    brand: Brand,
    rank: 1 | 2 | 3,
  ) {
    const slot = this.CONFIG.slots.find((s) => s.rank === rank);
    if (!slot) return;

    const x = slot.centerX - slot.size / 2;

    try {
      if (brand.imageUrl) {
        await this.drawRoundedImage(ctx, brand.imageUrl, x, slot.y, slot.size, 30);
      }
    } catch (error) {
      this.serviceLogger.warn(`Failed to load brand image for ${brand.name}:`, error as any);
      // Draw placeholder if image fails
      ctx.fillStyle = '#333333';
      ctx.beginPath();
      ctx.roundRect(x, slot.y, slot.size, slot.size, 30);
      ctx.fill();
    }
  }

  /**
   * Draw brand names at the bottom of the image
   */
  private async drawBrandNames(
    ctx: CanvasRenderingContext2D,
    brands: Brand[],
    canvasWidth: number,
  ) {
    const { brandNames, colors, fonts, slots } = this.CONFIG;

    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    ctx.font = `bold ${brandNames.fontSize}px ${fonts.primary}, ${fonts.fallback}`;
    ctx.fillStyle = colors.textWhite;

    // Draw each brand name at its slot's centerX position
    // Order: brands[0]=1st(center), brands[1]=2nd(left), brands[2]=3rd(right)
    const brandSlotMapping = [
      { brand: brands[1], rank: 2 }, // 2nd place - left
      { brand: brands[0], rank: 1 }, // 1st place - center
      { brand: brands[2], rank: 3 }, // 3rd place - right
    ];

    for (const { brand, rank } of brandSlotMapping) {
      const slot = slots.find((s) => s.rank === rank);
      if (slot && brand) {
        ctx.fillText(brand.name, slot.centerX, brandNames.y);
      }
    }
  }

  /**
   * Draw a rounded image
   */
  private async drawRoundedImage(
    ctx: CanvasRenderingContext2D,
    url: string,
    x: number,
    y: number,
    size: number,
    radius: number,
  ) {
    try {
      const img = await loadImage(url);
      ctx.save();
      ctx.beginPath();
      ctx.roundRect(x, y, size, size, radius);
      ctx.clip();
      (ctx as any).drawImage(img, x, y, size, size);
      ctx.restore();
    } catch (error) {
      this.serviceLogger.warn(`Failed to load image from ${url}:`, error as any);
      throw error;
    }
  }

  /**
   * Generate NFT image and upload to IPFS
   * Returns the IPFS URI (ipfs://...)
   */
  async generateAndUploadPodiumImage(brandIds: [number, number, number]): Promise<string> {
    this.serviceLogger.log(`🚀 Generating and uploading NFT image for brands: [${brandIds.join(', ')}]`);

    // Generate the image
    const imageBuffer = await this.generatePodiumNFTImage(brandIds);

    // Upload to IPFS
    const fileName = `podium_${brandIds.join('_')}_${Date.now()}.png`;
    const imageUri = await this.ipfsService.uploadFileToIpfs(imageBuffer, fileName, 'image/png');

    this.serviceLogger.log(`✅ NFT image uploaded to IPFS: ${imageUri}`);
    return imageUri;
  }

  /**
   * Generate a test NFT image from a random existing vote
   * Used for testing the image generation flow
   */
  async generateTestNFTImage(): Promise<{
    buffer: Buffer;
    brandIds: [number, number, number];
    brandNames: [string, string, string];
    nextTokenId: number;
  }> {
    // Get a random recent vote
    const recentVotes = await this.userBrandVotesRepository.find({
      take: 50,
      order: { date: 'DESC' },
      relations: ['brand1', 'brand2', 'brand3'],
    });

    if (!recentVotes.length) {
      throw new Error('No votes found to generate test image');
    }

    const randomVote = recentVotes[Math.floor(Math.random() * recentVotes.length)];
    const brandIds: [number, number, number] = [
      randomVote.brand1.id,
      randomVote.brand2.id,
      randomVote.brand3.id,
    ];

    const buffer = await this.generatePodiumNFTImage(brandIds);
    const brandNames = await this.getBrandNames(brandIds);
    const nextTokenId = await this.getNextTokenId();

    return {
      buffer,
      brandIds,
      brandNames,
      nextTokenId,
    };
  }
}
