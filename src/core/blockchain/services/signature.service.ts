import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import {
  createWalletClient,
  createPublicClient,
  http,
  encodeAbiParameters,
  verifyTypedData,
} from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { base } from 'viem/chains';

import { User, UserBrandVotes } from '../../../models';
import { getConfig } from '../../../security/config';
import { logger } from '../../../main';
import { PodiumService } from './podium.service';

@Injectable()
export class SignatureService {
  private readonly CONTRACT_ADDRESS = process.env.BRND_SEASON_2_ADDRESS;
  private readonly DOMAIN_NAME = 'BRNDSEASON2';
  private readonly DOMAIN_VERSION = '1';
  private readonly CHAIN_ID = 8453; // Base mainnet

  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    @InjectRepository(UserBrandVotes)
    private readonly userBrandVotesRepository: Repository<UserBrandVotes>,
    private readonly podiumService: PodiumService,
  ) {}

  async generateAuthorizationSignature(
    fid: number,
    walletAddress: string,
    deadline: number,
  ): Promise<string> {
    logger.log(
      `🔐 [SIGNATURE] Generating authorization signature for FID: ${fid}, Wallet: ${walletAddress}`,
    );

    const config = getConfig();

    if (!process.env.PRIVATE_KEY) {
      logger.error(
        `❌ [SIGNATURE] PRIVATE_KEY environment variable is not set`,
      );
      throw new Error('PRIVATE_KEY environment variable is not set');
    }

    logger.log(`✅ [SIGNATURE] Backend private key found`);

    const privateKey = process.env.PRIVATE_KEY.startsWith('0x')
      ? (process.env.PRIVATE_KEY as `0x${string}`)
      : (`0x${process.env.PRIVATE_KEY}` as `0x${string}`);

    const account = privateKeyToAccount(privateKey);
    logger.log(
      `🔐 [SIGNATURE] Created account from private key: ${account.address}`,
    );

    const walletClient = createWalletClient({
      account,
      chain: base,
      transport: http(config.blockchain.baseRpcUrl),
    });

    const domain = {
      name: this.DOMAIN_NAME,
      version: this.DOMAIN_VERSION,
      chainId: this.CHAIN_ID,
      verifyingContract: this.CONTRACT_ADDRESS as `0x${string}`,
    } as const;

    logger.log(`🔐 [SIGNATURE] EIP-712 domain configured:`);
    logger.log(`   - Name: ${domain.name}`);
    logger.log(`   - Version: ${domain.version}`);
    logger.log(`   - Chain ID: ${domain.chainId}`);
    logger.log(`   - Contract: ${domain.verifyingContract}`);

    const types = {
      Authorization: [
        { name: 'fid', type: 'uint256' },
        { name: 'wallet', type: 'address' },
        { name: 'deadline', type: 'uint256' },
      ],
    } as const;

    logger.log(`🔐 [SIGNATURE] Signing authorization message with:`);
    logger.log(`   - FID: ${fid}`);
    logger.log(`   - Wallet: ${walletAddress}`);
    logger.log(`   - Deadline: ${deadline}`);

    const signature = await walletClient.signTypedData({
      account,
      domain,
      types,
      primaryType: 'Authorization',
      message: {
        fid: BigInt(fid),
        wallet: walletAddress as `0x${string}`,
        deadline: BigInt(deadline),
      },
    });

    logger.log(
      `✅ [SIGNATURE] Authorization signature generated: ${signature}`,
    );

    const authData = encodeAbiParameters(
      [
        { name: 'fid', type: 'uint256' },
        { name: 'deadline', type: 'uint256' },
        { name: 'signature', type: 'bytes' },
      ],
      [BigInt(fid), BigInt(deadline), signature],
    );

    logger.log(`✅ [SIGNATURE] Auth data encoded: ${authData}`);

    return authData;
  }

  async generateLevelUpSignature(
    fid: number,
    newLevel: number,
    deadline: number,
    walletAddress: string,
  ): Promise<string> {
    logger.log(
      `📈 [SIGNATURE] Generating level up signature for FID: ${fid}, Level: ${newLevel}, Wallet: ${walletAddress}`,
    );

    if (!process.env.PRIVATE_KEY) {
      throw new Error('PRIVATE_KEY environment variable is not set');
    }

    const user = await this.userRepository.findOne({ where: { fid } });
    if (!user) {
      throw new Error('User not found');
    }

    if (!walletAddress) {
      throw new Error('Wallet address is required');
    }

    const privateKey = process.env.PRIVATE_KEY.startsWith('0x')
      ? (process.env.PRIVATE_KEY as `0x${string}`)
      : (`0x${process.env.PRIVATE_KEY}` as `0x${string}`);

    const account = privateKeyToAccount(privateKey);

    // Create a public client to read from the contract
    const publicClient = createPublicClient({
      chain: base,
      transport: http(),
    });

    // Define the fidNonces ABI fragment
    const fidNoncesAbi = [
      {
        inputs: [
          { name: 'fid', type: 'uint256' },
          { name: 'wallet', type: 'address' },
        ],
        name: 'fidNonces',
        outputs: [{ name: '', type: 'uint256' }],
        stateMutability: 'view',
        type: 'function',
      },
    ] as const;

    // Read the current nonce from the contract for this FID and wallet
    const nonce = await publicClient.readContract({
      address: this.CONTRACT_ADDRESS as `0x${string}`,
      abi: fidNoncesAbi,
      functionName: 'fidNonces',
      args: [BigInt(fid), walletAddress as `0x${string}`],
    } as any);

    logger.log(`📈 [SIGNATURE] Current nonce from contract: ${nonce}`);

    const walletClient = createWalletClient({
      account,
      chain: base,
      transport: http(),
    });

    const domain = {
      name: this.DOMAIN_NAME,
      version: this.DOMAIN_VERSION,
      chainId: this.CHAIN_ID,
      verifyingContract: this.CONTRACT_ADDRESS as `0x${string}`,
    } as const;

    const types = {
      LevelUp: [
        { name: 'fid', type: 'uint256' },
        { name: 'newLevel', type: 'uint8' },
        { name: 'nonce', type: 'uint256' },
        { name: 'deadline', type: 'uint256' },
      ],
    } as const;

    logger.log(`📈 [SIGNATURE] Signing level up message with:`);
    logger.log(`   - FID: ${fid}`);
    logger.log(`   - New Level: ${newLevel}`);
    logger.log(`   - Nonce: ${nonce}`);
    logger.log(`   - Deadline: ${deadline}`);
    logger.log(`   - Wallet: ${walletAddress}`);

    const signature = await walletClient.signTypedData({
      account,
      domain,
      types,
      primaryType: 'LevelUp',
      message: {
        fid: BigInt(fid),
        newLevel: newLevel,
        nonce: BigInt(nonce as bigint),
        deadline: BigInt(deadline),
      },
    });

    logger.log(`✅ [SIGNATURE] Level up signature generated: ${signature}`);

    return signature;
  }

  async generateRewardClaimSignature(
    recipient: string,
    fid: number,
    amount: string,
    day: number,
    castHash: string,
    deadline: number,
  ): Promise<{ signature: string; nonce: number; canClaim: boolean }> {
    if (!process.env.PRIVATE_KEY) {
      throw new Error('PRIVATE_KEY environment variable is not set');
    }

    // Validate recipient address format
    if (!recipient || !recipient.startsWith('0x') || recipient.length !== 42) {
      throw new Error('Invalid recipient address');
    }

    const user = await this.userRepository.findOne({ where: { fid } });
    if (!user) {
      throw new Error('User not found');
    }

    // Check if already claimed
    const vote = await this.userBrandVotesRepository.findOne({
      where: { user: { fid }, day },
      relations: ['user'],
    });

    if (vote?.claimedAt) {
      logger.log(
        `❌ [SIGNATURE] Reward already claimed for FID: ${fid}, Day: ${day}`,
      );
      throw new Error('Reward already claimed for this day');
    }

    const privateKey = process.env.PRIVATE_KEY.startsWith('0x')
      ? (process.env.PRIVATE_KEY as `0x${string}`)
      : (`0x${process.env.PRIVATE_KEY}` as `0x${string}`);

    const account = privateKeyToAccount(privateKey);

    const publicClient = createPublicClient({
      chain: base,
      transport: http(),
    });

    const rewardNoncesAbi = [
      {
        inputs: [{ name: 'recipient', type: 'address' }],
        name: 'rewardNonces',
        outputs: [{ name: '', type: 'uint256' }],
        stateMutability: 'view',
        type: 'function',
      },
    ] as const;

    const nonce = await publicClient.readContract({
      address: this.CONTRACT_ADDRESS as `0x${string}`,
      abi: rewardNoncesAbi,
      functionName: 'rewardNonces',
      args: [recipient as `0x${string}`],
    } as any);

    const walletClient = createWalletClient({
      account,
      chain: base,
      transport: http(),
    });

    const domain = {
      name: 'BRNDSEASON2',
      version: '1',
      chainId: 8453,
      verifyingContract: this.CONTRACT_ADDRESS as `0x${string}`,
    } as const;

    const types = {
      RewardClaim: [
        { name: 'recipient', type: 'address' },
        { name: 'amount', type: 'uint256' },
        { name: 'fid', type: 'uint256' },
        { name: 'day', type: 'uint256' },
        { name: 'castHash', type: 'string' },
        { name: 'nonce', type: 'uint256' },
        { name: 'deadline', type: 'uint256' },
      ],
    } as const;

    const nonceNumber =
      typeof nonce === 'bigint' ? Number(nonce) : Number(nonce);

    // Convert amount string to BigInt
    let amountBigInt: bigint;
    try {
      amountBigInt = BigInt(amount);
    } catch (error) {
      throw new Error(`Invalid amount format: ${amount}`);
    }

    const message = {
      recipient: recipient as `0x${string}`,
      amount: amountBigInt,
      fid: BigInt(fid),
      day: BigInt(day),
      castHash: castHash,
      nonce: BigInt(nonceNumber),
      deadline: BigInt(deadline),
    };

    const signature = await walletClient.signTypedData({
      account,
      domain,
      types,
      primaryType: 'RewardClaim',
      message,
    });

    // After generating the signature, verify it:
    const isValid = await verifyTypedData({
      address: account.address,
      domain,
      types,
      primaryType: 'RewardClaim',
      message,
      signature,
    });

    return {
      signature,
      nonce: nonceNumber,
      canClaim: true,
    };
  }

  /**
   * Generates EIP-712 signature for airdrop claim
   * Verifies wallet belongs to FID via Neynar before signing
   * UPDATED: Now uses baseAmount (whole number) instead of Wei amount
   */
  async generateAirdropClaimSignature(
    fid: number,
    walletAddress: string,
    baseAmount: number,
    merkleRoot: string,
    deadline: number,
  ): Promise<string> {
    logger.log(
      `🔐 [AIRDROP SIGNATURE] Generating airdrop claim signature for FID: ${fid}, Wallet: ${walletAddress}`,
    );

    // Validate wallet address format
    if (
      !walletAddress ||
      !walletAddress.startsWith('0x') ||
      walletAddress.length !== 42
    ) {
      throw new Error('Invalid wallet address');
    }

    // Verify wallet belongs to FID via Neynar
    const userInfo = await this.getNeynarUserInfo(fid);
    if (!userInfo?.verified_addresses?.eth_addresses) {
      throw new Error('No verified ETH addresses found for this FID');
    }

    const verifiedAddresses = userInfo.verified_addresses.eth_addresses.map(
      (addr: string) => addr.toLowerCase(),
    );
    const walletLower = walletAddress.toLowerCase();

    if (!verifiedAddresses.includes(walletLower)) {
      logger.error(
        `❌ [AIRDROP SIGNATURE] Wallet ${walletAddress} is not verified for FID ${fid}`,
      );
      throw new Error('Wallet address is not verified for this FID');
    }

    logger.log(
      `✅ [AIRDROP SIGNATURE] Wallet ${walletAddress} verified for FID ${fid}`,
    );

    if (!process.env.PRIVATE_KEY) {
      throw new Error('PRIVATE_KEY environment variable is not set');
    }

    const privateKey = process.env.PRIVATE_KEY.startsWith('0x')
      ? (process.env.PRIVATE_KEY as `0x${string}`)
      : (`0x${process.env.PRIVATE_KEY}` as `0x${string}`);

    const account = privateKeyToAccount(privateKey);

    const merkleRootBytes32 = merkleRoot as `0x${string}`;

    // Get airdrop contract address from config
    const config = getConfig();
    const airdropContractAddress = config.blockchain.airdropContractAddress;

    if (!airdropContractAddress) {
      throw new Error('AIRDROP_CONTRACT_ADDRESS not configured');
    }

    const walletClient = createWalletClient({
      account,
      chain: base,
      transport: http(),
    });

    const domain = {
      name: 'BRNDAIRDROP1',
      version: '1',
      chainId: this.CHAIN_ID,
      verifyingContract: airdropContractAddress as `0x${string}`,
    } as const;

    const types = {
      AirdropClaim: [
        { name: 'fid', type: 'uint256' },
        { name: 'wallet', type: 'address' },
        { name: 'baseAmount', type: 'uint256' },
        { name: 'merkleRoot', type: 'bytes32' },
        { name: 'deadline', type: 'uint256' },
      ],
    } as const;

    // Convert baseAmount to Wei (multiply by 1e18)

    const signature = await walletClient.signTypedData({
      account,
      domain,
      types,
      primaryType: 'AirdropClaim',
      message: {
        fid: BigInt(fid),
        wallet: walletAddress as `0x${string}`,
        baseAmount: BigInt(baseAmount),
        merkleRoot: merkleRootBytes32,
        deadline: BigInt(deadline),
      },
    });

    return signature;
  }

  /**
   * Helper method to get Neynar user info (same as in airdrop service)
   */
  private async getNeynarUserInfo(fid: number): Promise<any> {
    try {
      const apiKey = getConfig().neynar.apiKey.replace(/&$/, '');

      const response = await fetch(
        `https://api.neynar.com/v2/farcaster/user/bulk?fids=${fid}`,
        {
          headers: {
            accept: 'application/json',
            api_key: apiKey,
          },
        },
      );

      if (!response.ok) {
        throw new Error(
          `Neynar API error: ${response.status} ${response.statusText}`,
        );
      }

      const data = await response.json();
      return data?.users?.[0] || null;
    } catch (error) {
      logger.error('Error fetching Neynar user info:', error);
      return null;
    }
  }

  /**
   * Generates EIP-712 signature for claiming a podium NFT
   */
  async generateClaimPodiumSignature(
    fid: number,
    walletAddress: string,
    brandIds: [number, number, number],
    metadataURI: string,  // ← NEW PARAM
    deadline: number,
  ): Promise<string> {
    logger.log(
      `🏆 [PODIUM SIGNATURE] Generating claim signature for FID: ${fid}, Brands: [${brandIds.join(', ')}]`,
    );
  
    if (!process.env.PRIVATE_KEY) {
      throw new Error('PRIVATE_KEY environment variable is not set');
    }
  
    const nonce = await this.podiumService.getFidNonce(fid);
    logger.log(`🏆 [PODIUM SIGNATURE] Current nonce: ${nonce}`);
  
    const privateKey = process.env.PRIVATE_KEY.startsWith('0x')
      ? (process.env.PRIVATE_KEY as `0x${string}`)
      : (`0x${process.env.PRIVATE_KEY}` as `0x${string}`);
  
    const account = privateKeyToAccount(privateKey);
    console.log('🔑 [PODIUM SIGNATURE] Signer address:', account.address);
  
    const walletClient = createWalletClient({
      account,
      chain: base,
      transport: http(),
    });
  
    const PODIUM_CONTRACT_ADDRESS =
      '0x78E84851343DD61594a6588A38d1B154435B5dB2' as `0x${string}`;  // ← UPDATED
  
    const domain = {
      name: 'BRNDPodiumCollectables',
      version: '1',
      chainId: 8453,
      verifyingContract: PODIUM_CONTRACT_ADDRESS,
    } as const;
  
    // UPDATED - includes metadataURI
    const types = {
      ClaimPodium: [
        { name: 'brand1', type: 'uint16' },
        { name: 'brand2', type: 'uint16' },
        { name: 'brand3', type: 'uint16' },
        { name: 'fid', type: 'uint256' },
        { name: 'price', type: 'uint256' },
        { name: 'metadataURI', type: 'string' },  // ← NEW
        { name: 'nonce', type: 'uint256' },
        { name: 'deadline', type: 'uint256' },
      ],
    } as const;
  
    const BASE_PRICE = BigInt('1000000000000000000000000');
  
    const signature = await walletClient.signTypedData({
      account,
      domain,
      types,
      primaryType: 'ClaimPodium',
      message: {
        brand1: brandIds[0],
        brand2: brandIds[1],
        brand3: brandIds[2],
        fid: BigInt(fid),
        price: BASE_PRICE,
        metadataURI: metadataURI,  // ← NEW
        nonce: nonce,
        deadline: BigInt(deadline),
      },
    });
  
    logger.log(`✅ [PODIUM SIGNATURE] Claim signature generated: ${signature}`);
    return signature;
  }

  /**
   * Generates EIP-712 signature for buying a podium NFT
   */
  async generateBuyPodiumSignature(
    fid: number,
    walletAddress: string,
    tokenId: number,
    deadline: number,
  ): Promise<string> {
    logger.log(
      `💰 [PODIUM SIGNATURE] Generating buy signature for FID: ${fid}, TokenId: ${tokenId}`,
    );

    if (!process.env.PRIVATE_KEY) {
      throw new Error('PRIVATE_KEY environment variable is not set');
    }

    // Get nonce and podium data
    const nonce = await this.podiumService.getFidNonce(fid);
    const podiumData = await this.podiumService.getPodiumData(tokenId);
    const price = this.podiumService.calculatePrice(podiumData.claimCount);

    logger.log(
      `💰 [PODIUM SIGNATURE] Current nonce: ${nonce}, Price: ${price}`,
    );

    const privateKey = process.env.PRIVATE_KEY.startsWith('0x')
      ? (process.env.PRIVATE_KEY as `0x${string}`)
      : (`0x${process.env.PRIVATE_KEY}` as `0x${string}`);

    const account = privateKeyToAccount(privateKey);
    const config = getConfig();
    const walletClient = createWalletClient({
      account,
      chain: base,
      transport: http(config.blockchain.baseRpcUrl),
    });

    const PODIUM_CONTRACT_ADDRESS =
      '0x78e84851343dd61594a6588a38d1b154435b5db2' as `0x${string}`;

    const domain = {
      name: 'BRNDPodiumCollectables',
      version: '1',
      chainId: 8453,
      verifyingContract: PODIUM_CONTRACT_ADDRESS,
    } as const;

    const types = {
      BuyPodium: [
        { name: 'tokenId', type: 'uint256' },
        { name: 'buyerFid', type: 'uint256' },
        { name: 'price', type: 'uint256' },
        { name: 'nonce', type: 'uint256' },
        { name: 'deadline', type: 'uint256' },
      ],
    } as const;

    console.log('🔑 [PODIUM SIGNATURE] Signer address:', account.address);

    const signature = await walletClient.signTypedData({
      account,
      domain,
      types,
      primaryType: 'BuyPodium',
      message: {
        tokenId: BigInt(tokenId),
        buyerFid: BigInt(fid),
        price: price,
        nonce: nonce,
        deadline: BigInt(deadline),
      },
    });

    logger.log(`✅ [PODIUM SIGNATURE] Buy signature generated: ${signature}`);
    return signature;
  }

  /**
   * Generates EIP-712 signature for claiming fees from a podium
   */
  async generateClaimFeesSignature(
    tokenId: number,
    deadline: number,
  ): Promise<string> {
    logger.log(
      `💵 [PODIUM SIGNATURE] Generating claim fees signature for TokenId: ${tokenId}`,
    );

    if (!process.env.PRIVATE_KEY) {
      throw new Error('PRIVATE_KEY environment variable is not set');
    }

    // Get fee claim nonce and calculate fees
    const feeClaimNonce = await this.podiumService.getFeeClaimNonce(tokenId);
    const feeAmount = await this.podiumService.calculateAccumulatedFees(
      tokenId,
      feeClaimNonce,
    );

    logger.log(
      `💵 [PODIUM SIGNATURE] Fee claim nonce: ${feeClaimNonce}, Fee amount: ${feeAmount}`,
    );

    const privateKey = process.env.PRIVATE_KEY.startsWith('0x')
      ? (process.env.PRIVATE_KEY as `0x${string}`)
      : (`0x${process.env.PRIVATE_KEY}` as `0x${string}`);
    const config = getConfig();
    const account = privateKeyToAccount(privateKey);

    const walletClient = createWalletClient({
      account,
      chain: base,
      transport: http(config.blockchain.baseRpcUrl),
    });

    const PODIUM_CONTRACT_ADDRESS =
      '0x78e84851343dd61594a6588a38d1b154435b5db2' as `0x${string}`;

    const domain = {
      name: 'BRNDPodiumCollectables',
      version: '1',
      chainId: 8453,
      verifyingContract: PODIUM_CONTRACT_ADDRESS,
    } as const;

    const types = {
      ClaimFees: [
        { name: 'tokenId', type: 'uint256' },
        { name: 'feeAmount', type: 'uint256' },
        { name: 'nonce', type: 'uint256' },
        { name: 'deadline', type: 'uint256' },
      ],
    } as const;

    const signature = await walletClient.signTypedData({
      account,
      domain,
      types,
      primaryType: 'ClaimFees',
      message: {
        tokenId: BigInt(tokenId),
        feeAmount: feeAmount,
        nonce: feeClaimNonce,
        deadline: BigInt(deadline),
      },
    });

    logger.log(
      `✅ [PODIUM SIGNATURE] Claim fees signature generated: ${signature}`,
    );
    return signature;
  }
}
