/**
 * @file This file defines the User entity with its properties and methods.
 */
import { Entity, Column, PrimaryColumn, ManyToOne, JoinColumn } from 'typeorm';
import { User } from '../User/User.model';

/**
 * @class UserBrandVotes
 * @classdesc UserBrandVotes class represents the votes of the users for each brands in the system.
 */
@Entity({ name: 'user_brand_votes' })
export class UserBrandVotes {
  @PrimaryColumn({ length: 66 })
  transactionHash: string;

  @Column({ nullable: true, length: 66 })
  id: string;

  @ManyToOne('User', 'userBrandVotes')
  user: any;

  @ManyToOne('Brand', 'userBrandVotes1')
  brand1: any;

  @ManyToOne('Brand', 'userBrandVotes2')
  brand2: any;

  @ManyToOne('Brand', 'userBrandVotes3')
  brand3: any;

  @Column()
  date: Date;

  @Column({ default: false })
  shared: boolean;

  @Column({ nullable: true })
  castHash: string;

  @Column({ nullable: true })
  brndPaidWhenCreatingPodium: number;

  // Reward claim fields
  @Column({ type: 'decimal', precision: 64, scale: 18, nullable: true })
  rewardAmount: string;

  @Column({ nullable: true })
  day: number;

  @Column({ default: false })
  shareVerified: boolean;

  @Column({ nullable: true })
  shareVerifiedAt: Date;

  @Column({ nullable: true })
  signatureGeneratedAt: Date;

  @Column({ nullable: true })
  nonce: number;

  @Column({ nullable: true })
  claimedAt: Date;

  @Column({ nullable: true, length: 66 })
  claimTxHash: string;

  @Column({ nullable: true })
  podiumImageUrl: string;

  @Column({ nullable: true })
  castedFromFid: number;

  @Column({ default: false })
  isCollectible: boolean;

  @Column({ nullable: true })
  collectibleTokenId: number;

  @Column({ nullable: true })
  collectibleOwnerFid: number;

  @Column({ nullable: true, length: 42 })
  collectibleOwnerWallet: string;

  @ManyToOne('User', { nullable: true })
  @JoinColumn({ name: 'collectibleOwnerFid', referencedColumnName: 'fid' })
  collectibleOwner: User | null;

  @Column({ type: 'decimal', precision: 65, scale: 0, nullable: true })
  collectiblePrice: string; // Store bigint as string

  @Column({ nullable: true, length: 66 })
  collectibleMintTxHash: string;

  // Genesis creator gets 5% royalties forever - useful for UI
  @Column({ nullable: true })
  collectibleGenesisCreatorFid: number;

  @ManyToOne('User', {
    nullable: true,
  })
  @JoinColumn({
    name: 'collectibleGenesisCreatorFid',
    referencedColumnName: 'fid',
  })
  collectibleGenesisCreator: any;

  // How many times this podium has been traded (affects price)
  @Column({ nullable: true })
  collectibleClaimCount: number;

  // Fee generated when voting on someone else's collectible (10% of vote cost)
  @Column({ type: 'decimal', precision: 64, scale: 18, nullable: true })
  collectibleFeeGenerated: string;

  @Column({ type: 'decimal', precision: 65, scale: 0, nullable: true })
  collectibleTotalFeesEarned: string;

  // IPFS metadata URI for the collectible NFT (e.g., "ipfs://bafkrei...")
  @Column({ nullable: true })
  collectibleMetadataURI: string;

  // Flag indicating if this is the most recent vote for this brand combination
  // Only the last voter for a combination can mint the collectible
  @Column({ default: false })
  isLastVoteForCombination: boolean;

  // Total points earned from this vote (3 points for voting + brndPowerLevel * 3 when claimed)
  @Column({ nullable: true })
  pointsEarned: number;

  // Season number (1 = before Dec 13 2025, 2 = Dec 13 2025 onwards)
  @Column({ nullable: true })
  season: number;
}
